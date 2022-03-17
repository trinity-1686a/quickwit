// Copyright (C) 2021 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use async_trait::async_trait;
use quickwit_actors::{ActorExitStatus, Mailbox};
use quickwit_config::VecSourceParams;
use quickwit_metastore::checkpoint::{CheckpointDelta, PartitionId, Position, SourceCheckpoint};
use tracing::info;

use crate::actors::Indexer;
use crate::models::RawDocBatch;
use crate::source::{Source, SourceContext, TypedSourceFactory};

pub struct VecSource {
    next_item_idx: usize,
    params: VecSourceParams,
    partition: PartitionId,
}
pub struct VecSourceFactory;

#[async_trait]
impl TypedSourceFactory for VecSourceFactory {
    type Source = VecSource;
    type Params = VecSourceParams;
    async fn typed_create_source(
        params: VecSourceParams,
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self::Source> {
        let partition = PartitionId::from(params.partition.as_str());
        let next_item_idx = match checkpoint.position_for_partition(&partition) {
            Some(Position::Offset(offset_str)) => offset_str.parse::<usize>()? + 1,
            Some(Position::Beginning) | None => 0,
        };
        Ok(VecSource {
            next_item_idx,
            params,
            partition,
        })
    }
}

fn position_from_offset(offset: usize) -> Position {
    if offset == 0 {
        return Position::Beginning;
    }
    Position::from(offset as u64 - 1)
}

#[async_trait]
impl Source for VecSource {
    async fn emit_batches(
        &mut self,
        batch_sink: &Mailbox<Indexer>,
        ctx: &SourceContext,
    ) -> Result<(), ActorExitStatus> {
        let line_docs: Vec<String> = self.params.items[self.next_item_idx..]
            .iter()
            .take(self.params.batch_num_docs)
            .cloned()
            .collect();
        if line_docs.is_empty() {
            info!("Reached end of source.");
            ctx.send_exit_with_success(batch_sink).await?;
            return Err(ActorExitStatus::Success);
        }
        let from_item_idx = self.next_item_idx;
        self.next_item_idx += line_docs.len();
        let to_item_idx = self.next_item_idx;
        let checkpoint_delta = CheckpointDelta::from_partition_delta(
            self.partition.clone(),
            position_from_offset(from_item_idx),
            position_from_offset(to_item_idx),
        );
        let batch = RawDocBatch {
            docs: line_docs,
            checkpoint_delta,
        };
        ctx.send_message(batch_sink, batch).await?;
        Ok(())
    }

    fn name(&self) -> String {
        "VecSource".to_string()
    }

    fn observable_state(&self) -> serde_json::Value {
        serde_json::json!({
            "next_item_idx": self.next_item_idx,
        })
    }
}

#[cfg(test)]
mod tests {
    use quickwit_actors::{create_test_mailbox, Actor, Command, Universe};
    use serde_json::json;

    use super::*;
    use crate::source::SourceActor;

    #[tokio::test]
    async fn test_vec_source() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let (mailbox, inbox) = create_test_mailbox();
        let items = std::iter::repeat_with(|| "{}".to_string())
            .take(100)
            .collect();
        let params = VecSourceParams {
            items,
            batch_num_docs: 3,
            partition: "partition".to_string(),
        };
        let vec_source =
            VecSourceFactory::typed_create_source(params, SourceCheckpoint::default()).await?;
        let vec_source_actor = SourceActor {
            source: Box::new(vec_source),
            batch_sink: mailbox,
        };
        assert_eq!(vec_source_actor.name(), "VecSource");
        let (_vec_source_mailbox, vec_source_handle) =
            universe.spawn_actor(vec_source_actor).spawn();
        let (actor_termination, last_observation) = vec_source_handle.join().await;
        assert!(actor_termination.is_success());
        assert_eq!(last_observation, json!({"next_item_idx": 100u64}));
        let batches = inbox.drain_for_test();
        assert_eq!(batches.len(), 35);
        let raw_batch = batches[1].downcast_ref::<RawDocBatch>().unwrap();
        assert_eq!(
            format!("{:?}", raw_batch.checkpoint_delta),
            "∆(partition:(00000000000000000002..00000000000000000005])"
        );
        assert!(matches!(
            &batches[34].downcast_ref::<Command>().unwrap(),
            &Command::ExitWithSuccess
        ));
        Ok(())
    }

    #[tokio::test]
    async fn test_vec_source_from_checkpoint() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let (mailbox, inbox) = create_test_mailbox();
        let items = (0..10).map(|i| format!("{}", i)).collect();
        let params = VecSourceParams {
            items,
            batch_num_docs: 3,
            partition: "".to_string(),
        };
        let mut checkpoint = SourceCheckpoint::default();
        checkpoint.try_apply_delta(CheckpointDelta::from(0u64..2u64))?;

        let vec_source = VecSourceFactory::typed_create_source(params, checkpoint).await?;
        let vec_source_actor = SourceActor {
            source: Box::new(vec_source),
            batch_sink: mailbox,
        };
        let (_vec_source_mailbox, vec_source_handle) =
            universe.spawn_actor(vec_source_actor).spawn();
        let (actor_termination, last_observation) = vec_source_handle.join().await;
        assert!(actor_termination.is_success());
        assert_eq!(last_observation, json!({"next_item_idx": 10}));
        let messages = inbox.drain_for_test();
        let batch = messages[0].downcast_ref::<RawDocBatch>().unwrap();
        assert_eq!(&batch.docs[0], "2");
        Ok(())
    }
}
