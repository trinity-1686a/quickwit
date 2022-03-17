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

use std::collections::BTreeSet;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{bail, Context};
use async_trait::async_trait;
use fail::fail_point;
use itertools::Itertools;
use quickwit_actors::{
    Actor, ActorContext, ActorExitStatus, ActorRunner, Handler, Mailbox, QueueCapacity,
};
use quickwit_directories::write_hotcache;
use quickwit_doc_mapper::tag_pruning::append_to_tag_set;
use tantivy::schema::FieldType;
use tantivy::{InvertedIndexReader, ReloadPolicy, SegmentId, SegmentMeta};
use tracing::{debug, info, info_span, warn, Span};

/// Maximum distinct values allowed for a tag field within a split.
const MAX_VALUES_PER_TAG_FIELD: usize = if cfg!(any(test, feature = "testsuite")) {
    6
} else {
    1000
};

use super::NamedField;
use crate::actors::Uploader;
use crate::models::{
    IndexedSplit, IndexedSplitBatch, PackagedSplit, PackagedSplitBatch, ScratchDirectory,
};

/// The role of the packager is to get an index writer and
/// produce a split file.
///
/// This includes the following steps:
/// - commit: this step is CPU heavy
/// - indentifying the list of tags for the splits, and labelling it accordingly
/// - creating a bundle file
/// - computing the hotcache
/// - appending it to the split file.
///
/// The split format is described in `internals/split-format.md`
pub struct Packager {
    actor_name: &'static str,
    uploader_mailbox: Mailbox<Uploader>,
    /// List of tag fields ([`Vec<NamedField>`]) defined in the index config.
    tag_fields: Vec<NamedField>,
}

impl Packager {
    pub fn new(
        actor_name: &'static str,
        tag_fields: Vec<NamedField>,
        uploader_mailbox: Mailbox<Uploader>,
    ) -> Packager {
        Packager {
            actor_name,
            uploader_mailbox,
            tag_fields,
        }
    }

    pub fn process_indexed_split(
        &self,
        mut split: IndexedSplit,
        ctx: &ActorContext<Self>,
    ) -> anyhow::Result<PackagedSplit> {
        commit_split(&mut split, ctx)?;
        let segment_metas = merge_segments_if_required(&mut split, ctx)?;
        let packaged_split =
            create_packaged_split(&segment_metas[..], split, &self.tag_fields, ctx)?;
        Ok(packaged_split)
    }
}

#[async_trait]
impl Actor for Packager {
    type ObservableState = ();

    #[allow(clippy::unused_unit)]
    fn observable_state(&self) -> Self::ObservableState {
        ()
    }

    fn queue_capacity(&self) -> QueueCapacity {
        QueueCapacity::Bounded(0)
    }

    fn name(&self) -> String {
        self.actor_name.to_string()
    }

    fn runner(&self) -> ActorRunner {
        ActorRunner::DedicatedThread
    }
}

#[async_trait]
impl Handler<IndexedSplitBatch> for Packager {
    type Reply = ();

    fn message_span(&self, msg_id: u64, batch: &IndexedSplitBatch) -> Span {
        info_span!("", msg_id=&msg_id, num_splits=%batch.splits.len())
    }

    async fn handle(
        &mut self,
        batch: IndexedSplitBatch,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        info!(split_ids=?batch.splits.iter().map(|split| split.split_id.clone()).collect_vec(), "start-packaging-splits");
        for split in &batch.splits {
            if let Some(controlled_directory) = split.controlled_directory_opt.as_ref() {
                controlled_directory.set_progress_and_kill_switch(
                    ctx.progress().clone(),
                    ctx.kill_switch().clone(),
                );
            }
        }
        fail_point!("packager:before");
        let packaged_splits = batch
            .splits
            .into_iter()
            .map(|split| self.process_indexed_split(split, ctx))
            .try_collect()?;
        ctx.send_message(
            &self.uploader_mailbox,
            PackagedSplitBatch::new(packaged_splits),
        )
        .await?;
        fail_point!("packager:after");
        Ok(())
    }
}

/// returns true iff merge is required to reach a state where
/// we have zero, or a single segment with no deletes segment.
fn is_merge_required(segment_metas: &[SegmentMeta]) -> bool {
    match &segment_metas {
        // there are no segment to merge
        [] => false,
        // if there is only segment but it has deletes, it
        // still makes sense to merge it alone in order to remove deleted documents.
        [segment_meta] => segment_meta.has_deletes(),
        _ => true,
    }
}

/// Commits the tantivy Index.
/// Tantivy will serialize all of its remaining internal in RAM
/// datastructure and write them on disk.
///
/// It consists in several sequentials phases mixing both
/// CPU and IO, the longest once being the serialization of
/// the inverted index. This phase is CPU bound.
fn commit_split(split: &mut IndexedSplit, ctx: &ActorContext<Packager>) -> anyhow::Result<()> {
    info!(split_id=%split.split_id, "commit-split");
    let _protect_guard = ctx.protect_zone();
    split
        .index_writer
        .commit()
        .with_context(|| format!("Commit split `{:?}` failed", &split))?;
    Ok(())
}

fn list_split_files(
    segment_metas: &[SegmentMeta],
    scratch_directory: &ScratchDirectory,
) -> Vec<PathBuf> {
    let mut index_files = vec![scratch_directory.path().join("meta.json")];

    // list the segment files
    for segment_meta in segment_metas {
        for relative_path in segment_meta.list_files() {
            let filepath = scratch_directory.path().join(&relative_path);
            if filepath.exists() {
                // If the file is missing, this is fine.
                // segment_meta.list_files() may actually returns files that
                // may not exist.
                index_files.push(filepath);
            }
        }
    }
    index_files.sort();
    index_files
}

/// If necessary, merge all segments and returns a PackagedSplit.
///
/// Note this function implicitly drops the IndexWriter
/// which potentially olds a lot of RAM.
fn merge_segments_if_required(
    split: &mut IndexedSplit,
    ctx: &ActorContext<Packager>,
) -> anyhow::Result<Vec<SegmentMeta>> {
    debug!(
        split_id = split.split_id.as_str(),
        "merge-segments-if-required"
    );
    let segment_metas_before_merge = split.index.searchable_segment_metas()?;
    if is_merge_required(&segment_metas_before_merge[..]) {
        let segment_ids: Vec<SegmentId> = segment_metas_before_merge
            .into_iter()
            .map(|segment_meta| segment_meta.id())
            .collect();

        info!(split_id=split.split_id.as_str(), segment_ids=?segment_ids, "merging-segments");
        // TODO it would be nice if tantivy could let us run the merge in the current thread.
        let _protected_zone_guard = ctx.protect_zone();
        futures::executor::block_on(split.index_writer.merge(&segment_ids))?;
    }
    let segment_metas_after_merge: Vec<SegmentMeta> = split.index.searchable_segment_metas()?;
    Ok(segment_metas_after_merge)
}

fn build_hotcache<W: io::Write>(split_path: &Path, out: &mut W) -> anyhow::Result<()> {
    let mmap_directory = tantivy::directory::MmapDirectory::open(split_path)?;
    write_hotcache(mmap_directory, out)?;
    Ok(())
}

/// Attempts to exhaustively extract the list of terms in a
/// field term dictionary.
///
/// returns None if:
/// - the number of terms exceed MAX_VALUES_PER_TAG_FIELD
/// - some of the terms are not value utf8.
/// - an error occurs.
///
/// Returns None may hurt split pruning and affects performance,
/// but it does not affect Quickwit's result validity.
fn try_extract_terms(
    named_field: &NamedField,
    inv_indexes: &[Arc<InvertedIndexReader>],
    max_terms: usize,
) -> anyhow::Result<Vec<String>> {
    let num_terms = inv_indexes
        .iter()
        .map(|inv_index| inv_index.terms().num_terms())
        .sum::<usize>();
    if num_terms > max_terms {
        bail!(
            "Number of unique terms for tag field {} > {}.",
            named_field.name,
            max_terms
        );
    }
    let mut terms = Vec::with_capacity(num_terms);
    for inv_index in inv_indexes {
        let mut terms_streamer = inv_index.terms().stream()?;
        while let Some((term_data, _)) = terms_streamer.next() {
            let term = match named_field.field_type {
                FieldType::U64(_) => u64_from_term_data(term_data)?.to_string(),
                FieldType::I64(_) => {
                    tantivy::u64_to_i64(u64_from_term_data(term_data)?).to_string()
                }
                FieldType::F64(_) => {
                    tantivy::u64_to_f64(u64_from_term_data(term_data)?).to_string()
                }
                FieldType::Bytes(_) => {
                    bail!("Tags collection is not allowed on `bytes` fields.")
                }
                _ => std::str::from_utf8(term_data)?.to_string(),
            };
            terms.push(term);
        }
    }
    Ok(terms)
}

fn create_packaged_split(
    segment_metas: &[SegmentMeta],
    split: IndexedSplit,
    tag_fields: &[NamedField],
    ctx: &ActorContext<Packager>,
) -> anyhow::Result<PackagedSplit> {
    info!(split_id = split.split_id.as_str(), "create-packaged-split");
    let split_files = list_split_files(segment_metas, &split.split_scratch_directory);
    let num_docs = segment_metas
        .iter()
        .map(|segment_meta| segment_meta.num_docs() as u64)
        .sum();

    // Extracts tag values from inverted indexes only when a field cardinality is less
    // than `MAX_VALUES_PER_TAG_FIELD`.
    debug!(split_id = split.split_id.as_str(), tag_fields =? tag_fields, "extract-tags-values");
    let index_reader = split
        .index
        .reader_builder()
        .reload_policy(ReloadPolicy::Manual)
        .try_into()?;
    let mut tags = BTreeSet::default();
    for named_field in tag_fields {
        let inverted_indexes = index_reader
            .searcher()
            .segment_readers()
            .iter()
            .map(|segment| segment.inverted_index(named_field.field))
            .collect::<Result<Vec<_>, _>>()?;

        match try_extract_terms(named_field, &inverted_indexes, MAX_VALUES_PER_TAG_FIELD) {
            Ok(terms) => {
                append_to_tag_set(&named_field.name, &terms, &mut tags);
            }
            Err(tag_extraction_error) => {
                warn!(err=?tag_extraction_error,  "No field values will be registered in the split metadata.");
            }
        }
    }

    ctx.record_progress();

    debug!(split_id = split.split_id.as_str(), "build-hotcache");
    let mut hotcache_bytes = vec![];
    build_hotcache(split.split_scratch_directory.path(), &mut hotcache_bytes)?;
    ctx.record_progress();

    let packaged_split = PackagedSplit {
        split_id: split.split_id.to_string(),
        replaced_split_ids: split.replaced_split_ids,
        index_id: split.index_id,
        checkpoint_deltas: vec![split.checkpoint_delta],
        split_scratch_directory: split.split_scratch_directory,
        num_docs,
        demux_num_ops: split.demux_num_ops,
        time_range: split.time_range,
        size_in_bytes: split.docs_size_in_bytes,
        tags,
        split_date_of_birth: split.split_date_of_birth,
        split_files,
        hotcache_bytes,
    };
    Ok(packaged_split)
}

/// Reads u64 from stored term data.
fn u64_from_term_data(data: &[u8]) -> anyhow::Result<u64> {
    let u64_bytes: [u8; 8] = data[0..8]
        .try_into()
        .context("Could not interpret term bytes as u64")?;
    Ok(u64::from_be_bytes(u64_bytes))
}

#[cfg(test)]
mod tests {
    use std::ops::RangeInclusive;
    use std::time::Instant;

    use quickwit_actors::{create_test_mailbox, ObservationType, Universe};
    use quickwit_metastore::checkpoint::CheckpointDelta;
    use tantivy::schema::{NumericOptions, Schema, FAST, STRING, TEXT};
    use tantivy::{doc, Index};

    use super::*;
    use crate::models::ScratchDirectory;

    fn make_indexed_split_for_test(segments_timestamps: &[&[i64]]) -> anyhow::Result<IndexedSplit> {
        let split_scratch_directory = ScratchDirectory::for_test()?;
        let mut schema_builder = Schema::builder();
        let text_field = schema_builder.add_text_field("text", TEXT);
        let timestamp_field = schema_builder.add_u64_field("timestamp", FAST);
        let tag_str = schema_builder.add_text_field("tag_str", STRING);
        let tag_many = schema_builder.add_text_field("tag_many", STRING);
        let tag_u64 =
            schema_builder.add_u64_field("tag_u64", NumericOptions::default().set_indexed());
        let tag_i64 =
            schema_builder.add_i64_field("tag_i64", NumericOptions::default().set_indexed());
        let tag_f64 =
            schema_builder.add_f64_field("tag_f64", NumericOptions::default().set_indexed());
        let schema = schema_builder.build();
        let index = Index::create_in_dir(split_scratch_directory.path(), schema)?;
        let mut index_writer = index.writer_with_num_threads(1, 10_000_000)?;
        let mut timerange_opt: Option<RangeInclusive<i64>> = None;
        let mut num_docs = 0;
        for (segment_num, segment_timestamps) in segments_timestamps.iter().enumerate() {
            if segment_num > 0 {
                index_writer.commit()?;
            }
            for &timestamp in segment_timestamps.iter() {
                for num in 1..10 {
                    let doc = doc!(
                        text_field => format!("timestamp is {}", timestamp),
                        timestamp_field => timestamp,
                        tag_str => "value",
                        tag_many => format!("many-{}", num),
                        tag_u64 => 42u64,
                        tag_i64 => -42i64,
                        tag_f64 => -42.02f64,
                    );
                    index_writer.add_document(doc)?;
                    num_docs += 1;
                    timerange_opt = Some(
                        timerange_opt
                            .map(|timestamp_range| {
                                let start = timestamp.min(*timestamp_range.start());
                                let end = timestamp.max(*timestamp_range.end());
                                RangeInclusive::new(start, end)
                            })
                            .unwrap_or_else(|| RangeInclusive::new(timestamp, timestamp)),
                    )
                }
            }
        }
        // We don't commit, that's the job of the packager.
        //
        // TODO: In the future we would like that kind of segment flush to emit a new split,
        // but this will require work on tantivy.
        let indexed_split = IndexedSplit {
            split_id: "test-split".to_string(),
            index_id: "test-index".to_string(),
            time_range: timerange_opt,
            demux_num_ops: 0,
            num_docs,
            docs_size_in_bytes: num_docs * 15, //< bogus number
            split_date_of_birth: Instant::now(),
            index,
            index_writer,
            split_scratch_directory,
            checkpoint_delta: CheckpointDelta::from(10..20),
            replaced_split_ids: Vec::new(),
            controlled_directory_opt: None,
        };
        Ok(indexed_split)
    }

    fn get_tag_fields(schema: Schema, field_names: &[&str]) -> Vec<NamedField> {
        field_names
            .iter()
            .map(|field_name| {
                let field = schema.get_field(field_name).unwrap();
                let field_type = schema.get_field_entry(field).field_type().clone();
                NamedField {
                    name: field_name.to_string(),
                    field,
                    field_type,
                }
            })
            .collect()
    }

    #[tokio::test]
    async fn test_packager_no_merge_required() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let (mailbox, inbox) = create_test_mailbox();
        let indexed_split = make_indexed_split_for_test(&[&[1628203589, 1628203640]])?;
        let tag_fields = get_tag_fields(
            indexed_split.index.schema(),
            &["tag_str", "tag_many", "tag_u64", "tag_i64", "tag_f64"],
        );
        let packager = Packager::new("TestPackager", tag_fields, mailbox);
        let (packager_mailbox, packager_handle) = universe.spawn_actor(packager).spawn();
        universe
            .send_message(
                &packager_mailbox,
                IndexedSplitBatch {
                    splits: vec![indexed_split],
                },
            )
            .await?;
        assert_eq!(
            packager_handle.process_pending_and_observe().await.obs_type,
            ObservationType::Alive
        );
        let packaged_splits = inbox.drain_available_message_for_test();
        assert_eq!(packaged_splits.len(), 1);
        let packaged_split = packaged_splits[0].downcast_ref::<PackagedSplit>().unwrap();
        let split = &packaged_split.splits[0];
        assert_eq!(
            &split.tags.iter().map(|s| s.as_str()).collect::<Vec<&str>>(),
            &[
                "tag_f64!",
                "tag_f64:-42.02",
                "tag_i64!",
                "tag_i64:-42",
                "tag_str!",
                "tag_str:value",
                "tag_u64!",
                "tag_u64:42"
            ]
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_packager_merge_required() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let (mailbox, inbox) = create_test_mailbox();
        let indexed_split = make_indexed_split_for_test(&[&[1628203589], &[1628203640]])?;
        let tag_fields = get_tag_fields(indexed_split.index.schema(), &[]);
        let packager = Packager::new("TestPackager", tag_fields, mailbox);
        let (packager_mailbox, packager_handle) = universe.spawn_actor(packager).spawn();
        universe
            .send_message(
                &packager_mailbox,
                IndexedSplitBatch {
                    splits: vec![indexed_split],
                },
            )
            .await?;
        assert_eq!(
            packager_handle.process_pending_and_observe().await.obs_type,
            ObservationType::Alive
        );
        let packaged_splits = inbox.drain_available_message_for_test();
        assert_eq!(packaged_splits.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_package_two_indexed_split_and_merge_required() -> anyhow::Result<()> {
        quickwit_common::setup_logging_for_tests();
        let universe = Universe::new();
        let (mailbox, inbox) = create_test_mailbox();
        let indexed_split_1 = make_indexed_split_for_test(&[&[1628203589], &[1628203640]])?;
        let indexed_split_2 = make_indexed_split_for_test(&[&[1628204589], &[1629203640]])?;
        let tag_fields = get_tag_fields(indexed_split_1.index.schema(), &[]);
        let packager = Packager::new("TestPackager", tag_fields, mailbox);
        let (packager_mailbox, packager_handle) = universe.spawn_actor(packager).spawn();
        universe
            .send_message(
                &packager_mailbox,
                IndexedSplitBatch {
                    splits: vec![indexed_split_1, indexed_split_2],
                },
            )
            .await?;
        assert_eq!(
            packager_handle.process_pending_and_observe().await.obs_type,
            ObservationType::Alive
        );
        let mut packaged_splits = inbox.drain_available_message_for_test();
        assert_eq!(packaged_splits.len(), 1);
        assert_eq!(packaged_splits[0], ""); //.pop().unwrap().into_iter().count(), 2);
        Ok(())
    }
}
