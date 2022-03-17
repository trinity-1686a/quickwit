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

use anyhow::Context;
use tokio::sync::watch::{self, Sender};
use tracing::{debug, error, info, Instrument};

use crate::actor::{process_command, ActorExitStatus};
use crate::actor_handle::ActorHandle;
use crate::actor_with_state_tx::ActorWithStateTx;
use crate::join_handle::JoinHandle;
use crate::mailbox::{CommandOrMessage, Inbox};
use crate::{Actor, ActorContext, RecvError};

pub(crate) fn spawn_actor<A: Actor>(
    actor: A,
    ctx: ActorContext<A>,
    inbox: Inbox<A>,
) -> ActorHandle<A> {
    debug!(actor_id = %ctx.actor_instance_id(), "spawn-async");
    let (state_tx, state_rx) = watch::channel(actor.observable_state());
    let ctx_clone = ctx.clone();
    let span = actor.span(&ctx_clone);
    let (exit_status_tx, exit_status_rx) = watch::channel(None);
    let actor_instance_id = ctx.actor_instance_id().to_string();
    let loop_async_actor_future = async move {
        let exit_status = async_actor_loop(actor, inbox, ctx, state_tx).await;
        let _ = exit_status_tx.send(Some(exit_status));
    }
    .instrument(span);

    let join_handle: JoinHandle =
        JoinHandle::create_for_task(spawn_named(loop_async_actor_future, &actor_instance_id));
    ActorHandle::new(state_rx, join_handle, ctx_clone, exit_status_rx)
}

#[track_caller]
fn spawn_named<T>(
    task: impl std::future::Future<Output = T> + Send + 'static,
    _name: &str,
) -> tokio::task::JoinHandle<T>
where
    T: Send + 'static,
{
    #[cfg(tokio_unstable)]
    {
        return tokio::task::Builder::new().name(_name).spawn(task);
    }
    #[cfg(not(tokio_unstable))]
    tokio::spawn(task)
}

async fn process_msg<A: Actor>(
    actor: &mut A,
    msg_id: u64,
    inbox: &mut Inbox<A>,
    ctx: &mut ActorContext<A>,
    state_tx: &Sender<A::ObservableState>,
) -> Option<ActorExitStatus> {
    if ctx.kill_switch().is_dead() {
        return Some(ActorExitStatus::Killed);
    }
    ctx.progress().record_progress();

    let command_or_msg_recv_res = if ctx.state().is_running() {
        inbox.recv_timeout().await
    } else {
        // The actor is paused. We only process command and scheduled message.
        inbox.recv_timeout_cmd_and_scheduled_msg_only().await
    };

    ctx.progress().record_progress();
    if ctx.kill_switch().is_dead() {
        return Some(ActorExitStatus::Killed);
    }

    match command_or_msg_recv_res {
        Ok(CommandOrMessage::Command(cmd)) => {
            ctx.process();
            process_command(actor, cmd, ctx, state_tx)
        }
        Ok(CommandOrMessage::Message(mut msg)) => {
            ctx.process();
            msg.process(msg_id, actor, ctx).await.err()
        }
        Err(RecvError::Disconnected) => Some(ActorExitStatus::Success),
        Err(RecvError::Timeout) => {
            ctx.idle();
            if ctx.mailbox().is_last_mailbox() {
                // No one will be able to send us more messages.
                // We can exit the actor.
                Some(ActorExitStatus::Success)
            } else {
                None
            }
        }
    }
}

async fn async_actor_loop<A: Actor>(
    actor: A,
    mut inbox: Inbox<A>,
    mut ctx: ActorContext<A>,
    state_tx: Sender<A::ObservableState>,
) -> ActorExitStatus {
    // We rely on this object internally to fetch a post-mortem state,
    // even in case of a panic.
    let mut actor_with_state_tx = ActorWithStateTx { actor, state_tx };

    let mut exit_status_opt: Option<ActorExitStatus> =
        actor_with_state_tx.actor.initialize(&ctx).await.err();

    let mut msg_id: u64 = 1;
    let mut exit_status: ActorExitStatus = loop {
        tokio::task::yield_now().await;
        if let Some(exit_status) = exit_status_opt {
            break exit_status;
        }
        exit_status_opt = process_msg(
            &mut actor_with_state_tx.actor,
            msg_id,
            &mut inbox,
            &mut ctx,
            &actor_with_state_tx.state_tx,
        )
        .await;
        msg_id += 1;
    };
    ctx.record_progress();
    if let Err(finalize_error) = actor_with_state_tx
        .actor
        .finalize(&exit_status, &ctx)
        .await
        .with_context(|| format!("Finalization of actor {}", actor_with_state_tx.actor.name()))
    {
        error!(error=?finalize_error, "Finalizing failed, set exit status to panicked.");
        exit_status = ActorExitStatus::Panicked;
    }
    info!(actor_id = %ctx.actor_instance_id(), exit_status = %exit_status, "actor-exit");
    ctx.exit(&exit_status);
    exit_status
}
