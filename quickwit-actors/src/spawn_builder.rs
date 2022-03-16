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

use crate::mailbox::Inbox;
use crate::scheduler::Scheduler;
use crate::{create_mailbox, Actor, ActorContext, ActorHandle, ActorRunner, KillSwitch, Mailbox};

/// `SpawnBuilder` makes it possible to configure misc parameters before spawning an actor.
pub struct SpawnBuilder<A: Actor> {
    actor: A,
    scheduler_mailbox: Mailbox<Scheduler>,
    kill_switch: KillSwitch,
    #[allow(clippy::type_complexity)]
    mailboxes: Option<(Mailbox<A>, Inbox<A>)>,
}

impl<A: Actor> SpawnBuilder<A> {
    pub(crate) fn new(
        actor: A,
        scheduler_mailbox: Mailbox<Scheduler>,
        kill_switch: KillSwitch,
    ) -> Self {
        SpawnBuilder {
            actor,
            scheduler_mailbox,
            kill_switch,
            mailboxes: None,
        }
    }

    /// Sets a specific kill switch for the actor.
    ///
    /// By default, the kill switch is inherited from the context that was used to
    /// spawn the actor.
    pub fn set_kill_switch(mut self, kill_switch: KillSwitch) -> Self {
        self.kill_switch = kill_switch;
        self
    }

    /// Sets a specific set of mailbox.
    ///
    /// By default, a brand new set of mailboxes will be created
    /// when the actor is spawned.
    ///
    /// This function makes it possible to create non-DAG networks
    /// of actors.
    pub fn set_mailboxes(mut self, mailbox: Mailbox<A>, inbox: Inbox<A>) -> Self {
        self.mailboxes = Some((mailbox, inbox));
        self
    }

    fn create_actor_context_and_inbox(mut self) -> (A, ActorContext<A>, Inbox<A>) {
        let (mailbox, inbox) = self.mailboxes.take().unwrap_or_else(|| {
            let actor_name = self.actor.name();
            let queue_capacity = self.actor.queue_capacity();
            create_mailbox(actor_name, queue_capacity)
        });
        let ctx = ActorContext::new(
            mailbox,
            self.kill_switch.clone(),
            self.scheduler_mailbox.clone(),
        );
        (self.actor, ctx, inbox)
    }
}

impl<A: Actor> SpawnBuilder<A> {
    /// Spawns an async actor.
    pub fn spawn(self) -> (Mailbox<A>, ActorHandle<A>) {
        let runner = self.actor.runner();
        self.spawn_with_forced_runner(runner)
    }

    /// Ignore the actor default runner, and run the actor on a specific one.
    pub fn spawn_with_forced_runner(self, runner: ActorRunner) -> (Mailbox<A>, ActorHandle<A>) {
        let (actor, ctx, inbox) = self.create_actor_context_and_inbox();
        let mailbox = ctx.mailbox().clone();
        let actor_handle = runner.spawn_actor(actor, ctx, inbox);
        (mailbox, actor_handle)
    }
}
