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
//


pub(crate) trait SyncEnvelope<A: SyncActor> {
    fn process(&mut self, actor: &mut A, ctx: &ActorContext<A>) -> Result<(), ActorExitStatus>;
}

struct EnvelopeImpl<M: Send> {
    message: Option<M>,
}

impl<A, M: Send> Envelope<A> for SyncEnvelopeImpl<M>
    where A: SyncActor + Handler<M> {
    fn process(&mut self, actor: &mut A, ctx: &ActorContext<A>) -> Result<(), ActorExitStatus> {
        if let Some(msg ) = self.message.take() {
            actor.handle(msg, ctx);
        }
        // TODO
        Ok(())
    }
}


impl<M: Send> From<M> for SyncEnvelopeImpl<M> {
    fn from(message: M) -> Self {
        SyncEnvelopeImpl {
            message: Some(message)
        }

    }
}


fn wrap_in_sync_envelope<A, M: 'static + Send>(msg: M) -> Box<dyn SyncEnvelope<A>>
    where A: Handler<M> {
    Box::new(SyncEnvelopeImpl::from(msg)) as Box<dyn SyncEnvelope<A>>
}
pub enum CommandOrMessage<A: Actor> {
    Message(Box<dyn SyncEnvelope<A>>),
    Command(Command),
}
