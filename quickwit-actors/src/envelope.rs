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

use tokio::sync::oneshot;

use crate::{Actor, ActorContext, ActorExitStatus, AsyncHandler, Message, AsyncActor};

#[async_trait::async_trait]
pub(crate) trait AsyncEnvelope<A: Actor>: Send + Sync {
    async fn process(
        &mut self,
        actor: &mut A,
        ctx: &ActorContext<A>,
    ) -> Result<(), ActorExitStatus>;
}

pub(crate) trait SyncEnvelope<A: Actor>: Send + Sync {
    fn process(
        &mut self,
        actor: &mut A,
        ctx: &ActorContext<A>,
    ) -> Result<(), ActorExitStatus>;
}

pub(crate) enum Envelope<A: Actor> {
    Sync(Box<dyn SyncEnvelope<A>>),
    Async(Box<dyn AsyncEnvelope<A>>),
    Toto
}

#[async_trait::async_trait]
impl<A, M> AsyncEnvelope<A> for Option<(oneshot::Sender<M::Response>, M)>
where
    A: AsyncHandler<M>,
    M: Message,
{
    async fn process(
        &mut self,
        actor: &mut A,
        ctx: &ActorContext<A>,
    ) -> Result<(), ActorExitStatus> {
        if let Some((response_tx, msg)) = self.take() {
            let response = actor.handle(msg, ctx).await?;
            // It is fine if the caller is not waiting for the response.
            let _ = response_tx.send(response);
        }
        // TODO
        Ok(())
    }
}

pub(crate) fn wrap_in_async_envelope<A, M>(
    msg: M,
) -> (Box<dyn AsyncEnvelope<A>>, oneshot::Receiver<M::Response>)
where
    A: AsyncHandler<M>,
    M: Message,
{
    let (response_tx, response_rx) = oneshot::channel();
    let envelope = Some((response_tx, msg));
    (Box::new(envelope) as Box<dyn AsyncEnvelope<A>>, response_rx)
}

trait EnvelopeWrapper<M: Message, A: Actor> {
    fn message(&self, msg: M) -> (Envelope<A>, oneshot::Receiver<M::Response>);
}

impl<M, A> EnvelopeWrapper<M, A> for &&A
where
    M: Message,
    A: AsyncHandler<M> {
    fn message(&self, msg: M) -> (Envelope<A>, oneshot::Receiver<M::Response>) {
        let (response_tx, response_rx) = oneshot::channel();
        let envelope: Box<dyn AsyncEnvelope<A>> = Box::new(Some((response_tx, msg)));
        (Envelope::Async(envelope), response_rx)
    }
}

impl<M, A> EnvelopeWrapper<M, A> for &A
where
    M: Message,
    A: TotoTrait<M> {
    fn message(&self, msg: M) -> (Envelope<A>, oneshot::Receiver<M::Response>) {
        let (response_tx, response_rx) = oneshot::channel();
        // let envelope: Box<dyn AsyncEnvelope<A>> = Box::new(Some((response_tx, msg)));
        (Envelope::Toto, response_rx)
    }
}

trait TotoTrait<M>: Actor {}
