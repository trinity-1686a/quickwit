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

use crate::{Actor, ActorContext, ActorExitStatus, AsyncHandler, Message};

#[async_trait::async_trait]
pub(crate) trait AsyncEnvelope<A: Actor>: Send + Sync {
    async fn process(
        &mut self,
        actor: &mut A,
        ctx: &ActorContext<A>,
    ) -> Result<(), ActorExitStatus>;
}

struct AsyncEnvelopeImpl<M: Send> {
    message: Option<M>,
}

#[async_trait::async_trait]
impl<A, M: Send + Sync> AsyncEnvelope<A> for AsyncEnvelopeImpl<M>
where
    A: AsyncHandler<M>,
    M: Message,
{
    async fn process(
        &mut self,
        actor: &mut A,
        ctx: &ActorContext<A>,
    ) -> Result<(), ActorExitStatus> {
        if let Some(msg) = self.message.take() {
            actor.handle(msg, ctx).await?;
        }
        // TODO
        Ok(())
    }
}

impl<M: Send> From<M> for AsyncEnvelopeImpl<M> {
    fn from(message: M) -> Self {
        AsyncEnvelopeImpl {
            message: Some(message),
        }
    }
}

pub(crate) fn wrap_in_async_envelope<A, M>(msg: M) -> Box<dyn AsyncEnvelope<A>>
where
    A: AsyncHandler<M>,
    M: Message,
{
    Box::new(AsyncEnvelopeImpl::from(msg)) as Box<dyn AsyncEnvelope<A>>
}
