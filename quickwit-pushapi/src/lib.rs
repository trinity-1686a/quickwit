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

mod blocking_service;
mod errors;
mod position;
mod queue;
mod service;

use std::path::Path;
use std::sync::Arc;

pub use errors::PushApiError;
use errors::Result;
use once_cell::sync::OnceCell;
pub use position::Position;
use queue::Queues;
pub use queue::{add_doc, iter_doc_payloads};

pub use crate::service::PushApiServiceImpl;

static INSTANCE: OnceCell<Arc<PushApiServiceImpl>> = OnceCell::new();

pub fn init_push_api_service(queue_path: &Path) -> anyhow::Result<()> {
    INSTANCE.get_or_try_init(|| PushApiServiceImpl::start(&queue_path).map(Arc::new))?;
    Ok(())
}

pub fn get_push_api_service() -> Option<Arc<PushApiServiceImpl>> {
    INSTANCE.get().cloned()
}
