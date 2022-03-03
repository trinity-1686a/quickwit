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

use quickwit_proto::push_api::{
    FetchRequest, FetchResponse, IngestRequest, IngestResponse, SuggestTruncateRequest,
    SuggestTruncateResponse,
};

use crate::{iter_doc_payloads, Position, Queues};

pub(crate) struct PushAPIServiceBlockingImpl {
    queues: Queues,
}

impl PushAPIServiceBlockingImpl {
    pub fn with_queues(queues: Queues) -> Self {
        PushAPIServiceBlockingImpl { queues }
    }

    pub fn ingest(&mut self, request: IngestRequest) -> crate::Result<IngestResponse> {
        for doc_batch in &request.doc_batches {
            // TODO better error handling. If there is an error, we might want to crash and restart.
            for doc_payload in iter_doc_payloads(doc_batch) {
                self.queues.append(&doc_batch.index_id, doc_payload)?;
            }
        }
        Ok(IngestResponse {})
    }

    pub fn fetch(&mut self, request: FetchRequest) -> crate::Result<FetchResponse> {
        let start_from_opt: Option<Position> = request.start_after.map(Position::from);
        self.queues.fetch(&request.index_id, start_from_opt)
    }

    pub fn suggest_truncate(
        &mut self,
        request: SuggestTruncateRequest,
    ) -> crate::Result<SuggestTruncateResponse> {
        self.queues.suggest_truncate(
            &request.index_id,
            Position::from(request.up_to_position_included),
        )?;
        Ok(SuggestTruncateResponse::default())
    }
}
