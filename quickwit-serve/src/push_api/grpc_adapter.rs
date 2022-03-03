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

use std::sync::Arc;

use async_trait::async_trait;
use quickwit_proto::push_api::{
    push_api_service_server as grpc, FetchRequest, FetchResponse, IngestRequest, IngestResponse,
    SuggestTruncateRequest, SuggestTruncateResponse,
};
use quickwit_proto::tonic;
use quickwit_pushapi::PushApiServiceImpl;

use crate::error::convert_to_grpc_result;

#[derive(Clone)]
pub struct GrpcPushApiAdapter(Arc<PushApiServiceImpl>);

impl From<Arc<PushApiServiceImpl>> for GrpcPushApiAdapter {
    fn from(push_api_service: Arc<PushApiServiceImpl>) -> Self {
        GrpcPushApiAdapter(push_api_service)
    }
}

#[async_trait]
impl grpc::PushApiService for GrpcPushApiAdapter {
    async fn ingest(
        &self,
        request: tonic::Request<IngestRequest>,
    ) -> Result<tonic::Response<IngestResponse>, tonic::Status> {
        let req = request.into_inner();
        let res = self.0.ingest(req).await;
        convert_to_grpc_result(res)
    }

    async fn suggest_truncate(
        &self,
        request: tonic::Request<SuggestTruncateRequest>,
    ) -> Result<tonic::Response<SuggestTruncateResponse>, tonic::Status> {
        let req = request.into_inner();
        let res = self.0.suggest_truncate(req).await;
        convert_to_grpc_result(res)
    }

    async fn fetch(
        &self,
        request: tonic::Request<FetchRequest>,
    ) -> Result<tonic::Response<FetchResponse>, tonic::Status> {
        let req = request.into_inner();
        let res = self.0.fetch(req).await;
        convert_to_grpc_result(res)
    }
}
