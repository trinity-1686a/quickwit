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

use std::path::Path;

use anyhow::Context;
use flume::{Receiver, Sender};
use quickwit_proto::push_api::{
    FetchRequest, FetchResponse, IngestRequest, IngestResponse, SuggestTruncateRequest,
    SuggestTruncateResponse,
};
use tokio::sync::oneshot;
use tracing::{info, warn};

use crate::blocking_service::PushAPIServiceBlockingImpl;
use crate::Queues;

enum Command {
    Ingest {
        req: IngestRequest,
        resp_tx: oneshot::Sender<crate::Result<IngestResponse>>,
    },
    Fetch {
        req: FetchRequest,
        resp_tx: oneshot::Sender<crate::Result<FetchResponse>>,
    },
    SuggestTruncate {
        req: SuggestTruncateRequest,
        resp_tx: oneshot::Sender<crate::Result<SuggestTruncateResponse>>,
    },
}

trait EndPoint<Req, Resp> {
    fn build_command(req: Req) -> (Command, oneshot::Receiver<crate::Result<Resp>>);
}

impl EndPoint<IngestRequest, IngestResponse> for Command {
    fn build_command(
        req: IngestRequest,
    ) -> (Command, oneshot::Receiver<crate::Result<IngestResponse>>) {
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = Command::Ingest { req, resp_tx };
        (cmd, resp_rx)
    }
}

impl EndPoint<FetchRequest, FetchResponse> for Command {
    fn build_command(
        req: FetchRequest,
    ) -> (Command, oneshot::Receiver<crate::Result<FetchResponse>>) {
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = Command::Fetch { req, resp_tx };
        (cmd, resp_rx)
    }
}

impl EndPoint<SuggestTruncateRequest, SuggestTruncateResponse> for Command {
    fn build_command(
        req: SuggestTruncateRequest,
    ) -> (
        Command,
        oneshot::Receiver<crate::Result<SuggestTruncateResponse>>,
    ) {
        let (resp_tx, resp_rx) = oneshot::channel();
        let cmd = Command::SuggestTruncate { req, resp_tx };
        (cmd, resp_rx)
    }
}

pub struct PushApiServiceImpl {
    command_sender: Sender<Command>,
}

impl PushApiServiceImpl {
    pub(crate) fn start(queue_path: &Path) -> anyhow::Result<PushApiServiceImpl> {
        let (command_sender, rx) = flume::bounded(5);
        let queues = Queues::open(queue_path)
            .with_context(|| format!("Failed to open queues at path {queue_path:?}."))?;
        let blocking_service = PushAPIServiceBlockingImpl::with_queues(queues);
        std::thread::Builder::new()
            .name("quickwit-push-api".to_string())
            .spawn(move || {
                push_api_loop(blocking_service, rx);
            })
            .context("Failed to spawn push API")?;
        Ok(PushApiServiceImpl { command_sender })
    }
}

impl PushApiServiceImpl {
    async fn process_request<Req, Resp>(&self, req: Req) -> crate::Result<Resp>
    where Command: EndPoint<Req, Resp> {
        let (cmd, rx) = Command::build_command(req);
        self.command_sender
            .send_async(cmd)
            .await
            .map_err(|_| crate::PushApiError::PushAPIServiceDown)?;
        let cmd_res = rx
            .await
            .map_err(|_| crate::PushApiError::PushAPIServiceDown)??;
        Ok(cmd_res)
    }
}

impl PushApiServiceImpl {
    pub async fn ingest(&self, request: IngestRequest) -> crate::Result<IngestResponse> {
        // TODO is there a timeout logic in tonic
        self.process_request(request).await
    }

    pub async fn fetch(&self, request: FetchRequest) -> crate::Result<FetchResponse> {
        self.process_request(request).await
    }

    pub async fn suggest_truncate(
        &self,
        request: SuggestTruncateRequest,
    ) -> crate::Result<SuggestTruncateResponse> {
        self.process_request(request).await
    }
}

fn push_api_loop(mut service: PushAPIServiceBlockingImpl, rx: Receiver<Command>) {
    while let Ok(msg) = rx.recv() {
        run_push_api_cmd(msg, &mut service);
    }
    info!("All clients to the PushAPI are disconnected");
}

fn run_push_api_cmd(msg: Command, service: &mut PushAPIServiceBlockingImpl) {
    match msg {
        Command::Ingest { req, resp_tx } => {
            let ingest_resp = service.ingest(req);
            if resp_tx.send(ingest_resp).is_err() {
                warn!("Ingest succeeded, but the response receiver channel was dropped.");
            }
        }
        Command::Fetch { req, resp_tx } => {
            let fetch_resp = service.fetch(req);
            if resp_tx.send(fetch_resp).is_err() {
                warn!("Fetch succeeded, but the response receiver channel was dropped.");
            }
        }
        Command::SuggestTruncate { req, resp_tx } => {
            let suggest_truncate_resp = service.suggest_truncate(req);
            if resp_tx.send(suggest_truncate_resp).is_err() {
                warn!("Suggest truncate succeeded, but the response receiver channel was dropped.");
            }
        }
    }
}
