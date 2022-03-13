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
use std::sync::Arc;

use anyhow::bail;
use quickwit_config::{IndexerConfig, QuickwitConfig, SourceConfig};
use quickwit_metastore::Metastore;
use quickwit_storage::StorageUriResolver;

use crate::actors::{
    IndexingPipeline, IndexingPipelineParams, IndexingServer, IndexingServerClient,
};
use crate::models::IndexingStatistics;
pub use crate::split_store::{
    get_tantivy_directory_from_split_bundle, IndexingSplitStore, IndexingSplitStoreParams,
    SplitFolder,
};

pub mod actors;
mod controlled_directory;
mod garbage_collection;
pub mod merge_policy;
pub mod models;
pub mod source;
mod split_store;
mod test_utils;

pub use test_utils::{mock_split, mock_split_meta, TestSandbox};

pub use self::garbage_collection::{delete_splits_with_files, run_garbage_collect, FileEntry};
use self::merge_policy::{MergePolicy, StableMultitenantWithTimestampMergePolicy};
pub use self::source::check_source_connectivity;

pub async fn index_data(
    index_id: String,
    data_dir_path: &Path,
    indexer_config: &IndexerConfig,
    source: SourceConfig,
    metastore: Arc<dyn Metastore>,
    storage_resolver: StorageUriResolver,
) -> anyhow::Result<IndexingStatistics> {
    let client = IndexingServer::spawn(data_dir_path, indexer_config, metastore, storage_resolver);
    let pipeline_id = client.spawn_pipeline(index_id, source).await?;
    let pipeline_handle = client.detach_pipeline(&pipeline_id).await?;
    let (exit_status, statistics) = pipeline_handle.join().await;
    if !exit_status.is_success() {
        bail!(exit_status);
    }
    Ok(statistics)
}

pub fn new_split_id() -> String {
    ulid::Ulid::new().to_string()
}

pub async fn start_indexer_service(
    config: &QuickwitConfig,
    metastore: Arc<dyn Metastore>,
    storage_uri_resolver: StorageUriResolver,
) -> anyhow::Result<IndexingServerClient> {
    let client = IndexingServer::spawn(
        &config.data_dir_path,
        &config.indexer_config,
        metastore,
        storage_uri_resolver,
    );
    // for index_id in args.index_ids {
    //     client.spawn_pipelines(index_id).await?;
    // }
    Ok(client)
    // let (exit_status, _) = client.join_server().await;
    // if exit_status.is_success() {
    //     bail!(exit_status)
    // }
}
