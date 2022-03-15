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

//! Module for [`FileBackedMetastore`]. It is public so that the crate `quickwit-backward-compat`
//! can import [`FiledBackedIndex`] and run backward-compatibility tests. You should not have to
//! import anything from here directly.

pub mod file_backed_index;
mod file_backed_metastore_factory;
mod store_operations;

use std::collections::HashMap;
use std::ops::Range;
use std::sync::{Arc, Weak};
use std::time::Duration;

use async_trait::async_trait;
use futures::future::try_join_all;
use quickwit_config::SourceConfig;
use quickwit_doc_mapper::tag_pruning::TagFilterAst;
use quickwit_storage::Storage;
use tokio::sync::{Mutex, OwnedMutexGuard, RwLock};
use tracing::error;

use self::file_backed_index::FileBackedIndex;
pub use self::file_backed_metastore_factory::FileBackedMetastoreFactory;
use self::store_operations::{delete_index, fetch_index, index_exists, put_index, fetch_indexes, put_indexes, IndexState};
use crate::checkpoint::CheckpointDelta;
use crate::{
    IndexMetadata, Metastore, MetastoreError, MetastoreResult, Split, SplitMetadata, SplitState,
};

/// Metastore that stores all of the metadata associated to each index
/// into as many files and tracks indexes states in `Map<index_id: String, IndexState>`.
pub struct FileBackedMetastore {
    storage: Arc<dyn Storage>,
    per_index_metastores: RwLock<HashMap<String, Arc<Mutex<FileBackedIndex>>>>,
    indexes_states: RwLock<HashMap<String, IndexState>>,
    polling_interval_opt: Option<Duration>,
}

async fn poll_metastore_once(
    storage: &dyn Storage,
    index_id: &str,
    metadata_mutex: &Mutex<FileBackedIndex>,
) {
    let index_fetch_res = fetch_index(&*storage, index_id).await;
    match index_fetch_res {
        Ok(index) => {
            *metadata_mutex.lock().await = index;
        }
        Err(fetch_error) => {
            error!(error=?fetch_error, "fetch-metadata-error");
        }
    }
}

fn spawn_metastore_polling_task(
    storage: Arc<dyn Storage>,
    index_id: String,
    metastore_weak: Weak<Mutex<FileBackedIndex>>,
    polling_interval: Duration,
) {
    tokio::task::spawn(async move {
        let mut interval = tokio::time::interval(polling_interval);
        interval.tick().await; //< this is to prevent fetch right after the first population of the data.
        while let Some(metadata_mutex) = metastore_weak.upgrade() {
            interval.tick().await;
            poll_metastore_once(&*storage, &index_id, &*metadata_mutex).await;
        }
    });
}

impl FileBackedMetastore {
    /// Creates a [`FileBackedMetastore`] for tests.
    #[doc(hidden)]
    pub fn for_test(storage: Arc<dyn Storage>) -> Self {
        Self {
            storage,
            per_index_metastores: Default::default(),
            indexes_states: Default::default(),
            polling_interval_opt: None,
        }
    }

    /// Sets the polling interval.
    ///
    /// Only newly accessed indexes will be affected by the change of this setting.
    pub fn set_polling_interval(&mut self, polling_interval_opt: Option<Duration>) {
        self.polling_interval_opt = polling_interval_opt;
    }

    #[cfg(test)]
    pub fn storage(&self) -> Arc<dyn Storage> {
        self.storage.clone()
    }

    /// Creates a [`FileBackedMetastore`] for a specified storage.
    /// Indexes states are immediately fetched from the storage.
    pub async fn try_new(storage: Arc<dyn Storage>) -> MetastoreResult<Self> {
        let indexes = fetch_indexes(&*storage).await?;
        Ok(Self {
            storage,
            per_index_metastores: Default::default(),
            indexes_states: RwLock::new(indexes),
            polling_interval_opt: None,
        })
    }

    /// Save index metadata in per_index_metastores and on the storage.
    async fn save_index_metadata(&self, index_metadata: IndexMetadata) -> MetastoreResult<()> {
        let index_id = index_metadata.index_id.clone();
        // We pick the outer lock here, so that we enter a critical section.
        let mut per_index_metastores_wlock = self.per_index_metastores.write().await;
        let exists = per_index_metastores_wlock.contains_key(&index_metadata.index_id)
            || index_exists(&*self.storage, &index_metadata.index_id).await?;
        if exists {
            return Err(MetastoreError::IndexAlreadyExists {
                index_id: index_metadata.index_id.clone(),
            });
        }
        let index = FileBackedIndex::from(index_metadata);
        put_index(&*self.storage, &index).await?;
        let index_mutex = Arc::new(Mutex::new(index));
        per_index_metastores_wlock.insert(index_id, index_mutex);
        Ok(())
    }

    async fn mutate_indexes_states(
        &self,
        mutation: impl FnOnce(&mut HashMap<String, IndexState>) -> crate::MetastoreResult<()>,
    ) -> MetastoreResult<()> {
        let mut indexes_states_wlock = self.indexes_states.write().await;
        let mut indexes_states = indexes_states_wlock.clone();
        mutation(&mut indexes_states)?;
        put_indexes(&*self.storage, &indexes_states)
            .await
            .map(|_| {
                *indexes_states_wlock = indexes_states;
                ()
            })
    }

    async fn mutate(
        &self,
        index_id: &str,
        mutation: impl FnOnce(&mut FileBackedIndex) -> crate::MetastoreResult<bool>,
    ) -> MetastoreResult<()> {
        // On index mutation, first fix indexes state if it's in `Creating` and `Deleting` state.
        self.repair_index_state(index_id).await?;
        let mut locked_index = self.get_locked_index(index_id).await?;

        let mut index = locked_index.clone();
        let has_changed = mutation(&mut index)?;
        if !has_changed {
            return Ok(());
        }

        let put_result = put_index(&*self.storage, &index).await;
        match put_result {
            Ok(()) => {
                *locked_index = index;
                Ok(())
            }
            err @ Err(_) => {
                // For some of the error type here, we cannot know for sure
                // whether the content was written or not.
                //
                // Just to be sure, let's discard the cache.
                let mut per_index_metastores_wlock = self.per_index_metastores.write().await;

                // At this point, we hold both locks.
                per_index_metastores_wlock.remove(index_id);
                locked_index.discarded = true;

                err
            }
        }
    }

    async fn read<T, F>(&self, index_id: &str, view: F) -> MetastoreResult<T>
    where F: FnOnce(&FileBackedIndex) -> MetastoreResult<T> {
        let locked_index = self.get_locked_index(index_id).await?;
        view(&*locked_index)
    }

    /// Returns a valid metadataset that is locked.
    ///
    /// This function guarantees that the metadataset has not been
    /// marked as discarded.
    async fn get_locked_index(
        &self,
        index_id: &str,
    ) -> MetastoreResult<OwnedMutexGuard<FileBackedIndex>> {
        loop {
            let index_mutex = self.file_backed_index(index_id).await?;
            let index_lock = index_mutex.lock_owned().await;
            if !index_lock.discarded {
                return Ok(index_lock);
            }
        }
    }

    /// Returns an IndexView for the given index_id.
    ///
    /// If this is the first call during this instance for this
    /// `index_id`, a fetch to the storage will be initiated
    /// and might trigger an error.
    ///
    /// For a given index_id, only copies of the same index_view are returned.
    async fn file_backed_index(&self, index_id: &str) -> MetastoreResult<Arc<Mutex<FileBackedIndex>>> {
        {
            // Happy path!
            // If the object is already in our cache then we just return a copy
            let per_index_metastores = self.per_index_metastores.read().await;
            if let Some(index_view) = per_index_metastores.get(index_id) {
                return Ok(index_view.clone());
            }
        }

        // At this point we do not hold our mutex, so we need to do a little dance
        // to make sure we return the same instance.
        //
        // If there is an error here, note we do not return right away.
        // That's because we want to observe the property that after one success
        // all subsequent calls will succeed.
        let index_result = fetch_index(&*self.storage, index_id).await;

        // Here we retake the lock, still no io ongoing.
        let mut per_index_metastores_wlock = self.per_index_metastores.write().await;

        // At this point, some other client might have added another instance of the Metadataet in
        // the map. We want to avoid two copies to exist in the application, so we keep only
        // one.
        if let Some(index) = per_index_metastores_wlock.get(index_id) {
            return Ok(index.clone());
        }

        let index = index_result?;
        let index_mutex = Arc::new(Mutex::new(index));

        if let Some(polling_interval) = self.polling_interval_opt {
            spawn_metastore_polling_task(
                self.storage.clone(),
                index_id.to_string(),
                Arc::downgrade(&index_mutex),
                polling_interval,
            );
        }

        per_index_metastores_wlock.insert(index_id.to_string(), index_mutex.clone());
        Ok(index_mutex)
    }

    async fn repair_index_state(&self, index_id: &str) -> MetastoreResult<()> {
        {
            // Take a read lock, if the index is in `Creating` or `Deleting` state, then repair it. 
            let indexes_states_rlock = self.indexes_states.read().await;
            if let Some(index_state) = indexes_states_rlock.get(index_id) {
                match index_state {
                    IndexState::Alive => return Ok(()),
                    _ => ()
                }
            } else {
                // TODO: repair should raise error unless we are creating an index.
                return Ok(());
            }
        }
        // Repair index state.
        let mut indexes_states_wlock = self.indexes_states.write().await;
        let mut indexes_states = indexes_states_wlock.clone();
        match indexes_states.get(index_id) {
            Some(IndexState::Creating(index_metadata)) => {
                self.save_index_metadata(index_metadata.clone()).await?;
            },
            Some(IndexState::Deleting) => {
                indexes_states.remove(index_id);
            },
            _ => ()
        }
        put_indexes(&*self.storage, &indexes_states)
            .await
            .map(|_| {
                *indexes_states_wlock = indexes_states;
                ()
            })
    }

    // Helper used for testing to obtain the data associated with the given index.
    #[cfg(test)]
    async fn get_index(&self, index_id: &str) -> MetastoreResult<FileBackedIndex> {
        self.read(index_id, |index| Ok(index.clone())).await
    }

    #[cfg(test)]
    async fn index_exists(&self, index_id: &str) -> MetastoreResult<bool> {
        index_exists(&*self.storage, index_id).await
    }
}

#[async_trait]
impl Metastore for FileBackedMetastore {
    /// -------------------------------------------------------------------------------
    /// Mutations over the high-level index.
    async fn create_index(&self, index_metadata: IndexMetadata) -> MetastoreResult<()> {
        let index_id = index_metadata.index_id.clone();
        self.repair_index_state(&index_id).await?;
        // 1. Set index state to `Creating`.
        // TODO: raise exception if index is already in the map.
        self.mutate_indexes_states(|indexes_states| {
            indexes_states.insert(index_id.clone(), IndexState::Creating(index_metadata.clone()));
            Ok(())
        }).await?;

        // 2. Save index.
        self.save_index_metadata(index_metadata).await?;

        // 3. Set index state to `Alive`.
        self.mutate_indexes_states(|indexes_states| {
            indexes_states.insert(index_id.clone(), IndexState::Alive);
            Ok(())
        }).await?;

        Ok(())
    }

    async fn delete_index(&self, index_id: &str) -> MetastoreResult<()> {
        self.repair_index_state(index_id).await?;
        // 1. Set index state to `Deleting`.
        // TODO: raise exception if index is in the map?
        self.mutate_indexes_states(|indexes_states| {
            indexes_states.insert(index_id.to_string(), IndexState::Deleting);
            Ok(())
        }).await?;

        // 2. Delete index metadata.
        // We pick the outer lock here, so that we enter a critical section.
        let mut per_index_metastores_wlock = self.per_index_metastores.write().await;

        let delete_res = delete_index(&*self.storage, index_id).await;

        match &delete_res {
            Ok(()) |
            // If the index file does not exist, we still need to return an error,
            // but it makes sense to ensure that the index state is deleted.
            Err(MetastoreError::IndexDoesNotExist { .. }) => {
                per_index_metastores_wlock.remove(index_id);
                // 3. Delete index state.
                self.mutate_indexes_states(|indexes_states| {
                    indexes_states.remove(index_id);
                    Ok(())
                }).await?;
            },
            _ => {}
        }

        delete_res
    }

    /// -------------------------------------------------------------------------------
    /// Mutations over a single index

    async fn stage_split(
        &self,
        index_id: &str,
        split_metadata: SplitMetadata,
    ) -> MetastoreResult<()> {
        self.mutate(index_id, |index| {
            index.stage_split(split_metadata)?;
            Ok(true)
        })
        .await
    }

    async fn publish_splits<'a>(
        &self,
        index_id: &str,
        source_id: &str,
        split_ids: &[&'a str],
        checkpoint_delta: CheckpointDelta,
    ) -> MetastoreResult<()> {
        self.mutate(index_id, |index| {
            index.publish_splits(source_id, split_ids, checkpoint_delta)?;
            Ok(true)
        })
        .await
    }

    async fn replace_splits<'a>(
        &self,
        index_id: &str,
        new_split_ids: &[&'a str],
        replaced_split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        self.mutate(index_id, |index| {
            index.replace_splits(new_split_ids, replaced_split_ids)?;
            Ok(true)
        })
        .await
    }

    async fn mark_splits_for_deletion<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        self.mutate(index_id, |index| index.mark_splits_for_deletion(split_ids))
            .await
    }

    async fn delete_splits<'a>(
        &self,
        index_id: &str,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        self.mutate(index_id, |index| {
            index.delete_splits(split_ids)?;
            Ok(true)
        })
        .await
    }

    async fn add_source(&self, index_id: &str, source: SourceConfig) -> MetastoreResult<()> {
        self.mutate(index_id, |index| index.add_source(source))
            .await
    }

    async fn delete_source(&self, index_id: &str, source_id: &str) -> MetastoreResult<()> {
        self.mutate(index_id, |index| index.delete_source(source_id))
            .await
    }

    /// -------------------------------------------------------------------------------
    /// Read-only accessors

    async fn list_splits(
        &self,
        index_id: &str,
        state: SplitState,
        time_range_opt: Option<Range<i64>>,
        tags: Option<TagFilterAst>,
    ) -> MetastoreResult<Vec<Split>> {
        self.read(index_id, |index| {
            index.list_splits(state, time_range_opt, tags)
        })
        .await
    }

    async fn list_all_splits(&self, index_id: &str) -> MetastoreResult<Vec<Split>> {
        self.read(index_id, |index| index.list_all_splits()).await
    }

    async fn index_metadata(&self, index_id: &str) -> MetastoreResult<IndexMetadata> {
        self.read(index_id, |index| Ok(index.metadata().clone()))
            .await
    }

    async fn list_indexes(&self) -> MetastoreResult<Vec<IndexMetadata>> {
        let indexes = fetch_indexes(&*self.storage).await?;
        println!("{:?}", indexes);
        try_join_all(indexes.iter()
            .filter_map(|(index_id, index_state)| {
                match index_state {
                    IndexState::Alive => Some(index_id),
                    _ => None
                }
            })
            .map(|index_id| self.index_metadata(index_id))).await
    }

    fn uri(&self) -> String {
        self.storage.uri()
    }

    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.storage.check().await?;
        Ok(())
    }
}

#[cfg(test)]
#[async_trait]
impl crate::tests::test_suite::DefaultForTest for FileBackedMetastore {
    async fn default_for_test() -> Self {
        use quickwit_storage::RamStorage;
        FileBackedMetastore::for_test(Arc::new(RamStorage::default()))
    }
}

metastore_test_suite!(crate::FileBackedMetastore);

#[cfg(test)]
mod tests {
    use std::ops::RangeInclusive;
    use std::path::Path;
    use std::sync::Arc;

    use chrono::Utc;
    use futures::executor::block_on;
    use quickwit_storage::{MockStorage, RamStorage, Storage, StorageErrorKind};
    use rand::Rng;
    use tokio::time::Duration;
    use crate::tests::test_suite::DefaultForTest;

    use super::store_operations::{meta_path, put_index_given_index_id};
    use super::{FileBackedIndex, FileBackedMetastore};
    use crate::checkpoint::CheckpointDelta;
    use crate::{IndexMetadata, Metastore, MetastoreError, SplitMetadata, SplitState};

    #[tokio::test]
    async fn test_file_backed_metastore_index_exists() {
        let metastore = FileBackedMetastore::default_for_test().await;
        let index_id = "my-index";

        {
            // Check for the existence of index.
            let result = metastore.index_exists(index_id).await.unwrap();
            let expected = false;
            assert_eq!(result, expected);

            let index_metadata = IndexMetadata::for_test(index_id, "ram://indexes/my-index");

            // Create index
            metastore.create_index(index_metadata).await.unwrap();

            // Check for the existence of index.
            let result = metastore.index_exists(index_id).await.unwrap();
            let expected = true;
            assert_eq!(result, expected);
        }
    }

    #[tokio::test]
    async fn test_file_backed_metastore_get_index() {
        let metastore = FileBackedMetastore::default_for_test().await;
        let index_id = "my-index";

        {
            // Check for the existence of index.
            let result = metastore.index_exists(index_id).await.unwrap();
            let expected = false;
            assert_eq!(result, expected);

            let index_metadata = IndexMetadata::for_test(index_id, "ram://indexes/my-index");

            // Create index
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            // Check for the existence of index.
            let result = metastore.index_exists(index_id).await.unwrap();
            let expected = true;
            assert_eq!(result, expected);

            // Open index and check its metadata
            let created_index = metastore.get_index(index_id).await.unwrap();
            assert_eq!(created_index.index_id(), index_metadata.index_id);
            assert_eq!(created_index.metadata().index_uri, index_metadata.index_uri);
            // Open a non-existent index.
            let metastore_error = metastore.get_index("non-existent-index").await.unwrap_err();
            assert!(matches!(
                metastore_error,
                MetastoreError::IndexDoesNotExist { .. }
            ));
        }
    }

    #[tokio::test]
    async fn test_file_backed_metastore_storage_failing() {
        // The file-backed metastore should not update its internal state if the storage fails.
        let mut mock_storage = MockStorage::default();

        let current_timestamp = Utc::now().timestamp();

        let ram_storage = RamStorage::default();
        let ram_storage_clone = ram_storage.clone();

        mock_storage // remove this if we end up changing the semantics of create.
            .expect_exists()
            .returning(|_| Ok(false));
        mock_storage
            .expect_put()
            .times(4)
            .returning(move |path, put_payload| {
                assert!(path == Path::new("indexes.json") || path == meta_path("my-index"));
                block_on(ram_storage_clone.put(path, put_payload))
            });
        mock_storage
            .expect_get_all()
            .times(1)
            .returning(move |path| block_on(ram_storage.get_all(path)));
        mock_storage.expect_put().times(1).returning(|_uri, _| {
            Err(StorageErrorKind::Io
                .with_error(anyhow::anyhow!("Oops. Some network problem maybe?")))
        });
        let metastore = FileBackedMetastore::for_test(Arc::new(mock_storage));

        let index_id = "my-index";
        let source_id = "my-source";
        let split_id = "split-one";
        let split_metadata = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: split_id.to_string(),
            num_docs: 1,
            original_size_in_bytes: 2,
            time_range: Some(RangeInclusive::new(0, 99)),
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        let index_metadata = IndexMetadata::for_test(index_id, "ram://indexes/my-index");

        // create index
        metastore.create_index(index_metadata).await.unwrap();

        // stage split
        metastore
            .stage_split(index_id, split_metadata)
            .await
            .unwrap();

        // publish split fails
        let err = metastore
            .publish_splits(index_id, source_id, &[split_id], CheckpointDelta::default())
            .await;
        assert!(err.is_err());

        // empty
        let split = metastore
            .list_splits(index_id, SplitState::Published, None, None)
            .await
            .unwrap();
        assert!(split.is_empty());

        // not empty
        let split = metastore
            .list_splits(index_id, SplitState::Staged, None, None)
            .await
            .unwrap();
        assert!(!split.is_empty());
    }

    #[tokio::test]
    async fn test_file_backed_metastore_get_index_checks_for_inconsistent_index_id(
    ) -> crate::MetastoreResult<()> {
        let metastore = FileBackedMetastore::default_for_test().await;
        let storage = metastore.storage();
        let index_id = "my-index";
        let index_metadata =
            IndexMetadata::for_test("my-inconsistent-index", "ram://indexes/my-index");

        // Put inconsistent index into storage.
        let index = FileBackedIndex::from(index_metadata);

        put_index_given_index_id(&*storage, &index, index_id).await?;

        // Getting index with inconsistent index ID should raise an error.
        let metastore_error = metastore.get_index(index_id).await.unwrap_err();
        assert!(matches!(
            metastore_error,
            MetastoreError::InternalError { .. }
        ));

        Ok(())
    }

    #[tokio::test]
    async fn test_file_backed_metastore_wrt_directly_visible() -> crate::MetastoreResult<()> {
        let metastore = FileBackedMetastore::default_for_test().await;

        let index_id = "my-index";
        let index_metadata = IndexMetadata::for_test(index_id, "ram://indexes/my-index");
        metastore.create_index(index_metadata).await?;

        assert!(metastore.list_all_splits(index_id).await?.is_empty());
        let split_metadata = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: "split1".to_string(),
            num_docs: 1,
            original_size_in_bytes: 2,
            time_range: Some(0..=99),
            ..Default::default()
        };
        assert!(metastore.list_all_splits("my-index").await?.is_empty());
        metastore.stage_split(index_id, split_metadata).await?;
        assert_eq!(metastore.list_all_splits(index_id).await?.len(), 1);
        Ok(())
    }

    #[tokio::test]
    async fn test_file_backed_metastore_polling() -> crate::MetastoreResult<()> {
        let storage = Arc::new(RamStorage::default());

        let metastore_wrt = FileBackedMetastore::for_test(storage.clone());
        let mut metastore_read = FileBackedMetastore::for_test(storage);
        let polling_interval = Duration::from_millis(20);
        metastore_read.set_polling_interval(Some(polling_interval));

        let index_id = "my-index";
        let index_metadata = IndexMetadata::for_test(index_id, "ram://indexes/my-index");
        metastore_wrt.create_index(index_metadata).await?;

        assert!(metastore_wrt.list_all_splits(index_id).await?.is_empty());
        let split_metadata = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: "split1".to_string(),
            num_docs: 1,
            original_size_in_bytes: 2,
            time_range: Some(0..=99),
            ..Default::default()
        };
        assert!(metastore_read.list_all_splits("my-index").await?.is_empty());
        metastore_wrt.stage_split(index_id, split_metadata).await?;
        assert!(metastore_read.list_all_splits("my-index").await?.is_empty());
        for _ in 0..10 {
            tokio::time::sleep(polling_interval).await;
            if !metastore_read.list_all_splits("my-index").await?.is_empty() {
                return Ok(());
            }
        }
        panic!("The metastore should have been updated.");
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 3)]
    async fn test_file_backed_metastore_race_condition() {
        let metastore = Arc::new(FileBackedMetastore::default_for_test().await);
        let index_id = "my-index";
        let source_id = "my-source";

        let index_metadata = IndexMetadata::for_test(index_id, "ram://indexes/my-index");

        // Create index
        metastore.create_index(index_metadata).await.unwrap();

        // Stage the split in multiple threads
        let mut handles = Vec::new();
        let mut random_generator = rand::thread_rng();
        for i in 1..=20 {
            let sleep_duration = Duration::from_millis(random_generator.gen_range(0..=200));
            let metastore = metastore.clone();
            let current_timestamp = Utc::now().timestamp();
            let handle = tokio::spawn(async move {
                let split_metadata = SplitMetadata {
                    footer_offsets: 1000..2000,
                    split_id: format!("split-{}", i),
                    num_docs: 1,
                    original_size_in_bytes: 2,
                    time_range: Some(RangeInclusive::new(0, 99)),
                    create_timestamp: current_timestamp,
                    ..Default::default()
                };
                // stage split
                metastore
                    .stage_split(index_id, split_metadata)
                    .await
                    .unwrap();

                tokio::time::sleep(sleep_duration).await;

                // publish split
                let split_id = format!("split-{}", i);
                metastore
                    .publish_splits(
                        index_id,
                        source_id,
                        &[&split_id],
                        CheckpointDelta::default(),
                    )
                    .await
                    .unwrap();
            });
            handles.push(handle);
        }

        futures::future::try_join_all(handles).await.unwrap();

        let splits = metastore
            .list_splits(index_id, SplitState::Published, None, None)
            .await
            .unwrap();

        // Make sure that all 20 splits are in `Published`
        assert_eq!(splits.len(), 20);
    }
}
