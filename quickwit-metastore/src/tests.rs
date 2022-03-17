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

#[cfg(test)]
pub mod test_suite {
    use std::collections::{BTreeSet, HashSet};
    use std::ops::{Range, RangeInclusive};

    use async_trait::async_trait;
    use chrono::Utc;
    use itertools::Itertools;
    use quickwit_config::{SourceConfig, SourceParams};
    use quickwit_doc_mapper::tag_pruning::{no_tag, tag, TagFilterAst};
    use tokio::time::{sleep, Duration};

    use crate::checkpoint::{CheckpointDelta, SourceCheckpoint};
    use crate::{IndexMetadata, Metastore, MetastoreError, SplitMetadata, SplitState};

    #[async_trait]
    pub trait DefaultForTest {
        async fn default_for_test() -> Self;
    }

    fn to_set(tags: &[&str]) -> BTreeSet<String> {
        tags.iter().map(ToString::to_string).collect()
    }

    fn to_hash_set(split_ids: &[&str]) -> std::collections::HashSet<String> {
        split_ids.iter().map(ToString::to_string).collect()
    }

    async fn cleanup_index(metastore: &dyn Metastore, index_id: &str) {
        // List all splits.
        let all_splits = metastore.list_all_splits(index_id).await.unwrap();

        if !all_splits.is_empty() {
            let all_split_ids = all_splits
                .iter()
                .map(|meta| meta.split_id())
                .collect::<Vec<_>>();

            // Mark splits for deletion.
            metastore
                .mark_splits_for_deletion(index_id, &all_split_ids)
                .await
                .unwrap();

            // Delete splits.
            metastore
                .delete_splits(index_id, &all_split_ids)
                .await
                .unwrap();
        }

        // Delete index.
        let result = metastore.delete_index(index_id).await.unwrap();
        assert!(matches!(result, ()));
    }

    pub async fn test_metastore_add_source<MetastoreToTest: Metastore + DefaultForTest>(
    ) -> anyhow::Result<()> {
        let metastore = MetastoreToTest::default_for_test().await;

        let index_id = "test-metastore-add-source";
        let index_uri = "ram://indexes/test-metastore-add-source";
        let index_metadata = IndexMetadata::for_test(index_id, index_uri);

        metastore.create_index(index_metadata.clone()).await?;

        let source_id = "test-metastore-add-source--void-source-id";

        let source = SourceConfig {
            source_id: source_id.to_string(),
            source_params: SourceParams::void(),
        };

        assert_eq!(
            metastore
                .index_metadata(index_id)
                .await?
                .checkpoint
                .source_checkpoint(source_id),
            None
        );

        metastore.add_source(index_id, source.clone()).await?;

        let index_metadata = metastore.index_metadata(index_id).await?;

        assert_eq!(
            index_metadata.checkpoint.source_checkpoint(source_id),
            Some(&SourceCheckpoint::default())
        );

        let sources = index_metadata.sources;
        assert_eq!(sources.len(), 1);
        assert!(sources.contains_key(source_id));
        assert_eq!(sources.get(source_id).unwrap().source_id, source_id);
        assert_eq!(sources.get(source_id).unwrap().source_type(), "void");

        assert!(matches!(
            metastore
                .add_source(index_id, source.clone())
                .await
                .unwrap_err(),
            MetastoreError::SourceAlreadyExists { .. }
        ));
        assert!(matches!(
            metastore
                .add_source("index-id-does-not-exist", source)
                .await
                .unwrap_err(),
            MetastoreError::IndexDoesNotExist { .. }
        ));
        Ok(())
    }

    pub async fn test_metastore_delete_source<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let index_id = "test-metastore-delete-source";
        let index_uri = "ram://indexes/test-metastore-delete-source";
        let source_id = "test-metastore-delete-source--void-source-id";

        let source = SourceConfig {
            source_id: source_id.to_string(),
            source_params: SourceParams::void(),
        };

        let mut index_metadata = IndexMetadata::for_test(index_id, index_uri);
        index_metadata.sources.insert(source_id.to_string(), source);

        metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();
        metastore.delete_source(index_id, source_id).await.unwrap();

        let sources = metastore.index_metadata(index_id).await.unwrap().sources;
        assert!(sources.is_empty());

        assert!(matches!(
            metastore
                .delete_source(index_id, source_id)
                .await
                .unwrap_err(),
            MetastoreError::SourceDoesNotExist { .. }
        ));
        assert!(matches!(
            metastore
                .delete_source("index-id-does-not-exist", source_id)
                .await
                .unwrap_err(),
            MetastoreError::IndexDoesNotExist { .. }
        ));
    }

    pub async fn test_metastore_create_index<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let index_id = "create-index-index";
        let index_metadata = IndexMetadata::for_test(index_id, "ram://indexes/my-index");

        // Create an index
        let result = metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        cleanup_index(&metastore, index_id).await;
    }

    pub async fn test_metastore_delete_index<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let index_id = "delte-index-index";
        let index_metadata = IndexMetadata::for_test(index_id, "ram://indexes/my-index");

        // Delete a non-existent index
        let result = metastore
            .delete_index("non-existent-index")
            .await
            .unwrap_err();
        assert!(matches!(result, MetastoreError::IndexDoesNotExist { .. }));

        metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();

        // Delete an index
        let result = metastore.delete_index(index_id).await.unwrap();
        assert!(matches!(result, ()));
    }

    #[allow(unused_variables)]
    pub async fn test_metastore_index_metadata<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let index_id = "index-metadata-index";
        let index_metadata = IndexMetadata::for_test(index_id, "ram://indexes/my-index");

        // Get a non-existent index metadata
        let result = metastore
            .index_metadata("non-existent-index")
            .await
            .unwrap_err();
        assert!(matches!(result, MetastoreError::IndexDoesNotExist { .. }));

        metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();

        // Get an index metadata
        let result = metastore.index_metadata(index_id).await.unwrap();
        assert!(matches!(result, index_metadata));

        cleanup_index(&metastore, index_id).await;
    }

    pub async fn test_metastore_stage_split<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let current_timestamp = Utc::now().timestamp();

        let index_id = "stage-split-index";
        let index_metadata = IndexMetadata::for_test(index_id, "ram://indexes/my-index");

        let split_id = "stage-split-my-index-one";
        let split_metadata = SplitMetadata {
            split_id: split_id.to_string(),
            num_docs: 1,
            original_size_in_bytes: 2,
            time_range: Some(RangeInclusive::new(0, 99)),
            create_timestamp: current_timestamp,
            footer_offsets: 1000..2000,
            ..Default::default()
        };

        // Stage a split on a non-existent index
        let result = metastore
            .stage_split("non-existent-index", split_metadata.clone())
            .await
            .unwrap_err();
        assert!(matches!(result, MetastoreError::IndexDoesNotExist { .. }));

        metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();

        // Stage a split on an index
        let result = metastore
            .stage_split(index_id, split_metadata.clone())
            .await
            .unwrap();
        assert!(matches!(result, ()));

        // Stage a existent-split on an index
        let result = metastore
            .stage_split(index_id, split_metadata.clone())
            .await
            .unwrap_err();
        assert!(matches!(result, MetastoreError::InternalError { .. }));

        cleanup_index(&metastore, index_id).await;
    }

    pub async fn test_metastore_publish_splits<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let current_timestamp = Utc::now().timestamp();

        let index_id = "publish-splits-index";
        let source_id = "publish-splits-source";
        let index_metadata = IndexMetadata::for_test(index_id, "ram://indexes/my-index");

        let split_id_1 = "publish-splits-index-one";
        let split_metadata_1 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: split_id_1.to_string(),
            num_docs: 1,
            original_size_in_bytes: 2,
            time_range: Some(RangeInclusive::new(0, 99)),
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        let split_id_2 = "publish-splits-index-two";
        let split_metadata_2 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: split_id_2.to_string(),
            num_docs: 5,
            original_size_in_bytes: 6,
            time_range: Some(RangeInclusive::new(30, 99)),
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        // Publish a split on a non-existent index
        {
            let result = metastore
                .publish_splits(
                    "non-existent-index",
                    source_id,
                    &["non-existent-split"],
                    CheckpointDelta::from(1..10),
                )
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::IndexDoesNotExist { .. }));
        }

        // Publish a non-existent split on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            let result = metastore
                .publish_splits(
                    index_id,
                    source_id,
                    &["non-existent-split"],
                    CheckpointDelta::default(),
                )
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::SplitsDoNotExist { .. }));

            cleanup_index(&metastore, index_id).await;
        }

        // Publish a staged split on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            let result = metastore
                .publish_splits(
                    index_id,
                    source_id,
                    &[split_id_1],
                    CheckpointDelta::default(),
                )
                .await
                .unwrap();
            assert!(matches!(result, ()));

            cleanup_index(&metastore, index_id).await;
        }

        // Publish a published split on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .publish_splits(
                    index_id,
                    source_id,
                    &[split_id_1],
                    CheckpointDelta::from(1..12),
                )
                .await
                .unwrap();

            let publish_error = metastore
                .publish_splits(
                    index_id,
                    source_id,
                    &[split_id_1],
                    CheckpointDelta::from(1..12),
                )
                .await
                .unwrap_err();
            assert!(matches!(
                publish_error,
                MetastoreError::IncompatibleCheckpointDelta(_)
            ));

            cleanup_index(&metastore, index_id).await;
        }

        // Publish a non-staged split on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .publish_splits(
                    index_id,
                    source_id,
                    &[split_id_1],
                    CheckpointDelta::from(12..15),
                )
                .await
                .unwrap();

            metastore
                .mark_splits_for_deletion(index_id, &[split_id_1])
                .await
                .unwrap();

            let result = metastore
                .publish_splits(
                    index_id,
                    source_id,
                    &[split_id_1],
                    CheckpointDelta::from(15..18),
                )
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::SplitsNotStaged { .. }));

            cleanup_index(&metastore, index_id).await;
        }

        // Publish a staged split and non-existent split on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            let result = metastore
                .publish_splits(
                    index_id,
                    source_id,
                    &[split_id_1, "non-existent-split"],
                    CheckpointDelta::from(15..18),
                )
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::SplitsDoNotExist { .. }));

            cleanup_index(&metastore, index_id).await;
        }

        // Publish a published split and non-existant split on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            let result = metastore
                .publish_splits(
                    index_id,
                    source_id,
                    &[split_id_1],
                    CheckpointDelta::from(15..18),
                )
                .await
                .unwrap();
            assert!(matches!(result, ()));

            let result = metastore
                .publish_splits(
                    index_id,
                    source_id,
                    &[split_id_1, "non-existent-split"],
                    CheckpointDelta::from(18..24),
                )
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::SplitsDoNotExist { .. }));

            cleanup_index(&metastore, index_id).await;
        }

        // Publish a non-staged split and non-existant split on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .publish_splits(
                    index_id,
                    source_id,
                    &[split_id_1],
                    CheckpointDelta::from(18..24),
                )
                .await
                .unwrap();

            metastore
                .mark_splits_for_deletion(index_id, &[split_id_1])
                .await
                .unwrap();

            let result = metastore
                .publish_splits(
                    index_id,
                    source_id,
                    &[split_id_1, "non-existent-split"],
                    CheckpointDelta::from(24..26),
                )
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::SplitsDoNotExist { .. }));

            cleanup_index(&metastore, index_id).await;
        }

        // Publish staged splits on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_2.clone())
                .await
                .unwrap();

            let result = metastore
                .publish_splits(
                    index_id,
                    source_id,
                    &[split_id_1, split_id_2],
                    CheckpointDelta::from(24..26),
                )
                .await
                .unwrap();
            assert!(matches!(result, ()));

            cleanup_index(&metastore, index_id).await;
        }

        // Publish a staged split and published split on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_2.clone())
                .await
                .unwrap();

            metastore
                .publish_splits(
                    index_id,
                    source_id,
                    &[split_id_2],
                    CheckpointDelta::from(26..28),
                )
                .await
                .unwrap();

            let result = metastore
                .publish_splits(
                    index_id,
                    source_id,
                    &[split_id_1, split_id_2],
                    CheckpointDelta::from(28..30),
                )
                .await
                .unwrap();
            assert!(matches!(result, ()));

            cleanup_index(&metastore, index_id).await;
        }

        // Publish published splits on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_2.clone())
                .await
                .unwrap();

            metastore
                .publish_splits(
                    index_id,
                    source_id,
                    &[split_id_1, split_id_2],
                    CheckpointDelta::from(30..31),
                )
                .await
                .unwrap();

            let publish_error = metastore
                .publish_splits(
                    index_id,
                    source_id,
                    &[split_id_1, split_id_2],
                    CheckpointDelta::from(30..31),
                )
                .await
                .unwrap_err();
            assert!(matches!(
                publish_error,
                MetastoreError::IncompatibleCheckpointDelta(_)
            ));

            cleanup_index(&metastore, index_id).await;
        }
    }

    pub async fn test_metastore_replace_splits<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let current_timestamp = Utc::now().timestamp();

        let index_id = "replace_splits-index";
        let source_id = "replace_splits-source";
        let index_metadata = IndexMetadata::for_test(index_id, "ram://indexes/my-index");

        let split_id_1 = "replace_splits-index-one";
        let split_metadata_1 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: split_id_1.to_string(),
            num_docs: 1,
            original_size_in_bytes: 2,
            time_range: None,
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        let split_id_2 = "replace_splits-index-two";
        let split_metadata_2 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: split_id_2.to_string(),
            num_docs: 5,
            original_size_in_bytes: 6,
            time_range: None,
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        let split_id_3 = "replace_splits-index-three";
        let split_metadata_3 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: split_id_3.to_string(),
            num_docs: 5,
            original_size_in_bytes: 6,
            time_range: None,
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        // Replace splits on a non-existent index
        {
            let result = metastore
                .replace_splits(
                    "non-existent-index",
                    &["non-existent-split-one"],
                    &["non-existent-split-two"],
                )
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::IndexDoesNotExist { .. }));
        }

        // Replace a non-existent split on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            let result = metastore
                .replace_splits(
                    index_id,
                    &["non-existent-split"],
                    &["non-existent-split-two"],
                )
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::SplitsDoNotExist { .. }));

            cleanup_index(&metastore, index_id).await;
        }

        // Replace a publish split with a non existing split
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .publish_splits(
                    index_id,
                    source_id,
                    &[split_id_1],
                    CheckpointDelta::default(),
                )
                .await
                .unwrap();

            let result = metastore
                .replace_splits(index_id, &[split_id_2], &[split_id_1])
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::SplitsDoNotExist { .. }));

            cleanup_index(&metastore, index_id).await;
        }

        // Replace a publish split with a deleted split
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_2.clone())
                .await
                .unwrap();

            metastore
                .publish_splits(
                    index_id,
                    source_id,
                    &[split_id_1, split_id_2],
                    CheckpointDelta::default(),
                )
                .await
                .unwrap();

            metastore
                .mark_splits_for_deletion(index_id, &[split_id_2])
                .await
                .unwrap();

            let result = metastore
                .replace_splits(index_id, &[split_id_2], &[split_id_1])
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::SplitsNotStaged { .. }));

            cleanup_index(&metastore, index_id).await;
        }

        // Replace a publish split with mixed splits
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .publish_splits(
                    index_id,
                    source_id,
                    &[split_id_1],
                    CheckpointDelta::default(),
                )
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_2.clone())
                .await
                .unwrap();

            let result = metastore
                .replace_splits(index_id, &[split_id_2, split_id_3], &[split_id_1])
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::SplitsDoNotExist { .. }));

            cleanup_index(&metastore, index_id).await;
        }

        // Replace a publish split with staged splits
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .publish_splits(
                    index_id,
                    source_id,
                    &[split_id_1],
                    CheckpointDelta::default(),
                )
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_2.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_3.clone())
                .await
                .unwrap();

            let result = metastore
                .replace_splits(index_id, &[split_id_2, split_id_3], &[split_id_1])
                .await
                .unwrap();
            assert!(matches!(result, ()));

            cleanup_index(&metastore, index_id).await;
        }
    }

    pub async fn test_metastore_mark_splits_for_deletion<
        MetastoreToTest: Metastore + DefaultForTest,
    >() {
        let metastore = MetastoreToTest::default_for_test().await;

        let current_timestamp = Utc::now().timestamp();

        let index_id = "mark-splits-as-deleted-my-index";
        let index_metadata = IndexMetadata::for_test(index_id, "ram://indexes/my-index");
        let split_id_1 = "mark-splits-as-deleted-my-index-one";
        let split_metadata_1 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: split_id_1.to_string(),
            num_docs: 1,
            original_size_in_bytes: 2,
            time_range: Some(RangeInclusive::new(0, 99)),
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        // Mark a split for deletion on a non-existent index
        {
            let result = metastore
                .mark_splits_for_deletion("non-existent-index", &["non-existent-split"])
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::IndexDoesNotExist { .. }));
        }

        // Mark a non-existent split for deletion on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            let result = metastore
                .mark_splits_for_deletion(index_id, &["non-existent-split"])
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::SplitsDoNotExist { .. }));

            cleanup_index(&metastore, index_id).await;
        }

        // Mark an existent split for deletion on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            let result = metastore
                .mark_splits_for_deletion(index_id, &[split_id_1])
                .await
                .unwrap();
            assert!(matches!(result, ()));

            cleanup_index(&metastore, index_id).await;
        }
    }

    pub async fn test_metastore_delete_splits<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let current_timestamp = Utc::now().timestamp();

        let index_id = "delete-splits-index";
        let source_id = "delete-splits-source";
        let index_metadata = IndexMetadata::for_test(index_id, "ram://indexes/my-index");

        let split_id_1 = "delete-splits-index-one";
        let split_metadata_1 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: split_id_1.to_string(),
            num_docs: 1,
            original_size_in_bytes: 2,
            time_range: Some(RangeInclusive::new(0, 99)),
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        // Delete a split marked for deletion on a non-existent index
        {
            let result = metastore
                .delete_splits("non-existent-index", &["non-existent-split"])
                .await
                .unwrap_err();
            println!("{:?}", result);
            assert!(matches!(result, MetastoreError::IndexDoesNotExist { .. }));
        }

        // Delete a non-existent split marked for deletion on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            let result = metastore
                .delete_splits(index_id, &["non-existent-split"])
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::SplitsDoNotExist { .. }));

            cleanup_index(&metastore, index_id).await;
        }

        // Delete a staged split on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            let result = metastore
                .delete_splits(index_id, &[split_id_1])
                .await
                .unwrap();
            assert!(matches!(result, ()));

            cleanup_index(&metastore, index_id).await;
        }

        // Delete a split that has been marked for deletion on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .mark_splits_for_deletion(index_id, &[split_id_1])
                .await
                .unwrap();

            let result = metastore
                .delete_splits(index_id, &[split_id_1])
                .await
                .unwrap();
            assert!(matches!(result, ()));

            cleanup_index(&metastore, index_id).await;
        }

        // Delete a split that is not marked for deletion
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .publish_splits(
                    index_id,
                    source_id,
                    &[split_id_1],
                    CheckpointDelta::from(0..5),
                )
                .await
                .unwrap();

            let result = metastore
                .delete_splits(index_id, &[split_id_1])
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::SplitsNotDeletable { .. }));

            cleanup_index(&metastore, index_id).await;
        }
    }

    pub async fn test_metastore_list_all_splits<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let current_timestamp = Utc::now().timestamp();

        let index_id = "list-all-splits-index";
        let index_metadata = IndexMetadata::for_test(index_id, "ram://indexes/my-index");

        let split_id_1 = "list-all-splits-index-one";
        let split_metadata_1 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: split_id_1.to_string(),
            num_docs: 1,
            original_size_in_bytes: 2,
            time_range: Some(RangeInclusive::new(0, 99)),
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        let split_metadata_2 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: "list-all-splits-index-two".to_string(),
            num_docs: 1,
            original_size_in_bytes: 2,
            time_range: Some(RangeInclusive::new(100, 199)),
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        let split_metadata_3 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: "list-all-splits-index-three".to_string(),
            num_docs: 1,
            original_size_in_bytes: 2,
            time_range: Some(RangeInclusive::new(200, 299)),
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        let split_metadata_4 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: "list-all-splits-index-four".to_string(),
            num_docs: 1,
            original_size_in_bytes: 2,
            time_range: Some(RangeInclusive::new(300, 399)),
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        let split_metadata_5 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: "list-all-splits-index-five".to_string(),
            num_docs: 1,
            original_size_in_bytes: 2,
            time_range: None,
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        // List all splits on a non-existent index
        {
            let result = metastore
                .list_all_splits("non-existent-index")
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::IndexDoesNotExist { .. }));
        }

        // List all splits on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_2.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_3.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_4.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_5.clone())
                .await
                .unwrap();

            let splits = metastore.list_all_splits(index_id).await.unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(split_ids.contains("list-all-splits-index-one"), true);
            assert_eq!(split_ids.contains("list-all-splits-index-two"), true);
            assert_eq!(split_ids.contains("list-all-splits-index-three"), true);
            assert_eq!(split_ids.contains("list-all-splits-index-four"), true);
            assert_eq!(split_ids.contains("list-all-splits-index-five"), true);

            cleanup_index(&metastore, index_id).await;
        }
    }

    pub async fn test_metastore_list_splits<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let current_timestamp = Utc::now().timestamp();

        let index_id = "list-splits-index";
        let index_metadata = IndexMetadata::for_test(index_id, "ram://indexes/my-index");

        let split_id_1 = "list-splits-one";
        let split_metadata_1 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: split_id_1.to_string(),
            num_docs: 1,
            original_size_in_bytes: 2,
            time_range: Some(RangeInclusive::new(0, 99)),
            create_timestamp: current_timestamp,
            tags: to_set(&["tag!", "tag:foo", "tag:bar"]),
            demux_num_ops: 0,
        };

        let split_metadata_2 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: "list-splits-two".to_string(),
            num_docs: 1,
            original_size_in_bytes: 2,
            time_range: Some(RangeInclusive::new(100, 199)),
            create_timestamp: current_timestamp,
            tags: to_set(&["tag!", "tag:bar"]),
            demux_num_ops: 0,
        };

        let split_metadata_3 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: "list-splits-three".to_string(),
            num_docs: 1,
            original_size_in_bytes: 2,
            time_range: Some(RangeInclusive::new(200, 299)),
            create_timestamp: current_timestamp,
            tags: to_set(&["tag!", "tag:foo", "tag:baz"]),
            demux_num_ops: 0,
        };

        let split_metadata_4 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: "list-splits-four".to_string(),
            num_docs: 1,
            original_size_in_bytes: 2,
            time_range: Some(RangeInclusive::new(300, 399)),
            create_timestamp: current_timestamp,
            tags: to_set(&["tag!", "tag:foo"]),
            demux_num_ops: 0,
        };

        let split_metadata_5 = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: "list-splits-five".to_string(),
            num_docs: 1,
            original_size_in_bytes: 2,
            time_range: None,
            create_timestamp: current_timestamp,
            tags: to_set(&["tag!", "tag:baz", "tag:biz"]),
            demux_num_ops: 0,
        };

        // List all splits on a non-existent index
        {
            let result = metastore
                .list_splits("non-existent-index", SplitState::Staged, None, None)
                .await
                .unwrap_err();
            assert!(matches!(result, MetastoreError::IndexDoesNotExist { .. }));
        }

        // List all splits on an index
        {
            metastore
                .create_index(index_metadata.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_1.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_2.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_3.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_4.clone())
                .await
                .unwrap();

            metastore
                .stage_split(index_id, split_metadata_5.clone())
                .await
                .unwrap();

            let time_range_opt = Some(Range {
                start: 0i64,
                end: 99i64,
            });
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, time_range_opt, None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&["list-splits-one", "list-splits-five"])
            );

            let time_range_opt = Some(Range {
                start: 200,
                end: i64::MAX,
            });
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, time_range_opt, None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&["list-splits-three", "list-splits-four", "list-splits-five"])
            );
            let time_range_opt = Some(Range {
                start: i64::MIN,
                end: 200,
            });
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, time_range_opt, None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&["list-splits-one", "list-splits-two", "list-splits-five"])
            );

            let range = Some(Range { start: 0, end: 100 });
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range, None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&["list-splits-one", "list-splits-five"])
            );

            let range = Some(Range { start: 0, end: 101 });
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range, None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&["list-splits-one", "list-splits-two", "list-splits-five"])
            );

            let range = Some(Range { start: 0, end: 199 });
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range, None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&["list-splits-one", "list-splits-two", "list-splits-five"])
            );

            let range = Some(Range { start: 0, end: 200 });
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range, None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&["list-splits-one", "list-splits-two", "list-splits-five"])
            );

            let range = Some(Range { start: 0, end: 201 });
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range, None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&[
                    "list-splits-one",
                    "list-splits-two",
                    "list-splits-three",
                    "list-splits-five"
                ])
            );

            let range = Some(Range { start: 0, end: 299 });
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range, None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&[
                    "list-splits-one",
                    "list-splits-two",
                    "list-splits-three",
                    "list-splits-five"
                ])
            );

            let range = Some(0..300);
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range, None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&[
                    "list-splits-one",
                    "list-splits-two",
                    "list-splits-three",
                    "list-splits-five"
                ])
            );

            let range = Some(0..301);
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range, None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&[
                    "list-splits-one",
                    "list-splits-two",
                    "list-splits-three",
                    "list-splits-four",
                    "list-splits-five"
                ])
            );

            let range = Some(301..400);
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range, None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();

            assert_eq!(
                split_ids,
                to_hash_set(&["list-splits-four", "list-splits-five"])
            );
            let range = Some(300..400);
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range, None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();

            assert_eq!(
                split_ids,
                to_hash_set(&["list-splits-four", "list-splits-five"])
            );
            let range = Some(299..400);
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range, None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&["list-splits-three", "list-splits-four", "list-splits-five"])
            );

            let range = Some(201..400);
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range, None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&["list-splits-three", "list-splits-four", "list-splits-five"])
            );

            let range = Some(200..400);
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range, None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();

            assert_eq!(
                split_ids,
                to_hash_set(&["list-splits-three", "list-splits-four", "list-splits-five"])
            );

            let range = Some(199..400);
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range, None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&[
                    "list-splits-two",
                    "list-splits-three",
                    "list-splits-four",
                    "list-splits-five"
                ])
            );

            let range = Some(101..400);
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range, None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&[
                    "list-splits-two",
                    "list-splits-three",
                    "list-splits-four",
                    "list-splits-five"
                ])
            );

            let range = Some(101..400);
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range, None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&[
                    "list-splits-two",
                    "list-splits-three",
                    "list-splits-four",
                    "list-splits-five"
                ])
            );

            let range = Some(100..400);
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range, None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();

            assert_eq!(
                split_ids,
                to_hash_set(&[
                    "list-splits-two",
                    "list-splits-three",
                    "list-splits-four",
                    "list-splits-five"
                ])
            );

            let range = Some(99..400);
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range, None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&[
                    "list-splits-one",
                    "list-splits-two",
                    "list-splits-three",
                    "list-splits-four",
                    "list-splits-five"
                ])
            );

            let range = Some(1000..1100);
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range, None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(split_ids, to_hash_set(&["list-splits-five"]));

            // add a split without tag
            let split_metadata_6 = SplitMetadata {
                footer_offsets: 1000..2000,
                split_id: "list-splits-six".to_string(),
                num_docs: 1,
                original_size_in_bytes: 2,
                time_range: None,
                create_timestamp: current_timestamp,
                tags: to_set(&[]),
                demux_num_ops: 0,
            };
            metastore
                .stage_split(index_id, split_metadata_6.clone())
                .await
                .unwrap();

            let range = None;
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range, None)
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|metadata| metadata.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&[
                    "list-splits-one",
                    "list-splits-two",
                    "list-splits-three",
                    "list-splits-four",
                    "list-splits-five",
                    "list-splits-six",
                ])
            );

            let range = None;
            let tag_filter_ast = TagFilterAst::Or(vec![
                TagFilterAst::Or(vec![no_tag("tag!"), tag("tag:bar")]),
                TagFilterAst::Or(vec![no_tag("tag!"), tag("tag:baz")]),
            ]);
            let splits = metastore
                .list_splits(index_id, SplitState::Staged, range, Some(tag_filter_ast))
                .await
                .unwrap();
            let split_ids: HashSet<String> = splits
                .into_iter()
                .map(|meta| meta.split_id().to_string())
                .collect();
            assert_eq!(
                split_ids,
                to_hash_set(&[
                    "list-splits-one",
                    "list-splits-two",
                    "list-splits-three",
                    "list-splits-five",
                    "list-splits-six",
                ])
            );
            cleanup_index(&metastore, index_id).await;
        }
    }

    pub async fn test_metastore_split_update_timestamp<
        MetastoreToTest: Metastore + DefaultForTest,
    >() {
        let metastore = MetastoreToTest::default_for_test().await;

        let mut current_timestamp = Utc::now().timestamp();

        let index_id = "split-update-timestamp-index";
        let source_id = "split-update-timestamp-source";
        let index_metadata = IndexMetadata::for_test(index_id, "ram://indexes/my-index");

        let split_id = "split-update-timestamp-one";
        let split_metadata = SplitMetadata {
            footer_offsets: 1000..2000,
            split_id: split_id.to_string(),
            num_docs: 1,
            original_size_in_bytes: 2,
            time_range: Some(RangeInclusive::new(0, 99)),
            create_timestamp: current_timestamp,
            ..Default::default()
        };

        // Create an index
        metastore
            .create_index(index_metadata.clone())
            .await
            .unwrap();

        // wait for 1s, stage split & check `update_timestamp`
        sleep(Duration::from_secs(1)).await;
        metastore
            .stage_split(index_id, split_metadata.clone())
            .await
            .unwrap();
        let split_meta = metastore.list_all_splits(index_id).await.unwrap()[0].clone();
        assert!(split_meta.update_timestamp > current_timestamp);
        assert!(
            metastore
                .index_metadata(index_id)
                .await
                .unwrap()
                .update_timestamp
                > current_timestamp
        );

        current_timestamp = split_meta.update_timestamp;

        // wait for 1s, publish split & check `update_timestamp`
        sleep(Duration::from_secs(1)).await;
        metastore
            .publish_splits(
                index_id,
                source_id,
                &[split_id],
                CheckpointDelta::from(0..5),
            )
            .await
            .unwrap();
        let split_meta = metastore.list_all_splits(index_id).await.unwrap()[0].clone();
        assert!(split_meta.update_timestamp > current_timestamp);
        assert!(
            metastore
                .index_metadata(index_id)
                .await
                .unwrap()
                .update_timestamp
                > current_timestamp
        );

        current_timestamp = split_meta.update_timestamp;

        // wait for 1s, mark split for deletion & check `update_timestamp`
        sleep(Duration::from_secs(1)).await;
        metastore
            .mark_splits_for_deletion(index_id, &[split_id])
            .await
            .unwrap();
        let split_meta = metastore.list_all_splits(index_id).await.unwrap()[0].clone();
        assert!(split_meta.update_timestamp > current_timestamp);
        assert!(
            metastore
                .index_metadata(index_id)
                .await
                .unwrap()
                .update_timestamp
                > current_timestamp
        );

        current_timestamp = split_meta.update_timestamp;

        // wait for 1s, delete split & check the index `update_timestamp`
        sleep(Duration::from_secs(1)).await;
        metastore
            .delete_splits(index_id, &[split_id])
            .await
            .unwrap();
        assert!(
            metastore
                .index_metadata(index_id)
                .await
                .unwrap()
                .update_timestamp
                > current_timestamp
        );

        cleanup_index(&metastore, index_id).await;
    }

    #[allow(unused_variables)]
    pub async fn test_metastore_list_indexes<MetastoreToTest: Metastore + DefaultForTest>() {
        let metastore = MetastoreToTest::default_for_test().await;

        let index_id_1 = "index-metadata-list-indexes-1";
        let index_id_2 = "index-metadata-list-indexes-2";
        let index_metadata_1 = IndexMetadata::for_test(index_id_1, "ram://indexes/my-index");
        let index_metadata_2 = IndexMetadata::for_test(index_id_2, "ram://indexes/my-index");
        let index_ids = vec![index_id_1, index_id_2];

        // Get a non-existent index metadata
        let result = metastore
            .list_indexes()
            .await
            .unwrap()
            .into_iter()
            .filter(|index_metadata| index_ids.contains(&index_metadata.index_id.as_str()))
            .collect_vec();
        assert!(result.is_empty());

        metastore
            .create_index(index_metadata_1.clone())
            .await
            .unwrap();
        metastore
            .create_index(index_metadata_2.clone())
            .await
            .unwrap();

        // Get an index metadata
        let result = metastore
            .list_indexes()
            .await
            .unwrap()
            .into_iter()
            .filter(|index_metadata| index_ids.contains(&index_metadata.index_id.as_str()))
            .collect_vec();
        assert_eq!(2, result.len());

        cleanup_index(&metastore, index_id_1).await;
        cleanup_index(&metastore, index_id_2).await;

        // Check that no index is left.
        let result = metastore
            .list_indexes()
            .await
            .unwrap()
            .into_iter()
            .filter(|index_metadata| index_ids.contains(&index_metadata.index_id.as_str()))
            .collect_vec();
        assert!(result.is_empty());
    }
}

macro_rules! metastore_test_suite {
    ($metastore_type:ty) => {
        #[cfg(test)]
        mod common_tests {


            #[tokio::test]
            async fn test_metastore_create_index() {
                crate::tests::test_suite::test_metastore_create_index::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_delete_index() {
                crate::tests::test_suite::test_metastore_delete_index::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_index_metadata() {
                crate::tests::test_suite::test_metastore_index_metadata::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_list_indexes() {
                crate::tests::test_suite::test_metastore_list_indexes::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_stage_split() {
                crate::tests::test_suite::test_metastore_stage_split::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_publish_splits() {
                crate::tests::test_suite::test_metastore_publish_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_replace_splits() {
                crate::tests::test_suite::test_metastore_replace_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_mark_splits_for_deletion() {
                crate::tests::test_suite::test_metastore_mark_splits_for_deletion::<$metastore_type>(
                )
                .await;
            }

            #[tokio::test]
            async fn test_metastore_delete_splits() {
                crate::tests::test_suite::test_metastore_delete_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_list_all_splits() {
                crate::tests::test_suite::test_metastore_list_all_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_list_splits() {
                crate::tests::test_suite::test_metastore_list_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_split_update_timestamp() {
                crate::tests::test_suite::test_metastore_split_update_timestamp::<$metastore_type>(
                )
                .await;
            }

            #[tokio::test]
            async fn test_metastore_add_source() -> anyhow::Result<()> {
                crate::tests::test_suite::test_metastore_add_source::<$metastore_type>().await
            }

            #[tokio::test]
            async fn test_metastore_delete_source() {
                crate::tests::test_suite::test_metastore_delete_source::<$metastore_type>().await;
            }
        }
    };
}

#[cfg(feature = "postgres")]
macro_rules! metastore_test_suite_for_postgresql {
    ($metastore_type:ty) => {
        #[cfg(test)]
        mod common_tests {
            #[tokio::test]
            async fn test_metastore_create_index() {
                crate::tests::test_suite::test_metastore_create_index::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_delete_index() {
                crate::tests::test_suite::test_metastore_delete_index::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_index_metadata() {
                crate::tests::test_suite::test_metastore_index_metadata::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_list_indexes() {
                crate::tests::test_suite::test_metastore_list_indexes::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_stage_split() {
                crate::tests::test_suite::test_metastore_stage_split::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_publish_splits() {
                crate::tests::test_suite::test_metastore_publish_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_replace_splits() {
                crate::tests::test_suite::test_metastore_replace_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_mark_splits_for_deletion() {
                crate::tests::test_suite::test_metastore_mark_splits_for_deletion::<$metastore_type>(
                )
                .await;
            }

            #[tokio::test]
            async fn test_metastore_delete_splits() {
                crate::tests::test_suite::test_metastore_delete_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_list_all_splits() {
                crate::tests::test_suite::test_metastore_list_all_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_list_splits() {
                crate::tests::test_suite::test_metastore_list_splits::<$metastore_type>().await;
            }

            #[tokio::test]
            async fn test_metastore_split_update_timestamp() {
                crate::tests::test_suite::test_metastore_split_update_timestamp::<$metastore_type>(
                )
                .await;
            }
        }
    };
}
