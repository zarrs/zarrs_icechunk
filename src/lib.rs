//! [`icechunk`] store support for the [`zarrs`](https://docs.rs/zarrs/latest/zarrs/index.html) crate.
//!
//! Icechunk is a transactional store that enables `git`-like version control of Zarr hierarchies.
//!
//! `zarrs_icechunk` can read data in a range of archival formats (e.g., [`netCDF4`](https://www.unidata.ucar.edu/software/netcdf/), [`HDF5`](https://www.hdfgroup.org/solutions/hdf5/), etc.) that are converted to `icechunk`-backed "virtual Zarr datacubes" via [`VirtualiZarr`](https://github.com/zarr-developers/VirtualiZarr) (example below).
//!
//! ## Version Compatibility Matrix
//!
#![doc = include_str!("../doc/version_compatibility_matrix.md")]
//!
//! ## Examples
//! ### Basic Usage and Version Control
//! ```
//! # use std::sync::Arc;
//! # use zarrs_storage::{AsyncWritableStorageTraits, StoreKey};
//! # use tokio::sync::RwLock;
//! # use std::collections::HashMap;
//! use icechunk::{Repository, RepositoryConfig, repository::VersionInfo};
//! use zarrs_icechunk::AsyncIcechunkStore;
//! # tokio_test::block_on(async {
//! // Create an icechunk repository
//! let storage = icechunk::new_in_memory_storage().await?;
//! let config = RepositoryConfig::default();
//! let repo = Repository::create(Some(config), storage, Default::default(), Default::default(), true).await?;
//!
//! // Do some array/metadata manipulation with zarrs, then commit a snapshot
//! let session = repo.writable_session("main").await?;
//! let store = Arc::new(AsyncIcechunkStore::new(session));
//! # let root_json = StoreKey::new("zarr.json").unwrap();
//! # store.set(&root_json, r#"{"zarr_format":3,"node_type":"group"}"#.into()).await?;
//! let snapshot0 = store.session().write().await.commit("Initial commit").execute().await?;
//!
//! // Do some more array/metadata manipulation, then commit another snapshot
//! let session = repo.writable_session("main").await?;
//! let store = Arc::new(AsyncIcechunkStore::new(session));
//! # store.set(&root_json, r#"{"zarr_format":3,"node_type":"group","attributes":{"a":"b"}}"#.into()).await?;
//! let snapshot1 = store.session().write().await.commit("Update data").execute().await?;
//!
//! // Checkout the first snapshot
//! let session = repo.readonly_session(&VersionInfo::SnapshotId(snapshot0)).await?;
//! let store = Arc::new(AsyncIcechunkStore::new(session));
//! # Ok::<_, Box<dyn std::error::Error>>(())
//! # }).unwrap();
//! ```
//!
//! ### Virtualise NetCDF as Zarr (via [`VirtualiZarr`](https://github.com/zarr-developers/VirtualiZarr))
//! Decode a virtual Zarr array [`/examples/data/test.icechunk.zarr`]:
//! ```bash
//! cargo run --example virtualizarr_netcdf
//! ```
//! This references `/examples/data/test[0,1].nc` hosted in this repository over HTTP.
//! [`/examples/data/test.icechunk.zarr`] was created with [`/examples/virtualizarr_netcdf.py`](https://github.com/zarrs/zarrs_icechunk/blob/main/examples/virtualizarr_netcdf.py).
//!
//! ## Licence
//! `zarrs_icechunk` is licensed under either of
//! - the Apache License, Version 2.0 [LICENSE-APACHE](https://docs.rs/crate/zarrs_icechunk/latest/source/LICENCE-APACHE) or <http://www.apache.org/licenses/LICENSE-2.0> or
//! - the MIT license [LICENSE-MIT](https://docs.rs/crate/zarrs_icechunk/latest/source/LICENCE-MIT) or <http://opensource.org/licenses/MIT>, at your option.
//!
//! [`/examples/data/test.icechunk.zarr`]: https://github.com/zarrs/zarrs_icechunk/tree/main/examples/data/test.icechunk.zarr

use std::sync::Arc;

use futures::{future, stream::FuturesUnordered, StreamExt, TryStreamExt};
pub use icechunk;

use tokio::sync::RwLock;
use zarrs_storage::{
    byte_range::{ByteRange, ByteRangeIterator},
    AsyncListableStorageTraits, AsyncMaybeBytesIterator, AsyncReadableStorageTraits,
    AsyncWritableStorageTraits, Bytes, MaybeBytes, OffsetBytesIterator, StorageError, StoreKey,
    StoreKeys, StoreKeysPrefixes, StorePrefix,
};

fn handle_err(err: icechunk::store::StoreError) -> StorageError {
    StorageError::Other(err.to_string())
}

/// Map [`icechunk::zarr::StoreError::NotFound`] to None, pass through other errors
fn handle_result_notfound<T>(
    result: Result<T, icechunk::store::StoreError>,
) -> Result<Option<T>, StorageError> {
    match result {
        Ok(result) => Ok(Some(result)),
        Err(err) => {
            if matches!(
                err.kind(),
                &icechunk::store::StoreErrorKind::NotFound { .. }
            ) {
                Ok(None)
            } else {
                Err(StorageError::Other(err.to_string()))
            }
        }
    }
}

fn handle_result<T>(result: Result<T, icechunk::store::StoreError>) -> Result<T, StorageError> {
    result.map_err(handle_err)
}

/// An asynchronous store backed by an [`icechunk::session::Session`].
pub struct AsyncIcechunkStore {
    icechunk_session: Arc<RwLock<icechunk::session::Session>>,
}

impl From<Arc<RwLock<icechunk::session::Session>>> for AsyncIcechunkStore {
    fn from(icechunk_session: Arc<RwLock<icechunk::session::Session>>) -> Self {
        Self { icechunk_session }
    }
}

impl AsyncIcechunkStore {
    async fn store(&self) -> icechunk::Store {
        icechunk::Store::from_session(self.icechunk_session.clone()).await
    }

    /// Create a new [`AsyncIcechunkStore`].
    #[must_use]
    pub fn new(icechunk_session: icechunk::session::Session) -> Self {
        Self {
            icechunk_session: Arc::new(RwLock::new(icechunk_session)),
        }
    }

    /// Return the inner [`icechunk::session::Session`].
    #[must_use]
    pub fn session(&self) -> Arc<RwLock<icechunk::session::Session>> {
        self.icechunk_session.clone()
    }

    // TODO: Wait for async closures
    // // /// Run a method on the underlying session.
    // pub async fn with_session<F, T>(&self, f: F) -> icechunk::session::SessionResult<T>
    // where
    //     F: async FnOnce(&icechunk::session::Session) -> icechunk::session::SessionResult<T>,
    // {
    //     let session = self.icechunk_session.read().await;
    //     f(&session).await
    // }

    // /// Run a mutable method on the underlying session.
    // pub async fn with_session_mut<F, T>(&self, f: F) -> icechunk::session::SessionResult<T>
    // where
    //     F: async FnOnce(&icechunk::session::Session) -> icechunk::session::SessionResult<T>,
    // {
    //     let mut session = self.icechunk_session.write().await;
    //     f(&mut session).await
    // }
}

#[async_trait::async_trait]
impl AsyncReadableStorageTraits for AsyncIcechunkStore {
    async fn get(&self, key: &StoreKey) -> Result<MaybeBytes, StorageError> {
        handle_result_notfound(
            self.store()
                .await
                .get(key.as_str(), &icechunk::format::ByteRange::ALL)
                .await,
        )
    }

    async fn get_partial_many<'a>(
        &'a self,
        key: &StoreKey,
        byte_ranges: ByteRangeIterator<'a>,
    ) -> Result<AsyncMaybeBytesIterator<'a>, StorageError> {
        let byte_ranges: Vec<_> = byte_ranges
            .map(|byte_range| {
                let key = key.to_string();
                let byte_range = match byte_range {
                    ByteRange::FromStart(offset, None) => {
                        icechunk::format::ByteRange::from_offset(offset)
                    }
                    ByteRange::FromStart(offset, Some(length)) => {
                        icechunk::format::ByteRange::from_offset_with_length(offset, length)
                    }
                    ByteRange::Suffix(length) => icechunk::format::ByteRange::Last(length),
                };
                (key, byte_range)
            })
            .collect();
        let result =
            handle_result_notfound(self.store().await.get_partial_values(byte_ranges).await)?;
        if let Some(result) = result {
            Ok(Some(
                futures::stream::iter(result.into_iter().map(handle_result)).boxed(),
            ))
        } else {
            Ok(None)
        }
    }

    // NOTE: this does not differentiate between not found and empty
    async fn size_key(&self, key: &StoreKey) -> Result<Option<u64>, StorageError> {
        let key = key.to_string();
        handle_result(self.store().await.getsize(&key).await).map(Some)
    }

    fn supports_get_partial(&self) -> bool {
        true
    }
}

#[async_trait::async_trait]
impl AsyncWritableStorageTraits for AsyncIcechunkStore {
    async fn set(&self, key: &StoreKey, value: Bytes) -> Result<(), StorageError> {
        handle_result(self.store().await.set(key.as_str(), value).await)?;
        Ok(())
    }

    async fn set_partial_many<'a>(
        &'a self,
        _key: &StoreKey,
        _offset_values: OffsetBytesIterator<'a>,
    ) -> Result<(), StorageError> {
        if self
            .store()
            .await
            .supports_partial_writes()
            .map_err(handle_err)?
        {
            // FIXME: Upstream: icechunk::Store does not support partial writes
            Err(StorageError::Unsupported(
                "the store does not support partial writes".to_string(),
            ))
        } else {
            Err(StorageError::Unsupported(
                "the store does not support partial writes".to_string(),
            ))
        }
    }

    async fn erase(&self, key: &StoreKey) -> Result<(), StorageError> {
        if self.store().await.supports_deletes().map_err(handle_err)? {
            handle_result_notfound(self.store().await.delete(key.as_str()).await)?;
            Ok(())
        } else {
            Err(StorageError::Unsupported(
                "the store does not support deletion".to_string(),
            ))
        }
    }

    async fn erase_prefix(&self, prefix: &StorePrefix) -> Result<(), StorageError> {
        if self.store().await.supports_deletes().map_err(handle_err)? {
            let keys = self
                .store()
                .await
                .list_prefix(prefix.as_str())
                .await
                .map_err(handle_err)?
                .try_collect::<Vec<_>>() // TODO: do not collect, use try_for_each
                .await
                .map_err(handle_err)?;
            for key in keys {
                self.store().await.delete(&key).await.map_err(handle_err)?;
            }
            Ok(())
        } else {
            Err(StorageError::Unsupported(
                "the store does not support deletion".to_string(),
            ))
        }
    }

    fn supports_set_partial(&self) -> bool {
        false
    }
}

#[async_trait::async_trait]
impl AsyncListableStorageTraits for AsyncIcechunkStore {
    async fn list(&self) -> Result<StoreKeys, StorageError> {
        let keys = self.store().await.list().await.map_err(handle_err)?;
        keys.map(|key| match key {
            Ok(key) => Ok(StoreKey::new(&key)?),
            Err(err) => Err(StorageError::Other(err.to_string())),
        })
        .try_collect::<Vec<_>>()
        .await
    }

    async fn list_prefix(&self, prefix: &StorePrefix) -> Result<StoreKeys, StorageError> {
        let keys = self
            .store()
            .await
            .list_prefix(prefix.as_str())
            .await
            .map_err(handle_err)?;
        keys.map(|key| match key {
            Ok(key) => Ok(StoreKey::new(&key)?),
            Err(err) => Err(StorageError::Other(err.to_string())),
        })
        .try_collect::<Vec<_>>()
        .await
    }

    async fn list_dir(&self, prefix: &StorePrefix) -> Result<StoreKeysPrefixes, StorageError> {
        let keys_prefixes = self
            .store()
            .await
            .list_dir_items(prefix.as_str())
            .await
            .map_err(handle_err)?;
        let mut keys = vec![];
        let mut prefixes = vec![];
        keys_prefixes
            .map_err(handle_err)
            .map(|item| {
                match item? {
                    icechunk::store::ListDirItem::Key(key) => {
                        keys.push(StoreKey::new(format!("{}{}", prefix.as_str(), &key))?);
                    }
                    icechunk::store::ListDirItem::Prefix(prefix_inner) => {
                        prefixes.push(StorePrefix::new(format!(
                            "{}{}/",
                            prefix.as_str(),
                            &prefix_inner
                        ))?);
                    }
                }
                Ok::<_, StorageError>(())
            })
            .try_for_each(|_| future::ready(Ok(())))
            .await?;

        Ok(StoreKeysPrefixes::new(keys, prefixes))
    }

    async fn size_prefix(&self, prefix: &StorePrefix) -> Result<u64, StorageError> {
        let keys = self.list_prefix(prefix).await?;
        let mut futures: FuturesUnordered<_> = keys
            .into_iter()
            .map(|key| async move {
                let key = key.to_string();
                handle_result(self.store().await.getsize(&key).await)
            })
            .collect();
        let mut sum = 0;
        while let Some(result) = futures.next().await {
            sum += result?;
        }
        Ok(sum)
    }

    async fn size(&self) -> Result<u64, StorageError> {
        self.size_prefix(&StorePrefix::root()).await
    }
}

#[cfg(test)]
mod tests {
    use icechunk::{repository::VersionInfo, Repository, RepositoryConfig};

    use super::*;
    use std::error::Error;

    fn remove_whitespace(s: &str) -> String {
        s.chars().filter(|c| !c.is_whitespace()).collect()
    }

    // NOTE: The icechunk store is not a run-of-the-mill Zarr store that knows nothing about Zarr.
    // It adds additional requirements on keys/data (like looking for known zarr metadata, c prefix, etc.)
    // Thus it does not support the current zarrs async store test suite.
    // The test suite could be changed to only create a structure that is actually zarr specific (standard keys, actually valid group/array json, c/ prefix etc)
    #[tokio::test]
    #[ignore]
    async fn icechunk() -> Result<(), Box<dyn Error>> {
        let storage = icechunk::new_in_memory_storage().await?;
        let config = RepositoryConfig::default();
        let repo = Repository::create(
            Some(config),
            storage,
            Default::default(),
            Default::default(),
            true,
        )
        .await?;
        let store = AsyncIcechunkStore::new(repo.writable_session("main").await?);

        zarrs_storage::store_test::async_store_write(&store).await?;
        zarrs_storage::store_test::async_store_read(&store).await?;
        zarrs_storage::store_test::async_store_list(&store).await?;

        Ok(())
    }

    #[tokio::test]
    async fn icechunk_time_travel() -> Result<(), Box<dyn Error>> {
        let storage = icechunk::new_in_memory_storage().await?;
        let config = RepositoryConfig::default();
        let repo = Repository::create(
            Some(config),
            storage,
            Default::default(),
            Default::default(),
            true,
        )
        .await?;

        let json = r#"{
            "zarr_format": 3,
            "node_type": "group"
        }"#;
        let json: String = remove_whitespace(json);

        let json_updated = r#"{
            "zarr_format": 3,
            "node_type": "group",
            "attributes": {
                "icechunk": "x zarrs"
            }
        }"#;
        let json_updated: String = remove_whitespace(json_updated);

        let root_json = StoreKey::new("zarr.json").unwrap();

        let store = AsyncIcechunkStore::new(repo.writable_session("main").await?);
        assert_eq!(store.get(&root_json).await?, None);
        store.set(&root_json, json.clone().into()).await?;
        assert_eq!(store.get(&root_json).await?, Some(json.clone().into()));
        let snapshot0 = store
            .session()
            .write()
            .await
            .commit("intial commit")
            .execute()
            .await?;

        let store = AsyncIcechunkStore::new(repo.writable_session("main").await?);
        store.set(&root_json, json_updated.clone().into()).await?;
        let _snapshot1 = store
            .session()
            .write()
            .await
            .commit("write attributes")
            .execute()
            .await?;
        assert_eq!(store.get(&root_json).await?, Some(json_updated.into()));

        let session = repo
            .readonly_session(&VersionInfo::SnapshotId(snapshot0))
            .await?;
        let store = AsyncIcechunkStore::new(session);
        assert_eq!(store.get(&root_json).await?, Some(json.clone().into()));

        Ok(())
    }

    #[tokio::test]
    async fn list_dir_and_list_prefix_nested() -> Result<(), Box<dyn Error>> {
        // Create an icechunk repository with a deeply nested zarr hierarchy
        let storage = icechunk::new_in_memory_storage().await?;
        let config = RepositoryConfig::default();
        let repo = Repository::create(
            Some(config),
            storage,
            Default::default(),
            Default::default(),
            true,
        )
        .await?;
        let store = AsyncIcechunkStore::new(repo.writable_session("main").await?);

        let group_json = r#"{"zarr_format":3,"node_type":"group"}"#;
        let array_json = r#"{"zarr_format":3,"node_type":"array","shape":[10, 10],"data_type":"int32","chunk_grid":{"name":"regular","configuration":{"chunk_shape":[5, 5]}},"chunk_key_encoding":{"name":"default","configuration":{"separator":"/"}},"fill_value":0,"codecs":[{"name":"bytes","configuration":{"endian":"little"}}]}"#;

        // Create a deeply nested hierarchy:
        // root/
        //   zarr.json
        //   group0/
        //     zarr.json
        //     group1/
        //       zarr.json
        //       array0/
        //         zarr.json
        //         c/0/0
        //         c/0/1
        //       array1/
        //         zarr.json
        //         c/0/0

        // Create groups
        store
            .set(&StoreKey::new("zarr.json")?, group_json.into())
            .await?;
        store
            .set(&StoreKey::new("group0/zarr.json")?, group_json.into())
            .await?;
        store
            .set(
                &StoreKey::new("group0/group1/zarr.json")?,
                group_json.into(),
            )
            .await?;

        // Create arrays
        store
            .set(
                &StoreKey::new("group0/group1/array0/zarr.json")?,
                array_json.into(),
            )
            .await?;
        store
            .set(
                &StoreKey::new("group0/group1/array0/c/0/0")?,
                vec![0u8; 20].into(),
            )
            .await?;
        store
            .set(
                &StoreKey::new("group0/group1/array0/c/0/1")?,
                vec![1u8; 20].into(),
            )
            .await?;

        store
            .set(
                &StoreKey::new("group0/group1/array1/zarr.json")?,
                array_json.into(),
            )
            .await?;
        store
            .set(
                &StoreKey::new("group0/group1/array1/c/0/0")?,
                vec![2u8; 20].into(),
            )
            .await?;

        // Commit the data
        store
            .session()
            .write()
            .await
            .commit("Create nested hierarchy")
            .execute()
            .await?;

        // Test list_dir at root
        let root_items = store.list_dir(&StorePrefix::root()).await?;
        assert_eq!(root_items.keys().len(), 1); // zarr.json
        assert_eq!(root_items.prefixes().len(), 1); // group0/
        assert!(root_items.keys().contains(&StoreKey::new("zarr.json")?));
        assert!(root_items
            .prefixes()
            .contains(&StorePrefix::new("group0/")?));

        // Test list_dir at group0
        let group0_items = store.list_dir(&StorePrefix::new("group0/")?).await?;
        assert_eq!(group0_items.keys().len(), 1); // zarr.json
        assert_eq!(group0_items.prefixes().len(), 1); // group1/
        assert!(group0_items
            .keys()
            .contains(&StoreKey::new("group0/zarr.json")?));
        assert!(group0_items
            .prefixes()
            .contains(&StorePrefix::new("group0/group1/")?));

        // Test list_dir at group1
        let group1_items = store.list_dir(&StorePrefix::new("group0/group1/")?).await?;
        assert_eq!(group1_items.keys().len(), 1); // zarr.json
        assert_eq!(group1_items.prefixes().len(), 2); // array0/, array1/
        assert!(group1_items
            .keys()
            .contains(&StoreKey::new("group0/group1/zarr.json")?));
        assert!(group1_items
            .prefixes()
            .contains(&StorePrefix::new("group0/group1/array0/")?));
        assert!(group1_items
            .prefixes()
            .contains(&StorePrefix::new("group0/group1/array1/")?));

        // Test list_dir inside array0
        let array0_items = store
            .list_dir(&StorePrefix::new("group0/group1/array0/")?)
            .await?;
        assert_eq!(array0_items.keys().len(), 1); // zarr.json
        assert_eq!(array0_items.prefixes().len(), 1); // c/
        assert!(array0_items
            .keys()
            .contains(&StoreKey::new("group0/group1/array0/zarr.json")?));
        assert!(array0_items
            .prefixes()
            .contains(&StorePrefix::new("group0/group1/array0/c/")?));

        // Test list_dir inside array0/c/ directory
        let array0_c_items = store
            .list_dir(&StorePrefix::new("group0/group1/array0/c/")?)
            .await?;
        assert_eq!(array0_c_items.keys().len(), 0); // no direct keys
        assert_eq!(array0_c_items.prefixes().len(), 1); // 0/
        assert!(array0_c_items
            .prefixes()
            .contains(&StorePrefix::new("group0/group1/array0/c/0/")?));

        // Test list_dir inside array0/c/0/ directory
        let array0_c0_items = store
            .list_dir(&StorePrefix::new("group0/group1/array0/c/0/")?)
            .await?;
        assert_eq!(array0_c0_items.keys().len(), 2); // 0, 1
        assert_eq!(array0_c0_items.prefixes().len(), 0); // no subdirectories
        assert!(array0_c0_items
            .keys()
            .contains(&StoreKey::new("group0/group1/array0/c/0/0")?));
        assert!(array0_c0_items
            .keys()
            .contains(&StoreKey::new("group0/group1/array0/c/0/1")?));

        // Test list_prefix at root (should get all keys recursively)
        let all_keys = store.list_prefix(&StorePrefix::root()).await?;
        assert_eq!(all_keys.len(), 8); // Total of 8 keys in the hierarchy

        // Test list_prefix at group0
        let group0_keys = store.list_prefix(&StorePrefix::new("group0/")?).await?;
        assert_eq!(group0_keys.len(), 7); // All keys under group0/

        // Test list_prefix at group1
        let group1_keys = store
            .list_prefix(&StorePrefix::new("group0/group1/")?)
            .await?;
        assert_eq!(group1_keys.len(), 6); // zarr.json + 2 arrays with their chunks

        // Test list_prefix for array0 (should get all chunks recursively)
        let array0_keys = store
            .list_prefix(&StorePrefix::new("group0/group1/array0/")?)
            .await?;
        assert_eq!(array0_keys.len(), 3); // zarr.json + 2 chunks
        assert!(array0_keys.contains(&StoreKey::new("group0/group1/array0/zarr.json")?));
        assert!(array0_keys.contains(&StoreKey::new("group0/group1/array0/c/0/0")?));
        assert!(array0_keys.contains(&StoreKey::new("group0/group1/array0/c/0/1")?));

        // Test list_prefix for array1 (should get all chunks recursively)
        let array1_keys = store
            .list_prefix(&StorePrefix::new("group0/group1/array1/")?)
            .await?;
        assert_eq!(array1_keys.len(), 2); // zarr.json + 1 chunk
        assert!(array1_keys.contains(&StoreKey::new("group0/group1/array1/zarr.json")?));
        assert!(array1_keys.contains(&StoreKey::new("group0/group1/array1/c/0/0")?));

        Ok(())
    }
}
