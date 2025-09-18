use std::{collections::HashMap, sync::Arc};

use icechunk::{repository::VersionInfo, Repository, RepositoryConfig};
use zarrs::{
    array::{ArrayBuilder, DataType, FillValue},
    array_subset::ArraySubset,
};
use zarrs_icechunk::AsyncIcechunkStore;

#[tokio::test]
async fn icechunk_array() -> Result<(), Box<dyn std::error::Error>> {
    let storage = icechunk::new_in_memory_storage().await?;
    let config = RepositoryConfig::default();
    let repo = Repository::create(Some(config), storage, HashMap::new()).await?;

    let array_path = "/array";
    let mut builder = ArrayBuilder::new(
        vec![4, 4], // array shape
        vec![2, 2], // regular chunk shape
        DataType::UInt8,
        FillValue::from(0u8),
    );
    builder.bytes_to_bytes_codecs(vec![]);
    // builder.storage_transformers(vec![].into());
    builder.array_to_bytes_codec(Arc::new(
        zarrs::array::codec::array_to_bytes::sharding::ShardingCodecBuilder::new(
            vec![1, 1].try_into().unwrap(),
        )
        .bytes_to_bytes_codecs(vec![Arc::new(zarrs::array::codec::GzipCodec::new(5)?)])
        .build(),
    ));

    let session = repo.writable_session("main").await?;
    let store = Arc::new(AsyncIcechunkStore::new(session));
    let array = builder.build(store.clone(), array_path).unwrap();

    array.async_store_metadata().await?;

    assert_eq!(array.data_type(), &DataType::UInt8);
    assert_eq!(array.fill_value().as_ne_bytes(), &[0u8]);
    assert_eq!(array.shape(), &[4, 4]);
    assert_eq!(
        array.chunk_shape(&[0, 0]).unwrap(),
        [2, 2].try_into().unwrap()
    );
    assert_eq!(array.chunk_grid_shape(), &[2, 2]);

    // 1  2 | 0  0
    // 0  0 | 0  0
    // -----|-----
    // 0  0 | 0  0
    // 0  0 | 0  0
    array.async_store_chunk(&[0, 0], &[1, 2, 0, 0]).await?;
    let snapshot0 = store.session().write().await.commit("a", None).await?;

    let session = repo.writable_session("main").await?;
    let store = Arc::new(AsyncIcechunkStore::new(session));
    let array = builder.build(store.clone(), array_path).unwrap();

    // 1  2 | 3  4
    // 5  6 | 7  8
    // -----|-----
    // 9 10 | 0  0
    // 0  0 | 0  0
    array.async_store_chunk(&[0, 1], &[3, 4, 7, 8]).await?;
    array
        .async_store_array_subset(&ArraySubset::new_with_ranges(&[1..3, 0..2]), &[5, 6, 9, 10])
        .await?;
    assert!(array.async_retrieve_chunk(&[0, 0, 0]).await.is_err());
    assert_eq!(
        array.async_retrieve_chunk(&[0, 0]).await?,
        vec![1, 2, 5, 6].into()
    );
    let _snapshot1 = store.session().write().await.commit("b", None).await?;

    let session = repo
        .readonly_session(&VersionInfo::SnapshotId(snapshot0))
        .await?;
    let store = Arc::new(AsyncIcechunkStore::new(session));
    let array = builder.build(store.clone(), array_path).unwrap();

    assert_eq!(
        array.async_retrieve_chunk(&[0, 0]).await?,
        vec![1, 2, 0, 0].into()
    );

    Ok(())
}
