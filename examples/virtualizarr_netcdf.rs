use std::{collections::HashMap, error::Error, path::Path, sync::Arc};

use icechunk::{repository::VersionInfo, Repository, RepositoryConfig};
use zarrs::{
    array::{Array, ArrayMetadata},
    node::Node,
    storage::AsyncReadableStorageTraits,
};
use zarrs_icechunk::AsyncIcechunkStore;
use zarrs_storage::StoreKey;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let storage =
        icechunk::new_local_filesystem_storage(&Path::new("./examples/data/test.icechunk.zarr"))
            .await?;
    let config = RepositoryConfig::default();
    let repo = Repository::open(Some(config), storage, HashMap::new()).await?;

    let session = repo
        .readonly_session(&VersionInfo::BranchTipRef("main".to_string()))
        .await?;

    let store = Arc::new(AsyncIcechunkStore::new(session));

    let hierarchy = Node::async_open(store.clone(), "/").await?;
    println!("{}", hierarchy.hierarchy_tree());

    // Get the metadata
    let metadata = store.get(&StoreKey::new("data/zarr.json")?).await?.unwrap();
    let metadata: ArrayMetadata = serde_json::from_slice(&metadata)?;
    println!("{}", metadata.to_string_pretty());

    let array = Array::async_open(store.clone(), "/data").await?;
    println!("{}", array.metadata().to_string_pretty());

    println!(
        "{}",
        array
            .async_retrieve_array_subset_ndarray::<f64>(&array.subset_all())
            .await?
    );

    Ok(())
}
