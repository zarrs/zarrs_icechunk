use std::{collections::HashMap, error::Error, path::Path, sync::Arc};

use icechunk::{
    repository::VersionInfo, virtual_chunks::VirtualChunkContainer, Repository, RepositoryConfig,
};
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
    let mut config = RepositoryConfig::default();
    config.set_virtual_chunk_container(VirtualChunkContainer::new(
        "https://github.com".to_string(),
        icechunk::ObjectStoreConfig::Http(HashMap::default()),
    )?);
    let repo = Repository::open(
        Some(config),
        storage,
        HashMap::from([("https://github.com".to_string(), None)]),
    )
    .await?;

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

    // Get the `add_offset` and `scale_factor` (xarray attributes)
    // It would be preferable if virtualizarr could restore FixedScaleOffset so this doesn't need to be aware of xarray
    let get_float_attr = |key: &str, default: f64| {
        array.attributes().get(key).map_or(default, |value| {
            value
                .as_number()
                .map_or(default, |value| value.as_f64().unwrap_or(default))
        })
    };
    let add_offset = get_float_attr("add_offset", 0.0);
    let scale_factor = get_float_attr("scale_factor", 1.0);

    println!(
        "{}",
        array
            .async_retrieve_array_subset_ndarray::<f64>(&array.subset_all())
            .await?
            * scale_factor
            + add_offset
    );

    Ok(())
}
