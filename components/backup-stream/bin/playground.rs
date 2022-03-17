// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.
use backup_stream::{
    errors::Result,
    metadata::{store::EtcdStore, MetadataClient},
};
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> Result<()> {
    let meta_store =
        EtcdStore::from(etcd_client::Client::connect(&["127.0.0.1:12315"], None).await?);
    let meta_client = MetadataClient::new(meta_store, 42);
    let tasks = meta_client.get_tasks().await?;
    let rev = tasks.revision;
    let tasks = tasks.inner;
    println!("initial tasks: ");
    for t in tasks {
        println!("{:?}: ", t);
        let ranges = meta_client.ranges_of_task(&t.info.name).await?;
        println!("@{{revision={}}}", ranges.revision);
        for range in ranges.inner {
            println!("\t[{:?}, {:?})", range.0, range.1);
        }
        println!("ranges in [3, 8): ");
        let ranges_in_2_and_4 = meta_client
            .range_overlap_of_task(
                &t.info.name,
                (3u64.to_be_bytes().to_vec(), 8u64.to_be_bytes().to_vec()),
            )
            .await?;
        for range in ranges_in_2_and_4.inner {
            println!("\t[{:?}, {:?})", range.0, range.1);
        }
    }
    let mut sub = meta_client.events_from(rev).await?;
    while let Some(event) = sub.stream.next().await {
        println!("{:?}", event);
    }
    Ok(())
}
