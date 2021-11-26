use br_stream::{
    errors::Result,
    metadata::{store::EtcdStore, MetadataClient},
};
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> Result<()> {
    let meta_store = EtcdStore::connect(&["127.0.0.1:12315"]).await?;
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
    }
    let mut stream = meta_client.events_from(rev).await?;
    while let Some(event) = stream.next().await {
        println!("{:?}", event);
    }
    Ok(())
}
