use br_stream::{errors::Result, metadata::MetadataClient};
use tokio_stream::StreamExt;

#[tokio::main]
async fn main() -> Result<()> {
    let meta_client = MetadataClient::new(&["127.0.0.1:2379"]).await?;
    let tasks = meta_client.get_tasks().await?;
    let rev = tasks.revision;
    let tasks = tasks.inner;
    println!("initial tasks: ");
    for t in tasks {
        println!("{:?}", t);
    }
    let mut stream = meta_client.events_from(rev).await?;
    while let Some(event) = stream.next().await {
        println!("{:?}", event);
    }
    Ok(())
}
