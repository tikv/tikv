use std::{
    fs,
    sync::{mpsc, Arc},
    time::Duration,
};

use grpcio::*;
use grpcio_health::{proto::HealthCheckRequest, HealthClient, ServingStatus};
use kvproto::tikvpb::*;
use test_pd::Server as MockServer;
use tikv::{config::TikvConfig, server::service_event::ServiceEvent};

#[test]
fn test_run_kv() {
    let (service_event_tx, service_event_rx) = mpsc::channel();
    let sender = service_event_tx.clone();
    let (tx, rx) = mpsc::channel();
    std::thread::spawn(move || {
        let eps_count = 1;
        let server = MockServer::new(eps_count);
        let eps = server.bind_addrs();
        let mut config = TikvConfig::default();
        let dir: tempfile::TempDir = test_util::temp_dir("test_run_kv", true);
        config.storage.data_dir = dir.path().to_str().unwrap().to_string();
        config.pd.endpoints = vec![format!("{}:{}", eps[0].0, eps[0].1)];
        server::server::run_tikv(config, service_event_tx, service_event_rx);
        fs::remove_dir_all(dir.path()).unwrap();
        tx.send(true).unwrap();
    });
    std::thread::sleep(Duration::from_secs(30));
    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect("127.0.0.1:20160");
    let client = HealthClient::new(channel);
    let req = HealthCheckRequest {
        service: "".to_string(),
        ..Default::default()
    };
    let resp = client.check(&req).unwrap();
    assert_eq!(ServingStatus::Serving, resp.status);
    sender.send(ServiceEvent::PauseGrpc).unwrap();
    std::thread::sleep(Duration::from_secs(2));
    let resp = client.check(&req);
    assert!(resp.is_err());
    if let Err(Error::RpcFailure(status)) = resp {
        assert_eq!(status.code(), RpcStatusCode::UNAVAILABLE);
    }
    sender.send(ServiceEvent::ResumeGrpc).unwrap();
    std::thread::sleep(Duration::from_secs(2));
    let resp = client.check(&req).unwrap();
    assert_eq!(ServingStatus::Serving, resp.status);
    sender.send(ServiceEvent::Exit).unwrap();
    rx.recv().unwrap();
}
