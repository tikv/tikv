use std::{
    fs,
    sync::{mpsc, Arc},
    time::Duration,
};

use grpcio::*;
use grpcio_health::{proto::HealthCheckRequest, HealthClient, ServingStatus};
use service::service_event::ServiceEvent;
use test_pd::Server as MockServer;
use tikv::config::TikvConfig;

#[test]
fn test_restart_grpc() {
    let (service_event_tx, service_event_rx) = mpsc::channel();
    let sender = service_event_tx.clone();
    let tikv_thread = std::thread::spawn(move || {
        let eps_count = 1;
        let mut pd_server = MockServer::new(eps_count);
        let eps = pd_server.bind_addrs();
        let mut config = TikvConfig::default();
        let dir: tempfile::TempDir = test_util::temp_dir("test_run_kv", true);
        config.log.level = slog::Level::Critical.into();
        config.log.file.filename = "".to_string();
        config.storage.data_dir = dir.path().to_str().unwrap().to_string();
        config.pd.endpoints = vec![format!("{}:{}", eps[0].0, eps[0].1)];
        server::server::run_tikv(config, service_event_tx, service_event_rx);

        fs::remove_dir_all(dir.path()).unwrap();
        pd_server.stop();
    });
    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect("127.0.0.1:20160");
    let client: HealthClient = HealthClient::new(channel);
    let req = HealthCheckRequest {
        service: "".to_string(),
        ..Default::default()
    };
    let max_retry = 30;
    let check_heath_api = |max_retry, client: &HealthClient| {
        for i in 0..max_retry {
            let r = client.check(&req);
            if r.is_err() {
                assert!(i != max_retry - 1);
                std::thread::sleep(Duration::from_secs(1));
                continue;
            }
            let resp = r.unwrap();
            assert_eq!(ServingStatus::Serving, resp.status);
            break;
        }
    };
    check_heath_api(max_retry, &client);
    sender.send(ServiceEvent::PauseGrpc).unwrap();
    std::thread::sleep(Duration::from_secs(2));
    let resp = client.check(&req);
    assert!(resp.is_err());
    if let Err(Error::RpcFailure(status)) = resp {
        assert_eq!(status.code(), RpcStatusCode::UNAVAILABLE);
    }
    sender.send(ServiceEvent::ResumeGrpc).unwrap();
    check_heath_api(max_retry, &client);
    sender.send(ServiceEvent::Exit).unwrap();
    let _res = tikv_thread.join();
}
