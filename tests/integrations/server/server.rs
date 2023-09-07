use std::{sync::Arc, time::Duration};

use grpcio::*;
use grpcio_health::{proto::HealthCheckRequest, HealthClient, ServingStatus};
use service::service_event::ServiceEvent;
use test_pd::Server as MockServer;
use tikv::config::TikvConfig;

#[test]
fn test_restart_grpc_service() {
    fail::cfg("mock_force_uninitial_logger", "return").unwrap();
    let check_heath_api = |max_retry, client: &HealthClient| {
        let req = HealthCheckRequest {
            service: "".to_string(),
            ..Default::default()
        };
        for i in 0..max_retry {
            let r = client.check(&req);
            if r.is_err() {
                assert!(i != max_retry - 1);
                std::thread::sleep(Duration::from_millis(500));
                continue;
            }
            let resp = r.unwrap();
            assert_eq!(ServingStatus::Serving, resp.status);
            break;
        }
    };
    let (service_event_tx, service_event_rx) = tikv_util::mpsc::unbounded();
    let sender = service_event_tx.clone();
    let addr = format!("127.0.0.1:{}", test_util::alloc_port());
    let grpc_addr = addr.clone();
    let tikv_thread = std::thread::spawn(move || {
        let dir = test_util::temp_dir("test_run_tikv_server", true);
        let mut pd_server = MockServer::new(1);
        let eps = pd_server.bind_addrs();
        let mut config = TikvConfig::default();
        config.server.addr = grpc_addr;
        config.log.level = slog::Level::Critical.into();
        config.log.file.filename = "".to_string();
        config.storage.data_dir = dir.path().to_str().unwrap().to_string();
        config.pd.endpoints = vec![format!("{}:{}", eps[0].0, eps[0].1)];
        server::server::run_tikv(config, service_event_tx, service_event_rx);

        pd_server.stop();
    });

    let env = Arc::new(Environment::new(1));
    let channel = ChannelBuilder::new(env).connect(&addr);
    let client: HealthClient = HealthClient::new(channel);
    let req = HealthCheckRequest {
        service: "".to_string(),
        ..Default::default()
    };
    let max_retry = 30;
    check_heath_api(max_retry, &client);
    // PAUSE grpc service and validate.
    {
        let start = std::time::Instant::now();
        sender.send(ServiceEvent::PauseGrpc).unwrap();
        loop {
            if start.elapsed() > Duration::from_secs(5) {
                panic!();
            }
            let resp = client.check(&req);
            if resp.is_err() {
                if let Err(Error::RpcFailure(status)) = resp {
                    assert_eq!(status.code(), RpcStatusCode::UNAVAILABLE);
                }
                break;
            }
        }
    }
    // RESUME grpc service and validate.
    {
        sender.send(ServiceEvent::ResumeGrpc).unwrap();
        check_heath_api(max_retry, &client);
    }
    sender.send(ServiceEvent::Exit).unwrap();
    tikv_thread.join().unwrap();
    fail::remove("mock_force_uninitial_logger");
}
