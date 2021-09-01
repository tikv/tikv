// Copyright 2021 TiKV Project Authors. Licensed under Apache-2.0.

use super::util::Limiter;
use crate::cpu::reporter::endpoint::Endpoint;
use crate::cpu::reporter::record::{CpuTime, Records, Timestamp};

use hex::ToHex;
use hyper::client::HttpConnector;
use hyper::{Body, Client, Method, Request};
use tikv_util::warn;
use tokio::runtime::{Builder, Runtime};

pub struct VictoriaMetricsEndpoint {
    instance_name: String,
    address: String,
    limiter: Limiter,
    runtime: Runtime,
    client: Client<HttpConnector>,
}

impl Endpoint for VictoriaMetricsEndpoint {
    fn init(instance_name: &str, address: &str) -> Box<dyn Endpoint>
    where
        Self: Sized,
    {
        Box::new(Self {
            instance_name: instance_name.to_owned(),
            address: address.to_owned(),
            limiter: Limiter::default(),
            runtime: Builder::new_multi_thread()
                .enable_all()
                .worker_threads(2)
                .thread_name("victoria-metrics-endpoint")
                .build()
                .expect("fail to build tokio runtime"),
            client: Client::builder().build_http(),
        })
    }

    fn update(&mut self, address: &str) {
        if self.address != address {
            self.address = address.to_owned();
        }
    }

    fn report(&mut self, records: Records) {
        let handle = self.limiter.try_acquire();
        if handle.is_none() {
            return;
        }

        let client = self.client.clone();
        let instance_name = self.instance_name.to_owned();
        let address = self.address.clone();
        self.runtime.spawn(async move {
            let _hd = handle;

            let mut body_buf = vec![];
            if !encode_to_metrics_jsonl(&instance_name, records, &mut body_buf) {
                return;
            }

            let req = Request::builder()
                .method(Method::POST)
                .uri(&format!("http://{}/api/v1/import", address))
                .body(Body::from(body_buf));
            if let Err(err) = req {
                warn!("failed to build request"; "error" => ?err);
                return;
            }

            let resp = client.request(req.unwrap()).await;
            if let Err(err) = resp {
                warn!("failed to send request"; "error" => ?err);
            }
        });
    }

    fn name(&self) -> &'static str {
        "victoria-metrics"
    }
}

#[derive(serde::Serialize)]
struct Metric<'a> {
    metric: MetricsLabels<'a>,
    timestamps: &'a [Timestamp],
    values: &'a [CpuTime],
}

#[derive(serde::Serialize)]
struct MetricsLabels<'a> {
    #[serde(rename = "__name__")]
    name: &'static str,
    instance: &'a str,
    job: &'static str,
    #[serde(skip_serializing_if = "str::is_empty")]
    sql_digest: &'a str,
    #[serde(skip_serializing_if = "str::is_empty")]
    plan_digest: &'a str,
}

fn encode_to_metrics_jsonl(instance: &str, records: Records, mut buf: &mut Vec<u8>) -> bool {
    let mut sql_digest_hex = String::new();
    let mut plan_digest_hex = String::new();
    for (tag, (ts_list, cpu_list, _)) in records.records {
        sql_digest_hex.clear();
        plan_digest_hex.clear();
        decode_tag(&tag, &mut sql_digest_hex, &mut plan_digest_hex);
        if sql_digest_hex.is_empty() && plan_digest_hex.is_empty() {
            continue;
        }

        let metric = Metric {
            metric: MetricsLabels {
                name: "cpu_time",
                instance,
                job: "tikv",
                sql_digest: &sql_digest_hex,
                plan_digest: &plan_digest_hex,
            },
            timestamps: &ts_list,
            values: &cpu_list,
        };

        if let Err(err) = serde_json::to_writer(&mut buf, &metric) {
            warn!("failed to encode cpu records to json"; "error" => ?err);
            return false;
        }
        buf.push(b'\n');
    }

    if !records.others.is_empty() {
        let metric = Metric {
            metric: MetricsLabels {
                name: "cpu_time",
                instance,
                job: "tikv",
                sql_digest: "",
                plan_digest: "",
            },
            timestamps: &records.others.keys().cloned().collect::<Vec<_>>(),
            values: &records.others.values().cloned().collect::<Vec<_>>(),
        };
        if let Err(err) = serde_json::to_writer(&mut buf, &metric) {
            warn!("failed to encode cpu records to json"; "error" => ?err);
            return false;
        }
        buf.push(b'\n');
    }

    true
}

fn decode_tag(tag: &[u8], sql_digest_hex: &mut String, plan_digest_hex: &mut String) {
    use protobuf::CodedInputStream;
    use protobuf::Message;

    let mut resource_tag = tipb::ResourceGroupTag::default();
    if let Err(err) = resource_tag.merge_from(&mut CodedInputStream::from_bytes(tag)) {
        warn!("failed to decode resource tag from protobuf"; "error" => ?err);
        return;
    }

    let sql_digest = resource_tag.get_sql_digest();
    let plan_digest = resource_tag.get_plan_digest();

    if let Err(err) = sql_digest.write_hex(sql_digest_hex) {
        warn!("failed to encode bytes to hex"; "error" => ?err);
    }
    if let Err(err) = plan_digest.write_hex(plan_digest_hex) {
        warn!("failed to encode bytes to hex"; "error" => ?err);
    }
}
