// Copyright 2023 TiKV Project Authors. Licensed under Apache-2.0.

use prometheus::{register_gauge_vec, register_int_gauge, GaugeVec, IntGauge};

lazy_static::lazy_static! {
    pub static ref GRPC_CLIENT_CALLS_CREATED: IntGauge = register_int_gauge!(
        "tikv_grpc_client_calls_created",
        "Number of client side calls created by this process"
    )
    .unwrap();

    pub static ref GRPC_SERVER_CALLS_CREATED: IntGauge = register_int_gauge!(
        "tikv_grpc_server_calls_created",
        "Number of server side calls created by this process"
    )
    .unwrap();

    pub static ref GRPC_CLIENT_CHANNELS_CREATED: IntGauge = register_int_gauge!(
        "tikv_grpc_client_channels_created",
        "Number of client channels created"
    )
    .unwrap();

    pub static ref GRPC_CLIENT_SUBCHANNELS_CREATED: IntGauge = register_int_gauge!(
        "tikv_grpc_client_subchannels_created",
        "Number of client subchannels created"
    )
    .unwrap();

    pub static ref GRPC_SERVER_CHANNELS_CREATED: IntGauge = register_int_gauge!(
        "tikv_grpc_server_channels_created",
        "Number of server channels created"
    )
    .unwrap();

    pub static ref GRPC_INSECURE_CONNECTIONS_CREATED: IntGauge = register_int_gauge!(
        "tikv_grpc_insecure_connections_created",
        "Number of insecure connections created"
    )
    .unwrap();

    pub static ref GRPC_SYSCALL_WRITE: IntGauge = register_int_gauge!(
        "tikv_grpc_syscall_write",
        "Number of write syscalls (or equivalent - eg sendmsg) made by this process"
    )
    .unwrap();

    pub static ref GRPC_SYSCALL_READ: IntGauge = register_int_gauge!(
        "tikv_grpc_syscall_read",
        "Number of read syscalls (or equivalent - eg recvmsg) made by this process"
    )
    .unwrap();

    pub static ref GRPC_TCP_READ_ALLOC_8K: IntGauge = register_int_gauge!(
        "tikv_grpc_tcp_read_alloc_8k",
        "Number of 8k allocations by the TCP subsystem for reading"
    )
    .unwrap();

    pub static ref GRPC_TCP_READ_ALLOC_64K: IntGauge = register_int_gauge!(
        "tikv_grpc_tcp_read_alloc_64k",
        "Number of 64k allocations by the TCP subsystem for reading"
    )
    .unwrap();

    pub static ref GRPC_HTTP2_SETTINGS_WRITES: IntGauge = register_int_gauge!(
        "tikv_grpc_http2_settings_writes",
        "Number of settings frames sent"
    )
    .unwrap();

    pub static ref GRPC_HTTP_2PINGS_SENT: IntGauge = register_int_gauge!(
        "tikv_grpc_http2_pings_sent",
        "Number of HTTP2 pings sent by process"
    )
    .unwrap();

    pub static ref GRPC_HTTP2_WRITES_BEGUN: IntGauge = register_int_gauge!(
        "tikv_grpc_http2_writes_begun",
        "Number of HTTP2 writes initiated"
    )
    .unwrap();

    pub static ref GRPC_HTTP2_TRANSPORT_STALLS: IntGauge = register_int_gauge!(
        "tikv_grpc_http2_transport_stalls",
        "Number of times sending was completely stalled by the transport flow control window"
    )
    .unwrap();

    pub static ref GRPC_HTTP2_STREAM_STALLS: IntGauge = register_int_gauge!(
        "tikv_grpc_http2_stream_stalls",
        "Number of times sending was completely stalled by the stream flow control window"
    )
    .unwrap();

    pub static ref GRPC_CQ_PLUCK_CREATES: IntGauge = register_int_gauge!(
        "tikv_grpc_cq_pluck_creates",
        "Number of completion queues created for cq_pluck (indicates sync api usage)"
    )
    .unwrap();

    pub static ref GRPC_CQ_NEXT_CREATES: IntGauge = register_int_gauge!(
        "tikv_grpc_cq_next_creates",
        "Number of completion queues created for cq_next (indicates cq async api usage)"
    )
    .unwrap();

    pub static ref GRPC_CQ_CALLBACK_CREATES: IntGauge = register_int_gauge!(
        "tikv_grpc_cq_callback_creates",
        "Number of completion queues created for cq_callback (indicates callback api usage)"
    )
    .unwrap();

    pub static ref GRPC_CALL_INITIAL_SIZE: GaugeVec = register_gauge_vec!(
        "tikv_grpc_call_initial_size",
        "Initial size of the grpc_call arena created at call start",
        &["percentile"]
    )
    .unwrap();

    pub static ref GRPC_TCP_WRITE_SIZE: GaugeVec = register_gauge_vec!(
        "tikv_grpc_tcp_write_size",
        "Number of bytes offered to each syscall_write",
        &["percentile"]
    )
    .unwrap();

    pub static ref GRPC_TCP_WRITE_IOV_SIZE: GaugeVec = register_gauge_vec!(
        "tikv_grpc_tcp_write_iov_size",
        "Number of byte segments offered to each syscall_write",
        &["percentile"]
    )
    .unwrap();

    pub static ref GRPC_TCP_READ_SIZE: GaugeVec = register_gauge_vec!(
        "tikv_grpc_tcp_read_size",
        "Number of bytes received by each syscall_read",
        &["percentile"]
    )
    .unwrap();

    pub static ref GRPC_TCP_READ_OFFER: GaugeVec = register_gauge_vec!(
        "tikv_grpc_tcp_read_offer",
        "Number of bytes offered to each syscall_read",
        &["percentile"]
    )
    .unwrap();

    pub static ref GRPC_TCP_READ_OFFER_IOV_SIZE: GaugeVec = register_gauge_vec!(
        "tikv_grpc_tcp_read_offer_iov_size",
        "Number of byte segments offered to each syscall_read",
        &["percentile"]
    )
    .unwrap();

    pub static ref GRPC_HTTP2_SEND_MESSAGE_SIZE: GaugeVec = register_gauge_vec!(
        "tikv_grpc_http2_send_message_size",
        "Size of messages received by HTTP2 transport",
        &["percentile"]
    )
    .unwrap();

    pub static ref GRPC_HTTP2_METADATA_SIZE: GaugeVec = register_gauge_vec!(
        "tikv_grpc_http2_metadata_size",
        "Number of bytes consumed by metadata, according to HPACK accounting rules",
        &["percentile"]
    )
    .unwrap();
}
