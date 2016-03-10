// This file is generated. Do not edit
// @generated

// https://github.com/Manishearth/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy)]

#![allow(dead_code)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(unused_imports)]

use protobuf::Message as Message_imported_for_functions;
use protobuf::ProtobufEnum as ProtobufEnum_imported_for_functions;

#[derive(Clone,Default)]
pub struct Leader {
    // message fields
    addr: ::protobuf::SingularField<::std::string::String>,
    pid: ::std::option::Option<i64>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Leader {}

impl Leader {
    pub fn new() -> Leader {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Leader {
        static mut instance: ::protobuf::lazy::Lazy<Leader> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Leader,
        };
        unsafe {
            instance.get(|| {
                Leader {
                    addr: ::protobuf::SingularField::none(),
                    pid: ::std::option::Option::None,
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional string addr = 1;

    pub fn clear_addr(&mut self) {
        self.addr.clear();
    }

    pub fn has_addr(&self) -> bool {
        self.addr.is_some()
    }

    // Param is passed by value, moved
    pub fn set_addr(&mut self, v: ::std::string::String) {
        self.addr = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_addr<'a>(&'a mut self) -> &'a mut ::std::string::String {
        if self.addr.is_none() {
            self.addr.set_default();
        };
        self.addr.as_mut().unwrap()
    }

    // Take field
    pub fn take_addr(&mut self) -> ::std::string::String {
        self.addr.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_addr<'a>(&'a self) -> &'a str {
        match self.addr.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }

    // optional int64 pid = 2;

    pub fn clear_pid(&mut self) {
        self.pid = ::std::option::Option::None;
    }

    pub fn has_pid(&self) -> bool {
        self.pid.is_some()
    }

    // Param is passed by value, moved
    pub fn set_pid(&mut self, v: i64) {
        self.pid = ::std::option::Option::Some(v);
    }

    pub fn get_pid<'a>(&self) -> i64 {
        self.pid.unwrap_or(0)
    }
}

impl ::protobuf::Message for Leader {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    try!(::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.addr));
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    let tmp = try!(is.read_int64());
                    self.pid = ::std::option::Option::Some(tmp);
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in self.addr.iter() {
            my_size += ::protobuf::rt::string_size(1, &value);
        };
        for value in self.pid.iter() {
            my_size += ::protobuf::rt::value_size(2, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.addr.as_ref() {
            try!(os.write_string(1, &v));
        };
        if let Some(v) = self.pid {
            try!(os.write_int64(2, v));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<Leader>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Leader {
    fn new() -> Leader {
        Leader::new()
    }

    fn descriptor_static(_: ::std::option::Option<Leader>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_string_accessor(
                    "addr",
                    Leader::has_addr,
                    Leader::get_addr,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_i64_accessor(
                    "pid",
                    Leader::has_pid,
                    Leader::get_pid,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Leader>(
                    "Leader",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Leader {
    fn clear(&mut self) {
        self.clear_addr();
        self.clear_pid();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for Leader {
    fn eq(&self, other: &Leader) -> bool {
        self.addr == other.addr &&
        self.pid == other.pid &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for Leader {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct TsoRequest {
    // message fields
    number: ::std::option::Option<u32>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for TsoRequest {}

impl TsoRequest {
    pub fn new() -> TsoRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static TsoRequest {
        static mut instance: ::protobuf::lazy::Lazy<TsoRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const TsoRequest,
        };
        unsafe {
            instance.get(|| {
                TsoRequest {
                    number: ::std::option::Option::None,
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional uint32 number = 1;

    pub fn clear_number(&mut self) {
        self.number = ::std::option::Option::None;
    }

    pub fn has_number(&self) -> bool {
        self.number.is_some()
    }

    // Param is passed by value, moved
    pub fn set_number(&mut self, v: u32) {
        self.number = ::std::option::Option::Some(v);
    }

    pub fn get_number<'a>(&self) -> u32 {
        self.number.unwrap_or(0)
    }
}

impl ::protobuf::Message for TsoRequest {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    let tmp = try!(is.read_uint32());
                    self.number = ::std::option::Option::Some(tmp);
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in self.number.iter() {
            my_size += ::protobuf::rt::value_size(1, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.number {
            try!(os.write_uint32(1, v));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<TsoRequest>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for TsoRequest {
    fn new() -> TsoRequest {
        TsoRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<TsoRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_u32_accessor(
                    "number",
                    TsoRequest::has_number,
                    TsoRequest::get_number,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<TsoRequest>(
                    "TsoRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for TsoRequest {
    fn clear(&mut self) {
        self.clear_number();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for TsoRequest {
    fn eq(&self, other: &TsoRequest) -> bool {
        self.number == other.number &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for TsoRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct Timestamp {
    // message fields
    physical: ::std::option::Option<i64>,
    logical: ::std::option::Option<i64>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Timestamp {}

impl Timestamp {
    pub fn new() -> Timestamp {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Timestamp {
        static mut instance: ::protobuf::lazy::Lazy<Timestamp> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Timestamp,
        };
        unsafe {
            instance.get(|| {
                Timestamp {
                    physical: ::std::option::Option::None,
                    logical: ::std::option::Option::None,
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional int64 physical = 1;

    pub fn clear_physical(&mut self) {
        self.physical = ::std::option::Option::None;
    }

    pub fn has_physical(&self) -> bool {
        self.physical.is_some()
    }

    // Param is passed by value, moved
    pub fn set_physical(&mut self, v: i64) {
        self.physical = ::std::option::Option::Some(v);
    }

    pub fn get_physical<'a>(&self) -> i64 {
        self.physical.unwrap_or(0)
    }

    // optional int64 logical = 2;

    pub fn clear_logical(&mut self) {
        self.logical = ::std::option::Option::None;
    }

    pub fn has_logical(&self) -> bool {
        self.logical.is_some()
    }

    // Param is passed by value, moved
    pub fn set_logical(&mut self, v: i64) {
        self.logical = ::std::option::Option::Some(v);
    }

    pub fn get_logical<'a>(&self) -> i64 {
        self.logical.unwrap_or(0)
    }
}

impl ::protobuf::Message for Timestamp {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    let tmp = try!(is.read_int64());
                    self.physical = ::std::option::Option::Some(tmp);
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    let tmp = try!(is.read_int64());
                    self.logical = ::std::option::Option::Some(tmp);
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in self.physical.iter() {
            my_size += ::protobuf::rt::value_size(1, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        for value in self.logical.iter() {
            my_size += ::protobuf::rt::value_size(2, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.physical {
            try!(os.write_int64(1, v));
        };
        if let Some(v) = self.logical {
            try!(os.write_int64(2, v));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<Timestamp>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Timestamp {
    fn new() -> Timestamp {
        Timestamp::new()
    }

    fn descriptor_static(_: ::std::option::Option<Timestamp>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_i64_accessor(
                    "physical",
                    Timestamp::has_physical,
                    Timestamp::get_physical,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_i64_accessor(
                    "logical",
                    Timestamp::has_logical,
                    Timestamp::get_logical,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Timestamp>(
                    "Timestamp",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Timestamp {
    fn clear(&mut self) {
        self.clear_physical();
        self.clear_logical();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for Timestamp {
    fn eq(&self, other: &Timestamp) -> bool {
        self.physical == other.physical &&
        self.logical == other.logical &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for Timestamp {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct TsoResponse {
    // message fields
    timestamps: ::protobuf::RepeatedField<Timestamp>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for TsoResponse {}

impl TsoResponse {
    pub fn new() -> TsoResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static TsoResponse {
        static mut instance: ::protobuf::lazy::Lazy<TsoResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const TsoResponse,
        };
        unsafe {
            instance.get(|| {
                TsoResponse {
                    timestamps: ::protobuf::RepeatedField::new(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // repeated .pdpb.Timestamp timestamps = 1;

    pub fn clear_timestamps(&mut self) {
        self.timestamps.clear();
    }

    // Param is passed by value, moved
    pub fn set_timestamps(&mut self, v: ::protobuf::RepeatedField<Timestamp>) {
        self.timestamps = v;
    }

    // Mutable pointer to the field.
    pub fn mut_timestamps<'a>(&'a mut self) -> &'a mut ::protobuf::RepeatedField<Timestamp> {
        &mut self.timestamps
    }

    // Take field
    pub fn take_timestamps(&mut self) -> ::protobuf::RepeatedField<Timestamp> {
        ::std::mem::replace(&mut self.timestamps, ::protobuf::RepeatedField::new())
    }

    pub fn get_timestamps<'a>(&'a self) -> &'a [Timestamp] {
        &self.timestamps
    }
}

impl ::protobuf::Message for TsoResponse {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    try!(::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.timestamps));
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in self.timestamps.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        for v in self.timestamps.iter() {
            try!(os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<TsoResponse>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for TsoResponse {
    fn new() -> TsoResponse {
        TsoResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<TsoResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_repeated_message_accessor(
                    "timestamps",
                    TsoResponse::get_timestamps,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<TsoResponse>(
                    "TsoResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for TsoResponse {
    fn clear(&mut self) {
        self.clear_timestamps();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for TsoResponse {
    fn eq(&self, other: &TsoResponse) -> bool {
        self.timestamps == other.timestamps &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for TsoResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct BootstrapRequest {
    // message fields
    node: ::protobuf::SingularPtrField<super::metapb::Node>,
    stores: ::protobuf::RepeatedField<super::metapb::Store>,
    region: ::protobuf::SingularPtrField<super::metapb::Region>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for BootstrapRequest {}

impl BootstrapRequest {
    pub fn new() -> BootstrapRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static BootstrapRequest {
        static mut instance: ::protobuf::lazy::Lazy<BootstrapRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const BootstrapRequest,
        };
        unsafe {
            instance.get(|| {
                BootstrapRequest {
                    node: ::protobuf::SingularPtrField::none(),
                    stores: ::protobuf::RepeatedField::new(),
                    region: ::protobuf::SingularPtrField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional .metapb.Node node = 1;

    pub fn clear_node(&mut self) {
        self.node.clear();
    }

    pub fn has_node(&self) -> bool {
        self.node.is_some()
    }

    // Param is passed by value, moved
    pub fn set_node(&mut self, v: super::metapb::Node) {
        self.node = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_node<'a>(&'a mut self) -> &'a mut super::metapb::Node {
        if self.node.is_none() {
            self.node.set_default();
        };
        self.node.as_mut().unwrap()
    }

    // Take field
    pub fn take_node(&mut self) -> super::metapb::Node {
        self.node.take().unwrap_or_else(|| super::metapb::Node::new())
    }

    pub fn get_node<'a>(&'a self) -> &'a super::metapb::Node {
        self.node.as_ref().unwrap_or_else(|| super::metapb::Node::default_instance())
    }

    // repeated .metapb.Store stores = 2;

    pub fn clear_stores(&mut self) {
        self.stores.clear();
    }

    // Param is passed by value, moved
    pub fn set_stores(&mut self, v: ::protobuf::RepeatedField<super::metapb::Store>) {
        self.stores = v;
    }

    // Mutable pointer to the field.
    pub fn mut_stores<'a>(&'a mut self) -> &'a mut ::protobuf::RepeatedField<super::metapb::Store> {
        &mut self.stores
    }

    // Take field
    pub fn take_stores(&mut self) -> ::protobuf::RepeatedField<super::metapb::Store> {
        ::std::mem::replace(&mut self.stores, ::protobuf::RepeatedField::new())
    }

    pub fn get_stores<'a>(&'a self) -> &'a [super::metapb::Store] {
        &self.stores
    }

    // optional .metapb.Region region = 3;

    pub fn clear_region(&mut self) {
        self.region.clear();
    }

    pub fn has_region(&self) -> bool {
        self.region.is_some()
    }

    // Param is passed by value, moved
    pub fn set_region(&mut self, v: super::metapb::Region) {
        self.region = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_region<'a>(&'a mut self) -> &'a mut super::metapb::Region {
        if self.region.is_none() {
            self.region.set_default();
        };
        self.region.as_mut().unwrap()
    }

    // Take field
    pub fn take_region(&mut self) -> super::metapb::Region {
        self.region.take().unwrap_or_else(|| super::metapb::Region::new())
    }

    pub fn get_region<'a>(&'a self) -> &'a super::metapb::Region {
        self.region.as_ref().unwrap_or_else(|| super::metapb::Region::default_instance())
    }
}

impl ::protobuf::Message for BootstrapRequest {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.node));
                },
                2 => {
                    try!(::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.stores));
                },
                3 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.region));
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in self.node.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.stores.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.region.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.node.as_ref() {
            try!(os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        for v in self.stores.iter() {
            try!(os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.region.as_ref() {
            try!(os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<BootstrapRequest>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for BootstrapRequest {
    fn new() -> BootstrapRequest {
        BootstrapRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<BootstrapRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "node",
                    BootstrapRequest::has_node,
                    BootstrapRequest::get_node,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_message_accessor(
                    "stores",
                    BootstrapRequest::get_stores,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "region",
                    BootstrapRequest::has_region,
                    BootstrapRequest::get_region,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<BootstrapRequest>(
                    "BootstrapRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for BootstrapRequest {
    fn clear(&mut self) {
        self.clear_node();
        self.clear_stores();
        self.clear_region();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for BootstrapRequest {
    fn eq(&self, other: &BootstrapRequest) -> bool {
        self.node == other.node &&
        self.stores == other.stores &&
        self.region == other.region &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for BootstrapRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct BootstrapResponse {
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for BootstrapResponse {}

impl BootstrapResponse {
    pub fn new() -> BootstrapResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static BootstrapResponse {
        static mut instance: ::protobuf::lazy::Lazy<BootstrapResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const BootstrapResponse,
        };
        unsafe {
            instance.get(|| {
                BootstrapResponse {
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }
}

impl ::protobuf::Message for BootstrapResponse {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<BootstrapResponse>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for BootstrapResponse {
    fn new() -> BootstrapResponse {
        BootstrapResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<BootstrapResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let fields = ::std::vec::Vec::new();
                ::protobuf::reflect::MessageDescriptor::new::<BootstrapResponse>(
                    "BootstrapResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for BootstrapResponse {
    fn clear(&mut self) {
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for BootstrapResponse {
    fn eq(&self, other: &BootstrapResponse) -> bool {
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for BootstrapResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct IsBootstrappedRequest {
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for IsBootstrappedRequest {}

impl IsBootstrappedRequest {
    pub fn new() -> IsBootstrappedRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static IsBootstrappedRequest {
        static mut instance: ::protobuf::lazy::Lazy<IsBootstrappedRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const IsBootstrappedRequest,
        };
        unsafe {
            instance.get(|| {
                IsBootstrappedRequest {
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }
}

impl ::protobuf::Message for IsBootstrappedRequest {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<IsBootstrappedRequest>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for IsBootstrappedRequest {
    fn new() -> IsBootstrappedRequest {
        IsBootstrappedRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<IsBootstrappedRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let fields = ::std::vec::Vec::new();
                ::protobuf::reflect::MessageDescriptor::new::<IsBootstrappedRequest>(
                    "IsBootstrappedRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for IsBootstrappedRequest {
    fn clear(&mut self) {
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for IsBootstrappedRequest {
    fn eq(&self, other: &IsBootstrappedRequest) -> bool {
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for IsBootstrappedRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct IsBootstrappedResponse {
    // message fields
    bootstrapped: ::std::option::Option<bool>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for IsBootstrappedResponse {}

impl IsBootstrappedResponse {
    pub fn new() -> IsBootstrappedResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static IsBootstrappedResponse {
        static mut instance: ::protobuf::lazy::Lazy<IsBootstrappedResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const IsBootstrappedResponse,
        };
        unsafe {
            instance.get(|| {
                IsBootstrappedResponse {
                    bootstrapped: ::std::option::Option::None,
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional bool bootstrapped = 1;

    pub fn clear_bootstrapped(&mut self) {
        self.bootstrapped = ::std::option::Option::None;
    }

    pub fn has_bootstrapped(&self) -> bool {
        self.bootstrapped.is_some()
    }

    // Param is passed by value, moved
    pub fn set_bootstrapped(&mut self, v: bool) {
        self.bootstrapped = ::std::option::Option::Some(v);
    }

    pub fn get_bootstrapped<'a>(&self) -> bool {
        self.bootstrapped.unwrap_or(false)
    }
}

impl ::protobuf::Message for IsBootstrappedResponse {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    let tmp = try!(is.read_bool());
                    self.bootstrapped = ::std::option::Option::Some(tmp);
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if self.bootstrapped.is_some() {
            my_size += 2;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.bootstrapped {
            try!(os.write_bool(1, v));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<IsBootstrappedResponse>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for IsBootstrappedResponse {
    fn new() -> IsBootstrappedResponse {
        IsBootstrappedResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<IsBootstrappedResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_bool_accessor(
                    "bootstrapped",
                    IsBootstrappedResponse::has_bootstrapped,
                    IsBootstrappedResponse::get_bootstrapped,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<IsBootstrappedResponse>(
                    "IsBootstrappedResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for IsBootstrappedResponse {
    fn clear(&mut self) {
        self.clear_bootstrapped();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for IsBootstrappedResponse {
    fn eq(&self, other: &IsBootstrappedResponse) -> bool {
        self.bootstrapped == other.bootstrapped &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for IsBootstrappedResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct AllocIdRequest {
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for AllocIdRequest {}

impl AllocIdRequest {
    pub fn new() -> AllocIdRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static AllocIdRequest {
        static mut instance: ::protobuf::lazy::Lazy<AllocIdRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const AllocIdRequest,
        };
        unsafe {
            instance.get(|| {
                AllocIdRequest {
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }
}

impl ::protobuf::Message for AllocIdRequest {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<AllocIdRequest>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for AllocIdRequest {
    fn new() -> AllocIdRequest {
        AllocIdRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<AllocIdRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let fields = ::std::vec::Vec::new();
                ::protobuf::reflect::MessageDescriptor::new::<AllocIdRequest>(
                    "AllocIdRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for AllocIdRequest {
    fn clear(&mut self) {
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for AllocIdRequest {
    fn eq(&self, other: &AllocIdRequest) -> bool {
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for AllocIdRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct AllocIdResponse {
    // message fields
    id: ::std::option::Option<u64>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for AllocIdResponse {}

impl AllocIdResponse {
    pub fn new() -> AllocIdResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static AllocIdResponse {
        static mut instance: ::protobuf::lazy::Lazy<AllocIdResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const AllocIdResponse,
        };
        unsafe {
            instance.get(|| {
                AllocIdResponse {
                    id: ::std::option::Option::None,
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional uint64 id = 2;

    pub fn clear_id(&mut self) {
        self.id = ::std::option::Option::None;
    }

    pub fn has_id(&self) -> bool {
        self.id.is_some()
    }

    // Param is passed by value, moved
    pub fn set_id(&mut self, v: u64) {
        self.id = ::std::option::Option::Some(v);
    }

    pub fn get_id<'a>(&self) -> u64 {
        self.id.unwrap_or(0)
    }
}

impl ::protobuf::Message for AllocIdResponse {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    let tmp = try!(is.read_uint64());
                    self.id = ::std::option::Option::Some(tmp);
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in self.id.iter() {
            my_size += ::protobuf::rt::value_size(2, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.id {
            try!(os.write_uint64(2, v));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<AllocIdResponse>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for AllocIdResponse {
    fn new() -> AllocIdResponse {
        AllocIdResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<AllocIdResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_u64_accessor(
                    "id",
                    AllocIdResponse::has_id,
                    AllocIdResponse::get_id,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<AllocIdResponse>(
                    "AllocIdResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for AllocIdResponse {
    fn clear(&mut self) {
        self.clear_id();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for AllocIdResponse {
    fn eq(&self, other: &AllocIdResponse) -> bool {
        self.id == other.id &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for AllocIdResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct GetMetaRequest {
    // message fields
    meta_type: ::std::option::Option<MetaType>,
    node_id: ::std::option::Option<u64>,
    store_id: ::std::option::Option<u64>,
    region_key: ::protobuf::SingularField<::std::string::String>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for GetMetaRequest {}

impl GetMetaRequest {
    pub fn new() -> GetMetaRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static GetMetaRequest {
        static mut instance: ::protobuf::lazy::Lazy<GetMetaRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const GetMetaRequest,
        };
        unsafe {
            instance.get(|| {
                GetMetaRequest {
                    meta_type: ::std::option::Option::None,
                    node_id: ::std::option::Option::None,
                    store_id: ::std::option::Option::None,
                    region_key: ::protobuf::SingularField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional .pdpb.MetaType meta_type = 1;

    pub fn clear_meta_type(&mut self) {
        self.meta_type = ::std::option::Option::None;
    }

    pub fn has_meta_type(&self) -> bool {
        self.meta_type.is_some()
    }

    // Param is passed by value, moved
    pub fn set_meta_type(&mut self, v: MetaType) {
        self.meta_type = ::std::option::Option::Some(v);
    }

    pub fn get_meta_type<'a>(&self) -> MetaType {
        self.meta_type.unwrap_or(MetaType::InvalidMeta)
    }

    // optional uint64 node_id = 2;

    pub fn clear_node_id(&mut self) {
        self.node_id = ::std::option::Option::None;
    }

    pub fn has_node_id(&self) -> bool {
        self.node_id.is_some()
    }

    // Param is passed by value, moved
    pub fn set_node_id(&mut self, v: u64) {
        self.node_id = ::std::option::Option::Some(v);
    }

    pub fn get_node_id<'a>(&self) -> u64 {
        self.node_id.unwrap_or(0)
    }

    // optional uint64 store_id = 3;

    pub fn clear_store_id(&mut self) {
        self.store_id = ::std::option::Option::None;
    }

    pub fn has_store_id(&self) -> bool {
        self.store_id.is_some()
    }

    // Param is passed by value, moved
    pub fn set_store_id(&mut self, v: u64) {
        self.store_id = ::std::option::Option::Some(v);
    }

    pub fn get_store_id<'a>(&self) -> u64 {
        self.store_id.unwrap_or(0)
    }

    // optional string region_key = 4;

    pub fn clear_region_key(&mut self) {
        self.region_key.clear();
    }

    pub fn has_region_key(&self) -> bool {
        self.region_key.is_some()
    }

    // Param is passed by value, moved
    pub fn set_region_key(&mut self, v: ::std::string::String) {
        self.region_key = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_region_key<'a>(&'a mut self) -> &'a mut ::std::string::String {
        if self.region_key.is_none() {
            self.region_key.set_default();
        };
        self.region_key.as_mut().unwrap()
    }

    // Take field
    pub fn take_region_key(&mut self) -> ::std::string::String {
        self.region_key.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_region_key<'a>(&'a self) -> &'a str {
        match self.region_key.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }
}

impl ::protobuf::Message for GetMetaRequest {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    let tmp = try!(is.read_enum());
                    self.meta_type = ::std::option::Option::Some(tmp);
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    let tmp = try!(is.read_uint64());
                    self.node_id = ::std::option::Option::Some(tmp);
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    let tmp = try!(is.read_uint64());
                    self.store_id = ::std::option::Option::Some(tmp);
                },
                4 => {
                    try!(::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.region_key));
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in self.meta_type.iter() {
            my_size += ::protobuf::rt::enum_size(1, *value);
        };
        for value in self.node_id.iter() {
            my_size += ::protobuf::rt::value_size(2, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        for value in self.store_id.iter() {
            my_size += ::protobuf::rt::value_size(3, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        for value in self.region_key.iter() {
            my_size += ::protobuf::rt::string_size(4, &value);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.meta_type {
            try!(os.write_enum(1, v.value()));
        };
        if let Some(v) = self.node_id {
            try!(os.write_uint64(2, v));
        };
        if let Some(v) = self.store_id {
            try!(os.write_uint64(3, v));
        };
        if let Some(v) = self.region_key.as_ref() {
            try!(os.write_string(4, &v));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<GetMetaRequest>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for GetMetaRequest {
    fn new() -> GetMetaRequest {
        GetMetaRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<GetMetaRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_enum_accessor(
                    "meta_type",
                    GetMetaRequest::has_meta_type,
                    GetMetaRequest::get_meta_type,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_u64_accessor(
                    "node_id",
                    GetMetaRequest::has_node_id,
                    GetMetaRequest::get_node_id,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_u64_accessor(
                    "store_id",
                    GetMetaRequest::has_store_id,
                    GetMetaRequest::get_store_id,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_string_accessor(
                    "region_key",
                    GetMetaRequest::has_region_key,
                    GetMetaRequest::get_region_key,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<GetMetaRequest>(
                    "GetMetaRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for GetMetaRequest {
    fn clear(&mut self) {
        self.clear_meta_type();
        self.clear_node_id();
        self.clear_store_id();
        self.clear_region_key();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for GetMetaRequest {
    fn eq(&self, other: &GetMetaRequest) -> bool {
        self.meta_type == other.meta_type &&
        self.node_id == other.node_id &&
        self.store_id == other.store_id &&
        self.region_key == other.region_key &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for GetMetaRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct GetMetaResponse {
    // message fields
    meta_type: ::std::option::Option<MetaType>,
    node: ::protobuf::SingularPtrField<super::metapb::Node>,
    store: ::protobuf::SingularPtrField<super::metapb::Store>,
    region: ::protobuf::SingularPtrField<super::metapb::Region>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for GetMetaResponse {}

impl GetMetaResponse {
    pub fn new() -> GetMetaResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static GetMetaResponse {
        static mut instance: ::protobuf::lazy::Lazy<GetMetaResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const GetMetaResponse,
        };
        unsafe {
            instance.get(|| {
                GetMetaResponse {
                    meta_type: ::std::option::Option::None,
                    node: ::protobuf::SingularPtrField::none(),
                    store: ::protobuf::SingularPtrField::none(),
                    region: ::protobuf::SingularPtrField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional .pdpb.MetaType meta_type = 1;

    pub fn clear_meta_type(&mut self) {
        self.meta_type = ::std::option::Option::None;
    }

    pub fn has_meta_type(&self) -> bool {
        self.meta_type.is_some()
    }

    // Param is passed by value, moved
    pub fn set_meta_type(&mut self, v: MetaType) {
        self.meta_type = ::std::option::Option::Some(v);
    }

    pub fn get_meta_type<'a>(&self) -> MetaType {
        self.meta_type.unwrap_or(MetaType::InvalidMeta)
    }

    // optional .metapb.Node node = 2;

    pub fn clear_node(&mut self) {
        self.node.clear();
    }

    pub fn has_node(&self) -> bool {
        self.node.is_some()
    }

    // Param is passed by value, moved
    pub fn set_node(&mut self, v: super::metapb::Node) {
        self.node = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_node<'a>(&'a mut self) -> &'a mut super::metapb::Node {
        if self.node.is_none() {
            self.node.set_default();
        };
        self.node.as_mut().unwrap()
    }

    // Take field
    pub fn take_node(&mut self) -> super::metapb::Node {
        self.node.take().unwrap_or_else(|| super::metapb::Node::new())
    }

    pub fn get_node<'a>(&'a self) -> &'a super::metapb::Node {
        self.node.as_ref().unwrap_or_else(|| super::metapb::Node::default_instance())
    }

    // optional .metapb.Store store = 3;

    pub fn clear_store(&mut self) {
        self.store.clear();
    }

    pub fn has_store(&self) -> bool {
        self.store.is_some()
    }

    // Param is passed by value, moved
    pub fn set_store(&mut self, v: super::metapb::Store) {
        self.store = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_store<'a>(&'a mut self) -> &'a mut super::metapb::Store {
        if self.store.is_none() {
            self.store.set_default();
        };
        self.store.as_mut().unwrap()
    }

    // Take field
    pub fn take_store(&mut self) -> super::metapb::Store {
        self.store.take().unwrap_or_else(|| super::metapb::Store::new())
    }

    pub fn get_store<'a>(&'a self) -> &'a super::metapb::Store {
        self.store.as_ref().unwrap_or_else(|| super::metapb::Store::default_instance())
    }

    // optional .metapb.Region region = 4;

    pub fn clear_region(&mut self) {
        self.region.clear();
    }

    pub fn has_region(&self) -> bool {
        self.region.is_some()
    }

    // Param is passed by value, moved
    pub fn set_region(&mut self, v: super::metapb::Region) {
        self.region = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_region<'a>(&'a mut self) -> &'a mut super::metapb::Region {
        if self.region.is_none() {
            self.region.set_default();
        };
        self.region.as_mut().unwrap()
    }

    // Take field
    pub fn take_region(&mut self) -> super::metapb::Region {
        self.region.take().unwrap_or_else(|| super::metapb::Region::new())
    }

    pub fn get_region<'a>(&'a self) -> &'a super::metapb::Region {
        self.region.as_ref().unwrap_or_else(|| super::metapb::Region::default_instance())
    }
}

impl ::protobuf::Message for GetMetaResponse {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    let tmp = try!(is.read_enum());
                    self.meta_type = ::std::option::Option::Some(tmp);
                },
                2 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.node));
                },
                3 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.store));
                },
                4 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.region));
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in self.meta_type.iter() {
            my_size += ::protobuf::rt::enum_size(1, *value);
        };
        for value in self.node.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.store.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.region.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.meta_type {
            try!(os.write_enum(1, v.value()));
        };
        if let Some(v) = self.node.as_ref() {
            try!(os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.store.as_ref() {
            try!(os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.region.as_ref() {
            try!(os.write_tag(4, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<GetMetaResponse>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for GetMetaResponse {
    fn new() -> GetMetaResponse {
        GetMetaResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<GetMetaResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_enum_accessor(
                    "meta_type",
                    GetMetaResponse::has_meta_type,
                    GetMetaResponse::get_meta_type,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "node",
                    GetMetaResponse::has_node,
                    GetMetaResponse::get_node,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "store",
                    GetMetaResponse::has_store,
                    GetMetaResponse::get_store,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "region",
                    GetMetaResponse::has_region,
                    GetMetaResponse::get_region,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<GetMetaResponse>(
                    "GetMetaResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for GetMetaResponse {
    fn clear(&mut self) {
        self.clear_meta_type();
        self.clear_node();
        self.clear_store();
        self.clear_region();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for GetMetaResponse {
    fn eq(&self, other: &GetMetaResponse) -> bool {
        self.meta_type == other.meta_type &&
        self.node == other.node &&
        self.store == other.store &&
        self.region == other.region &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for GetMetaResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct PutMetaRequest {
    // message fields
    meta_type: ::std::option::Option<MetaType>,
    node: ::protobuf::SingularPtrField<super::metapb::Node>,
    store: ::protobuf::SingularPtrField<super::metapb::Store>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for PutMetaRequest {}

impl PutMetaRequest {
    pub fn new() -> PutMetaRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static PutMetaRequest {
        static mut instance: ::protobuf::lazy::Lazy<PutMetaRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const PutMetaRequest,
        };
        unsafe {
            instance.get(|| {
                PutMetaRequest {
                    meta_type: ::std::option::Option::None,
                    node: ::protobuf::SingularPtrField::none(),
                    store: ::protobuf::SingularPtrField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional .pdpb.MetaType meta_type = 1;

    pub fn clear_meta_type(&mut self) {
        self.meta_type = ::std::option::Option::None;
    }

    pub fn has_meta_type(&self) -> bool {
        self.meta_type.is_some()
    }

    // Param is passed by value, moved
    pub fn set_meta_type(&mut self, v: MetaType) {
        self.meta_type = ::std::option::Option::Some(v);
    }

    pub fn get_meta_type<'a>(&self) -> MetaType {
        self.meta_type.unwrap_or(MetaType::InvalidMeta)
    }

    // optional .metapb.Node node = 2;

    pub fn clear_node(&mut self) {
        self.node.clear();
    }

    pub fn has_node(&self) -> bool {
        self.node.is_some()
    }

    // Param is passed by value, moved
    pub fn set_node(&mut self, v: super::metapb::Node) {
        self.node = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_node<'a>(&'a mut self) -> &'a mut super::metapb::Node {
        if self.node.is_none() {
            self.node.set_default();
        };
        self.node.as_mut().unwrap()
    }

    // Take field
    pub fn take_node(&mut self) -> super::metapb::Node {
        self.node.take().unwrap_or_else(|| super::metapb::Node::new())
    }

    pub fn get_node<'a>(&'a self) -> &'a super::metapb::Node {
        self.node.as_ref().unwrap_or_else(|| super::metapb::Node::default_instance())
    }

    // optional .metapb.Store store = 3;

    pub fn clear_store(&mut self) {
        self.store.clear();
    }

    pub fn has_store(&self) -> bool {
        self.store.is_some()
    }

    // Param is passed by value, moved
    pub fn set_store(&mut self, v: super::metapb::Store) {
        self.store = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_store<'a>(&'a mut self) -> &'a mut super::metapb::Store {
        if self.store.is_none() {
            self.store.set_default();
        };
        self.store.as_mut().unwrap()
    }

    // Take field
    pub fn take_store(&mut self) -> super::metapb::Store {
        self.store.take().unwrap_or_else(|| super::metapb::Store::new())
    }

    pub fn get_store<'a>(&'a self) -> &'a super::metapb::Store {
        self.store.as_ref().unwrap_or_else(|| super::metapb::Store::default_instance())
    }
}

impl ::protobuf::Message for PutMetaRequest {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    let tmp = try!(is.read_enum());
                    self.meta_type = ::std::option::Option::Some(tmp);
                },
                2 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.node));
                },
                3 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.store));
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in self.meta_type.iter() {
            my_size += ::protobuf::rt::enum_size(1, *value);
        };
        for value in self.node.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.store.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.meta_type {
            try!(os.write_enum(1, v.value()));
        };
        if let Some(v) = self.node.as_ref() {
            try!(os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.store.as_ref() {
            try!(os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<PutMetaRequest>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for PutMetaRequest {
    fn new() -> PutMetaRequest {
        PutMetaRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<PutMetaRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_enum_accessor(
                    "meta_type",
                    PutMetaRequest::has_meta_type,
                    PutMetaRequest::get_meta_type,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "node",
                    PutMetaRequest::has_node,
                    PutMetaRequest::get_node,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "store",
                    PutMetaRequest::has_store,
                    PutMetaRequest::get_store,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<PutMetaRequest>(
                    "PutMetaRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for PutMetaRequest {
    fn clear(&mut self) {
        self.clear_meta_type();
        self.clear_node();
        self.clear_store();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for PutMetaRequest {
    fn eq(&self, other: &PutMetaRequest) -> bool {
        self.meta_type == other.meta_type &&
        self.node == other.node &&
        self.store == other.store &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for PutMetaRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct PutMetaResponse {
    // message fields
    meta_type: ::std::option::Option<MetaType>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for PutMetaResponse {}

impl PutMetaResponse {
    pub fn new() -> PutMetaResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static PutMetaResponse {
        static mut instance: ::protobuf::lazy::Lazy<PutMetaResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const PutMetaResponse,
        };
        unsafe {
            instance.get(|| {
                PutMetaResponse {
                    meta_type: ::std::option::Option::None,
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional .pdpb.MetaType meta_type = 1;

    pub fn clear_meta_type(&mut self) {
        self.meta_type = ::std::option::Option::None;
    }

    pub fn has_meta_type(&self) -> bool {
        self.meta_type.is_some()
    }

    // Param is passed by value, moved
    pub fn set_meta_type(&mut self, v: MetaType) {
        self.meta_type = ::std::option::Option::Some(v);
    }

    pub fn get_meta_type<'a>(&self) -> MetaType {
        self.meta_type.unwrap_or(MetaType::InvalidMeta)
    }
}

impl ::protobuf::Message for PutMetaResponse {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    let tmp = try!(is.read_enum());
                    self.meta_type = ::std::option::Option::Some(tmp);
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in self.meta_type.iter() {
            my_size += ::protobuf::rt::enum_size(1, *value);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.meta_type {
            try!(os.write_enum(1, v.value()));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<PutMetaResponse>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for PutMetaResponse {
    fn new() -> PutMetaResponse {
        PutMetaResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<PutMetaResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_enum_accessor(
                    "meta_type",
                    PutMetaResponse::has_meta_type,
                    PutMetaResponse::get_meta_type,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<PutMetaResponse>(
                    "PutMetaResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for PutMetaResponse {
    fn clear(&mut self) {
        self.clear_meta_type();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for PutMetaResponse {
    fn eq(&self, other: &PutMetaResponse) -> bool {
        self.meta_type == other.meta_type &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for PutMetaResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct DeleteMetaRequest {
    // message fields
    meta_type: ::std::option::Option<MetaType>,
    node_id: ::std::option::Option<u64>,
    store_id: ::std::option::Option<u64>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for DeleteMetaRequest {}

impl DeleteMetaRequest {
    pub fn new() -> DeleteMetaRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static DeleteMetaRequest {
        static mut instance: ::protobuf::lazy::Lazy<DeleteMetaRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const DeleteMetaRequest,
        };
        unsafe {
            instance.get(|| {
                DeleteMetaRequest {
                    meta_type: ::std::option::Option::None,
                    node_id: ::std::option::Option::None,
                    store_id: ::std::option::Option::None,
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional .pdpb.MetaType meta_type = 1;

    pub fn clear_meta_type(&mut self) {
        self.meta_type = ::std::option::Option::None;
    }

    pub fn has_meta_type(&self) -> bool {
        self.meta_type.is_some()
    }

    // Param is passed by value, moved
    pub fn set_meta_type(&mut self, v: MetaType) {
        self.meta_type = ::std::option::Option::Some(v);
    }

    pub fn get_meta_type<'a>(&self) -> MetaType {
        self.meta_type.unwrap_or(MetaType::InvalidMeta)
    }

    // optional uint64 node_id = 2;

    pub fn clear_node_id(&mut self) {
        self.node_id = ::std::option::Option::None;
    }

    pub fn has_node_id(&self) -> bool {
        self.node_id.is_some()
    }

    // Param is passed by value, moved
    pub fn set_node_id(&mut self, v: u64) {
        self.node_id = ::std::option::Option::Some(v);
    }

    pub fn get_node_id<'a>(&self) -> u64 {
        self.node_id.unwrap_or(0)
    }

    // optional uint64 store_id = 3;

    pub fn clear_store_id(&mut self) {
        self.store_id = ::std::option::Option::None;
    }

    pub fn has_store_id(&self) -> bool {
        self.store_id.is_some()
    }

    // Param is passed by value, moved
    pub fn set_store_id(&mut self, v: u64) {
        self.store_id = ::std::option::Option::Some(v);
    }

    pub fn get_store_id<'a>(&self) -> u64 {
        self.store_id.unwrap_or(0)
    }
}

impl ::protobuf::Message for DeleteMetaRequest {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    let tmp = try!(is.read_enum());
                    self.meta_type = ::std::option::Option::Some(tmp);
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    let tmp = try!(is.read_uint64());
                    self.node_id = ::std::option::Option::Some(tmp);
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    let tmp = try!(is.read_uint64());
                    self.store_id = ::std::option::Option::Some(tmp);
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in self.meta_type.iter() {
            my_size += ::protobuf::rt::enum_size(1, *value);
        };
        for value in self.node_id.iter() {
            my_size += ::protobuf::rt::value_size(2, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        for value in self.store_id.iter() {
            my_size += ::protobuf::rt::value_size(3, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.meta_type {
            try!(os.write_enum(1, v.value()));
        };
        if let Some(v) = self.node_id {
            try!(os.write_uint64(2, v));
        };
        if let Some(v) = self.store_id {
            try!(os.write_uint64(3, v));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<DeleteMetaRequest>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for DeleteMetaRequest {
    fn new() -> DeleteMetaRequest {
        DeleteMetaRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<DeleteMetaRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_enum_accessor(
                    "meta_type",
                    DeleteMetaRequest::has_meta_type,
                    DeleteMetaRequest::get_meta_type,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_u64_accessor(
                    "node_id",
                    DeleteMetaRequest::has_node_id,
                    DeleteMetaRequest::get_node_id,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_u64_accessor(
                    "store_id",
                    DeleteMetaRequest::has_store_id,
                    DeleteMetaRequest::get_store_id,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<DeleteMetaRequest>(
                    "DeleteMetaRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for DeleteMetaRequest {
    fn clear(&mut self) {
        self.clear_meta_type();
        self.clear_node_id();
        self.clear_store_id();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for DeleteMetaRequest {
    fn eq(&self, other: &DeleteMetaRequest) -> bool {
        self.meta_type == other.meta_type &&
        self.node_id == other.node_id &&
        self.store_id == other.store_id &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for DeleteMetaRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct DeleteMetaResponse {
    // message fields
    meta_type: ::std::option::Option<MetaType>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for DeleteMetaResponse {}

impl DeleteMetaResponse {
    pub fn new() -> DeleteMetaResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static DeleteMetaResponse {
        static mut instance: ::protobuf::lazy::Lazy<DeleteMetaResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const DeleteMetaResponse,
        };
        unsafe {
            instance.get(|| {
                DeleteMetaResponse {
                    meta_type: ::std::option::Option::None,
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional .pdpb.MetaType meta_type = 1;

    pub fn clear_meta_type(&mut self) {
        self.meta_type = ::std::option::Option::None;
    }

    pub fn has_meta_type(&self) -> bool {
        self.meta_type.is_some()
    }

    // Param is passed by value, moved
    pub fn set_meta_type(&mut self, v: MetaType) {
        self.meta_type = ::std::option::Option::Some(v);
    }

    pub fn get_meta_type<'a>(&self) -> MetaType {
        self.meta_type.unwrap_or(MetaType::InvalidMeta)
    }
}

impl ::protobuf::Message for DeleteMetaResponse {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    let tmp = try!(is.read_enum());
                    self.meta_type = ::std::option::Option::Some(tmp);
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in self.meta_type.iter() {
            my_size += ::protobuf::rt::enum_size(1, *value);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.meta_type {
            try!(os.write_enum(1, v.value()));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<DeleteMetaResponse>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for DeleteMetaResponse {
    fn new() -> DeleteMetaResponse {
        DeleteMetaResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<DeleteMetaResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_enum_accessor(
                    "meta_type",
                    DeleteMetaResponse::has_meta_type,
                    DeleteMetaResponse::get_meta_type,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<DeleteMetaResponse>(
                    "DeleteMetaResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for DeleteMetaResponse {
    fn clear(&mut self) {
        self.clear_meta_type();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for DeleteMetaResponse {
    fn eq(&self, other: &DeleteMetaResponse) -> bool {
        self.meta_type == other.meta_type &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for DeleteMetaResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct AskChangePeerRequest {
    // message fields
    region: ::protobuf::SingularPtrField<super::metapb::Region>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for AskChangePeerRequest {}

impl AskChangePeerRequest {
    pub fn new() -> AskChangePeerRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static AskChangePeerRequest {
        static mut instance: ::protobuf::lazy::Lazy<AskChangePeerRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const AskChangePeerRequest,
        };
        unsafe {
            instance.get(|| {
                AskChangePeerRequest {
                    region: ::protobuf::SingularPtrField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional .metapb.Region region = 1;

    pub fn clear_region(&mut self) {
        self.region.clear();
    }

    pub fn has_region(&self) -> bool {
        self.region.is_some()
    }

    // Param is passed by value, moved
    pub fn set_region(&mut self, v: super::metapb::Region) {
        self.region = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_region<'a>(&'a mut self) -> &'a mut super::metapb::Region {
        if self.region.is_none() {
            self.region.set_default();
        };
        self.region.as_mut().unwrap()
    }

    // Take field
    pub fn take_region(&mut self) -> super::metapb::Region {
        self.region.take().unwrap_or_else(|| super::metapb::Region::new())
    }

    pub fn get_region<'a>(&'a self) -> &'a super::metapb::Region {
        self.region.as_ref().unwrap_or_else(|| super::metapb::Region::default_instance())
    }
}

impl ::protobuf::Message for AskChangePeerRequest {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.region));
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in self.region.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.region.as_ref() {
            try!(os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<AskChangePeerRequest>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for AskChangePeerRequest {
    fn new() -> AskChangePeerRequest {
        AskChangePeerRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<AskChangePeerRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "region",
                    AskChangePeerRequest::has_region,
                    AskChangePeerRequest::get_region,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<AskChangePeerRequest>(
                    "AskChangePeerRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for AskChangePeerRequest {
    fn clear(&mut self) {
        self.clear_region();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for AskChangePeerRequest {
    fn eq(&self, other: &AskChangePeerRequest) -> bool {
        self.region == other.region &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for AskChangePeerRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct AskChangePeerResponse {
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for AskChangePeerResponse {}

impl AskChangePeerResponse {
    pub fn new() -> AskChangePeerResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static AskChangePeerResponse {
        static mut instance: ::protobuf::lazy::Lazy<AskChangePeerResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const AskChangePeerResponse,
        };
        unsafe {
            instance.get(|| {
                AskChangePeerResponse {
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }
}

impl ::protobuf::Message for AskChangePeerResponse {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<AskChangePeerResponse>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for AskChangePeerResponse {
    fn new() -> AskChangePeerResponse {
        AskChangePeerResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<AskChangePeerResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let fields = ::std::vec::Vec::new();
                ::protobuf::reflect::MessageDescriptor::new::<AskChangePeerResponse>(
                    "AskChangePeerResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for AskChangePeerResponse {
    fn clear(&mut self) {
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for AskChangePeerResponse {
    fn eq(&self, other: &AskChangePeerResponse) -> bool {
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for AskChangePeerResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct AskSplitRequest {
    // message fields
    region: ::protobuf::SingularPtrField<super::metapb::Region>,
    split_key: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for AskSplitRequest {}

impl AskSplitRequest {
    pub fn new() -> AskSplitRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static AskSplitRequest {
        static mut instance: ::protobuf::lazy::Lazy<AskSplitRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const AskSplitRequest,
        };
        unsafe {
            instance.get(|| {
                AskSplitRequest {
                    region: ::protobuf::SingularPtrField::none(),
                    split_key: ::protobuf::SingularField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional .metapb.Region region = 1;

    pub fn clear_region(&mut self) {
        self.region.clear();
    }

    pub fn has_region(&self) -> bool {
        self.region.is_some()
    }

    // Param is passed by value, moved
    pub fn set_region(&mut self, v: super::metapb::Region) {
        self.region = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_region<'a>(&'a mut self) -> &'a mut super::metapb::Region {
        if self.region.is_none() {
            self.region.set_default();
        };
        self.region.as_mut().unwrap()
    }

    // Take field
    pub fn take_region(&mut self) -> super::metapb::Region {
        self.region.take().unwrap_or_else(|| super::metapb::Region::new())
    }

    pub fn get_region<'a>(&'a self) -> &'a super::metapb::Region {
        self.region.as_ref().unwrap_or_else(|| super::metapb::Region::default_instance())
    }

    // optional bytes split_key = 2;

    pub fn clear_split_key(&mut self) {
        self.split_key.clear();
    }

    pub fn has_split_key(&self) -> bool {
        self.split_key.is_some()
    }

    // Param is passed by value, moved
    pub fn set_split_key(&mut self, v: ::std::vec::Vec<u8>) {
        self.split_key = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_split_key<'a>(&'a mut self) -> &'a mut ::std::vec::Vec<u8> {
        if self.split_key.is_none() {
            self.split_key.set_default();
        };
        self.split_key.as_mut().unwrap()
    }

    // Take field
    pub fn take_split_key(&mut self) -> ::std::vec::Vec<u8> {
        self.split_key.take().unwrap_or_else(|| ::std::vec::Vec::new())
    }

    pub fn get_split_key<'a>(&'a self) -> &'a [u8] {
        match self.split_key.as_ref() {
            Some(v) => &v,
            None => &[],
        }
    }
}

impl ::protobuf::Message for AskSplitRequest {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.region));
                },
                2 => {
                    try!(::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.split_key));
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in self.region.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.split_key.iter() {
            my_size += ::protobuf::rt::bytes_size(2, &value);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.region.as_ref() {
            try!(os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.split_key.as_ref() {
            try!(os.write_bytes(2, &v));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<AskSplitRequest>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for AskSplitRequest {
    fn new() -> AskSplitRequest {
        AskSplitRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<AskSplitRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "region",
                    AskSplitRequest::has_region,
                    AskSplitRequest::get_region,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_bytes_accessor(
                    "split_key",
                    AskSplitRequest::has_split_key,
                    AskSplitRequest::get_split_key,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<AskSplitRequest>(
                    "AskSplitRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for AskSplitRequest {
    fn clear(&mut self) {
        self.clear_region();
        self.clear_split_key();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for AskSplitRequest {
    fn eq(&self, other: &AskSplitRequest) -> bool {
        self.region == other.region &&
        self.split_key == other.split_key &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for AskSplitRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct AskSplitResponse {
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for AskSplitResponse {}

impl AskSplitResponse {
    pub fn new() -> AskSplitResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static AskSplitResponse {
        static mut instance: ::protobuf::lazy::Lazy<AskSplitResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const AskSplitResponse,
        };
        unsafe {
            instance.get(|| {
                AskSplitResponse {
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }
}

impl ::protobuf::Message for AskSplitResponse {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<AskSplitResponse>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for AskSplitResponse {
    fn new() -> AskSplitResponse {
        AskSplitResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<AskSplitResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let fields = ::std::vec::Vec::new();
                ::protobuf::reflect::MessageDescriptor::new::<AskSplitResponse>(
                    "AskSplitResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for AskSplitResponse {
    fn clear(&mut self) {
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for AskSplitResponse {
    fn eq(&self, other: &AskSplitResponse) -> bool {
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for AskSplitResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct RequestHeader {
    // message fields
    uuid: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    cluster_id: ::std::option::Option<u64>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for RequestHeader {}

impl RequestHeader {
    pub fn new() -> RequestHeader {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static RequestHeader {
        static mut instance: ::protobuf::lazy::Lazy<RequestHeader> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const RequestHeader,
        };
        unsafe {
            instance.get(|| {
                RequestHeader {
                    uuid: ::protobuf::SingularField::none(),
                    cluster_id: ::std::option::Option::None,
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional bytes uuid = 1;

    pub fn clear_uuid(&mut self) {
        self.uuid.clear();
    }

    pub fn has_uuid(&self) -> bool {
        self.uuid.is_some()
    }

    // Param is passed by value, moved
    pub fn set_uuid(&mut self, v: ::std::vec::Vec<u8>) {
        self.uuid = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_uuid<'a>(&'a mut self) -> &'a mut ::std::vec::Vec<u8> {
        if self.uuid.is_none() {
            self.uuid.set_default();
        };
        self.uuid.as_mut().unwrap()
    }

    // Take field
    pub fn take_uuid(&mut self) -> ::std::vec::Vec<u8> {
        self.uuid.take().unwrap_or_else(|| ::std::vec::Vec::new())
    }

    pub fn get_uuid<'a>(&'a self) -> &'a [u8] {
        match self.uuid.as_ref() {
            Some(v) => &v,
            None => &[],
        }
    }

    // optional uint64 cluster_id = 2;

    pub fn clear_cluster_id(&mut self) {
        self.cluster_id = ::std::option::Option::None;
    }

    pub fn has_cluster_id(&self) -> bool {
        self.cluster_id.is_some()
    }

    // Param is passed by value, moved
    pub fn set_cluster_id(&mut self, v: u64) {
        self.cluster_id = ::std::option::Option::Some(v);
    }

    pub fn get_cluster_id<'a>(&self) -> u64 {
        self.cluster_id.unwrap_or(0)
    }
}

impl ::protobuf::Message for RequestHeader {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    try!(::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.uuid));
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    let tmp = try!(is.read_uint64());
                    self.cluster_id = ::std::option::Option::Some(tmp);
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in self.uuid.iter() {
            my_size += ::protobuf::rt::bytes_size(1, &value);
        };
        for value in self.cluster_id.iter() {
            my_size += ::protobuf::rt::value_size(2, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.uuid.as_ref() {
            try!(os.write_bytes(1, &v));
        };
        if let Some(v) = self.cluster_id {
            try!(os.write_uint64(2, v));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<RequestHeader>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for RequestHeader {
    fn new() -> RequestHeader {
        RequestHeader::new()
    }

    fn descriptor_static(_: ::std::option::Option<RequestHeader>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_bytes_accessor(
                    "uuid",
                    RequestHeader::has_uuid,
                    RequestHeader::get_uuid,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_u64_accessor(
                    "cluster_id",
                    RequestHeader::has_cluster_id,
                    RequestHeader::get_cluster_id,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<RequestHeader>(
                    "RequestHeader",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for RequestHeader {
    fn clear(&mut self) {
        self.clear_uuid();
        self.clear_cluster_id();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for RequestHeader {
    fn eq(&self, other: &RequestHeader) -> bool {
        self.uuid == other.uuid &&
        self.cluster_id == other.cluster_id &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for RequestHeader {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct ResponseHeader {
    // message fields
    uuid: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    cluster_id: ::std::option::Option<u64>,
    error: ::protobuf::SingularPtrField<Error>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for ResponseHeader {}

impl ResponseHeader {
    pub fn new() -> ResponseHeader {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static ResponseHeader {
        static mut instance: ::protobuf::lazy::Lazy<ResponseHeader> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ResponseHeader,
        };
        unsafe {
            instance.get(|| {
                ResponseHeader {
                    uuid: ::protobuf::SingularField::none(),
                    cluster_id: ::std::option::Option::None,
                    error: ::protobuf::SingularPtrField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional bytes uuid = 1;

    pub fn clear_uuid(&mut self) {
        self.uuid.clear();
    }

    pub fn has_uuid(&self) -> bool {
        self.uuid.is_some()
    }

    // Param is passed by value, moved
    pub fn set_uuid(&mut self, v: ::std::vec::Vec<u8>) {
        self.uuid = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_uuid<'a>(&'a mut self) -> &'a mut ::std::vec::Vec<u8> {
        if self.uuid.is_none() {
            self.uuid.set_default();
        };
        self.uuid.as_mut().unwrap()
    }

    // Take field
    pub fn take_uuid(&mut self) -> ::std::vec::Vec<u8> {
        self.uuid.take().unwrap_or_else(|| ::std::vec::Vec::new())
    }

    pub fn get_uuid<'a>(&'a self) -> &'a [u8] {
        match self.uuid.as_ref() {
            Some(v) => &v,
            None => &[],
        }
    }

    // optional uint64 cluster_id = 2;

    pub fn clear_cluster_id(&mut self) {
        self.cluster_id = ::std::option::Option::None;
    }

    pub fn has_cluster_id(&self) -> bool {
        self.cluster_id.is_some()
    }

    // Param is passed by value, moved
    pub fn set_cluster_id(&mut self, v: u64) {
        self.cluster_id = ::std::option::Option::Some(v);
    }

    pub fn get_cluster_id<'a>(&self) -> u64 {
        self.cluster_id.unwrap_or(0)
    }

    // optional .pdpb.Error error = 3;

    pub fn clear_error(&mut self) {
        self.error.clear();
    }

    pub fn has_error(&self) -> bool {
        self.error.is_some()
    }

    // Param is passed by value, moved
    pub fn set_error(&mut self, v: Error) {
        self.error = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_error<'a>(&'a mut self) -> &'a mut Error {
        if self.error.is_none() {
            self.error.set_default();
        };
        self.error.as_mut().unwrap()
    }

    // Take field
    pub fn take_error(&mut self) -> Error {
        self.error.take().unwrap_or_else(|| Error::new())
    }

    pub fn get_error<'a>(&'a self) -> &'a Error {
        self.error.as_ref().unwrap_or_else(|| Error::default_instance())
    }
}

impl ::protobuf::Message for ResponseHeader {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    try!(::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.uuid));
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    let tmp = try!(is.read_uint64());
                    self.cluster_id = ::std::option::Option::Some(tmp);
                },
                3 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.error));
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in self.uuid.iter() {
            my_size += ::protobuf::rt::bytes_size(1, &value);
        };
        for value in self.cluster_id.iter() {
            my_size += ::protobuf::rt::value_size(2, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        for value in self.error.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.uuid.as_ref() {
            try!(os.write_bytes(1, &v));
        };
        if let Some(v) = self.cluster_id {
            try!(os.write_uint64(2, v));
        };
        if let Some(v) = self.error.as_ref() {
            try!(os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<ResponseHeader>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for ResponseHeader {
    fn new() -> ResponseHeader {
        ResponseHeader::new()
    }

    fn descriptor_static(_: ::std::option::Option<ResponseHeader>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_bytes_accessor(
                    "uuid",
                    ResponseHeader::has_uuid,
                    ResponseHeader::get_uuid,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_u64_accessor(
                    "cluster_id",
                    ResponseHeader::has_cluster_id,
                    ResponseHeader::get_cluster_id,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "error",
                    ResponseHeader::has_error,
                    ResponseHeader::get_error,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<ResponseHeader>(
                    "ResponseHeader",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for ResponseHeader {
    fn clear(&mut self) {
        self.clear_uuid();
        self.clear_cluster_id();
        self.clear_error();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for ResponseHeader {
    fn eq(&self, other: &ResponseHeader) -> bool {
        self.uuid == other.uuid &&
        self.cluster_id == other.cluster_id &&
        self.error == other.error &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for ResponseHeader {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct Request {
    // message fields
    header: ::protobuf::SingularPtrField<RequestHeader>,
    cmd_type: ::std::option::Option<CommandType>,
    tso: ::protobuf::SingularPtrField<TsoRequest>,
    bootstrap: ::protobuf::SingularPtrField<BootstrapRequest>,
    is_bootstrapped: ::protobuf::SingularPtrField<IsBootstrappedRequest>,
    alloc_id: ::protobuf::SingularPtrField<AllocIdRequest>,
    get_meta: ::protobuf::SingularPtrField<GetMetaRequest>,
    put_meta: ::protobuf::SingularPtrField<PutMetaRequest>,
    delete_meta: ::protobuf::SingularPtrField<DeleteMetaRequest>,
    ask_change_peer: ::protobuf::SingularPtrField<AskChangePeerRequest>,
    ask_split: ::protobuf::SingularPtrField<AskSplitRequest>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Request {}

impl Request {
    pub fn new() -> Request {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Request {
        static mut instance: ::protobuf::lazy::Lazy<Request> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Request,
        };
        unsafe {
            instance.get(|| {
                Request {
                    header: ::protobuf::SingularPtrField::none(),
                    cmd_type: ::std::option::Option::None,
                    tso: ::protobuf::SingularPtrField::none(),
                    bootstrap: ::protobuf::SingularPtrField::none(),
                    is_bootstrapped: ::protobuf::SingularPtrField::none(),
                    alloc_id: ::protobuf::SingularPtrField::none(),
                    get_meta: ::protobuf::SingularPtrField::none(),
                    put_meta: ::protobuf::SingularPtrField::none(),
                    delete_meta: ::protobuf::SingularPtrField::none(),
                    ask_change_peer: ::protobuf::SingularPtrField::none(),
                    ask_split: ::protobuf::SingularPtrField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional .pdpb.RequestHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: RequestHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header<'a>(&'a mut self) -> &'a mut RequestHeader {
        if self.header.is_none() {
            self.header.set_default();
        };
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> RequestHeader {
        self.header.take().unwrap_or_else(|| RequestHeader::new())
    }

    pub fn get_header<'a>(&'a self) -> &'a RequestHeader {
        self.header.as_ref().unwrap_or_else(|| RequestHeader::default_instance())
    }

    // optional .pdpb.CommandType cmd_type = 2;

    pub fn clear_cmd_type(&mut self) {
        self.cmd_type = ::std::option::Option::None;
    }

    pub fn has_cmd_type(&self) -> bool {
        self.cmd_type.is_some()
    }

    // Param is passed by value, moved
    pub fn set_cmd_type(&mut self, v: CommandType) {
        self.cmd_type = ::std::option::Option::Some(v);
    }

    pub fn get_cmd_type<'a>(&self) -> CommandType {
        self.cmd_type.unwrap_or(CommandType::Invalid)
    }

    // optional .pdpb.TsoRequest tso = 3;

    pub fn clear_tso(&mut self) {
        self.tso.clear();
    }

    pub fn has_tso(&self) -> bool {
        self.tso.is_some()
    }

    // Param is passed by value, moved
    pub fn set_tso(&mut self, v: TsoRequest) {
        self.tso = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_tso<'a>(&'a mut self) -> &'a mut TsoRequest {
        if self.tso.is_none() {
            self.tso.set_default();
        };
        self.tso.as_mut().unwrap()
    }

    // Take field
    pub fn take_tso(&mut self) -> TsoRequest {
        self.tso.take().unwrap_or_else(|| TsoRequest::new())
    }

    pub fn get_tso<'a>(&'a self) -> &'a TsoRequest {
        self.tso.as_ref().unwrap_or_else(|| TsoRequest::default_instance())
    }

    // optional .pdpb.BootstrapRequest bootstrap = 4;

    pub fn clear_bootstrap(&mut self) {
        self.bootstrap.clear();
    }

    pub fn has_bootstrap(&self) -> bool {
        self.bootstrap.is_some()
    }

    // Param is passed by value, moved
    pub fn set_bootstrap(&mut self, v: BootstrapRequest) {
        self.bootstrap = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_bootstrap<'a>(&'a mut self) -> &'a mut BootstrapRequest {
        if self.bootstrap.is_none() {
            self.bootstrap.set_default();
        };
        self.bootstrap.as_mut().unwrap()
    }

    // Take field
    pub fn take_bootstrap(&mut self) -> BootstrapRequest {
        self.bootstrap.take().unwrap_or_else(|| BootstrapRequest::new())
    }

    pub fn get_bootstrap<'a>(&'a self) -> &'a BootstrapRequest {
        self.bootstrap.as_ref().unwrap_or_else(|| BootstrapRequest::default_instance())
    }

    // optional .pdpb.IsBootstrappedRequest is_bootstrapped = 5;

    pub fn clear_is_bootstrapped(&mut self) {
        self.is_bootstrapped.clear();
    }

    pub fn has_is_bootstrapped(&self) -> bool {
        self.is_bootstrapped.is_some()
    }

    // Param is passed by value, moved
    pub fn set_is_bootstrapped(&mut self, v: IsBootstrappedRequest) {
        self.is_bootstrapped = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_is_bootstrapped<'a>(&'a mut self) -> &'a mut IsBootstrappedRequest {
        if self.is_bootstrapped.is_none() {
            self.is_bootstrapped.set_default();
        };
        self.is_bootstrapped.as_mut().unwrap()
    }

    // Take field
    pub fn take_is_bootstrapped(&mut self) -> IsBootstrappedRequest {
        self.is_bootstrapped.take().unwrap_or_else(|| IsBootstrappedRequest::new())
    }

    pub fn get_is_bootstrapped<'a>(&'a self) -> &'a IsBootstrappedRequest {
        self.is_bootstrapped.as_ref().unwrap_or_else(|| IsBootstrappedRequest::default_instance())
    }

    // optional .pdpb.AllocIdRequest alloc_id = 6;

    pub fn clear_alloc_id(&mut self) {
        self.alloc_id.clear();
    }

    pub fn has_alloc_id(&self) -> bool {
        self.alloc_id.is_some()
    }

    // Param is passed by value, moved
    pub fn set_alloc_id(&mut self, v: AllocIdRequest) {
        self.alloc_id = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_alloc_id<'a>(&'a mut self) -> &'a mut AllocIdRequest {
        if self.alloc_id.is_none() {
            self.alloc_id.set_default();
        };
        self.alloc_id.as_mut().unwrap()
    }

    // Take field
    pub fn take_alloc_id(&mut self) -> AllocIdRequest {
        self.alloc_id.take().unwrap_or_else(|| AllocIdRequest::new())
    }

    pub fn get_alloc_id<'a>(&'a self) -> &'a AllocIdRequest {
        self.alloc_id.as_ref().unwrap_or_else(|| AllocIdRequest::default_instance())
    }

    // optional .pdpb.GetMetaRequest get_meta = 7;

    pub fn clear_get_meta(&mut self) {
        self.get_meta.clear();
    }

    pub fn has_get_meta(&self) -> bool {
        self.get_meta.is_some()
    }

    // Param is passed by value, moved
    pub fn set_get_meta(&mut self, v: GetMetaRequest) {
        self.get_meta = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_get_meta<'a>(&'a mut self) -> &'a mut GetMetaRequest {
        if self.get_meta.is_none() {
            self.get_meta.set_default();
        };
        self.get_meta.as_mut().unwrap()
    }

    // Take field
    pub fn take_get_meta(&mut self) -> GetMetaRequest {
        self.get_meta.take().unwrap_or_else(|| GetMetaRequest::new())
    }

    pub fn get_get_meta<'a>(&'a self) -> &'a GetMetaRequest {
        self.get_meta.as_ref().unwrap_or_else(|| GetMetaRequest::default_instance())
    }

    // optional .pdpb.PutMetaRequest put_meta = 8;

    pub fn clear_put_meta(&mut self) {
        self.put_meta.clear();
    }

    pub fn has_put_meta(&self) -> bool {
        self.put_meta.is_some()
    }

    // Param is passed by value, moved
    pub fn set_put_meta(&mut self, v: PutMetaRequest) {
        self.put_meta = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_put_meta<'a>(&'a mut self) -> &'a mut PutMetaRequest {
        if self.put_meta.is_none() {
            self.put_meta.set_default();
        };
        self.put_meta.as_mut().unwrap()
    }

    // Take field
    pub fn take_put_meta(&mut self) -> PutMetaRequest {
        self.put_meta.take().unwrap_or_else(|| PutMetaRequest::new())
    }

    pub fn get_put_meta<'a>(&'a self) -> &'a PutMetaRequest {
        self.put_meta.as_ref().unwrap_or_else(|| PutMetaRequest::default_instance())
    }

    // optional .pdpb.DeleteMetaRequest delete_meta = 9;

    pub fn clear_delete_meta(&mut self) {
        self.delete_meta.clear();
    }

    pub fn has_delete_meta(&self) -> bool {
        self.delete_meta.is_some()
    }

    // Param is passed by value, moved
    pub fn set_delete_meta(&mut self, v: DeleteMetaRequest) {
        self.delete_meta = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_delete_meta<'a>(&'a mut self) -> &'a mut DeleteMetaRequest {
        if self.delete_meta.is_none() {
            self.delete_meta.set_default();
        };
        self.delete_meta.as_mut().unwrap()
    }

    // Take field
    pub fn take_delete_meta(&mut self) -> DeleteMetaRequest {
        self.delete_meta.take().unwrap_or_else(|| DeleteMetaRequest::new())
    }

    pub fn get_delete_meta<'a>(&'a self) -> &'a DeleteMetaRequest {
        self.delete_meta.as_ref().unwrap_or_else(|| DeleteMetaRequest::default_instance())
    }

    // optional .pdpb.AskChangePeerRequest ask_change_peer = 10;

    pub fn clear_ask_change_peer(&mut self) {
        self.ask_change_peer.clear();
    }

    pub fn has_ask_change_peer(&self) -> bool {
        self.ask_change_peer.is_some()
    }

    // Param is passed by value, moved
    pub fn set_ask_change_peer(&mut self, v: AskChangePeerRequest) {
        self.ask_change_peer = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_ask_change_peer<'a>(&'a mut self) -> &'a mut AskChangePeerRequest {
        if self.ask_change_peer.is_none() {
            self.ask_change_peer.set_default();
        };
        self.ask_change_peer.as_mut().unwrap()
    }

    // Take field
    pub fn take_ask_change_peer(&mut self) -> AskChangePeerRequest {
        self.ask_change_peer.take().unwrap_or_else(|| AskChangePeerRequest::new())
    }

    pub fn get_ask_change_peer<'a>(&'a self) -> &'a AskChangePeerRequest {
        self.ask_change_peer.as_ref().unwrap_or_else(|| AskChangePeerRequest::default_instance())
    }

    // optional .pdpb.AskSplitRequest ask_split = 11;

    pub fn clear_ask_split(&mut self) {
        self.ask_split.clear();
    }

    pub fn has_ask_split(&self) -> bool {
        self.ask_split.is_some()
    }

    // Param is passed by value, moved
    pub fn set_ask_split(&mut self, v: AskSplitRequest) {
        self.ask_split = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_ask_split<'a>(&'a mut self) -> &'a mut AskSplitRequest {
        if self.ask_split.is_none() {
            self.ask_split.set_default();
        };
        self.ask_split.as_mut().unwrap()
    }

    // Take field
    pub fn take_ask_split(&mut self) -> AskSplitRequest {
        self.ask_split.take().unwrap_or_else(|| AskSplitRequest::new())
    }

    pub fn get_ask_split<'a>(&'a self) -> &'a AskSplitRequest {
        self.ask_split.as_ref().unwrap_or_else(|| AskSplitRequest::default_instance())
    }
}

impl ::protobuf::Message for Request {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.header));
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    let tmp = try!(is.read_enum());
                    self.cmd_type = ::std::option::Option::Some(tmp);
                },
                3 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.tso));
                },
                4 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.bootstrap));
                },
                5 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.is_bootstrapped));
                },
                6 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.alloc_id));
                },
                7 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.get_meta));
                },
                8 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.put_meta));
                },
                9 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.delete_meta));
                },
                10 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.ask_change_peer));
                },
                11 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.ask_split));
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in self.header.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.cmd_type.iter() {
            my_size += ::protobuf::rt::enum_size(2, *value);
        };
        for value in self.tso.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.bootstrap.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.is_bootstrapped.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.alloc_id.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.get_meta.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.put_meta.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.delete_meta.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.ask_change_peer.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.ask_split.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.header.as_ref() {
            try!(os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.cmd_type {
            try!(os.write_enum(2, v.value()));
        };
        if let Some(v) = self.tso.as_ref() {
            try!(os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.bootstrap.as_ref() {
            try!(os.write_tag(4, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.is_bootstrapped.as_ref() {
            try!(os.write_tag(5, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.alloc_id.as_ref() {
            try!(os.write_tag(6, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.get_meta.as_ref() {
            try!(os.write_tag(7, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.put_meta.as_ref() {
            try!(os.write_tag(8, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.delete_meta.as_ref() {
            try!(os.write_tag(9, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.ask_change_peer.as_ref() {
            try!(os.write_tag(10, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.ask_split.as_ref() {
            try!(os.write_tag(11, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<Request>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Request {
    fn new() -> Request {
        Request::new()
    }

    fn descriptor_static(_: ::std::option::Option<Request>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "header",
                    Request::has_header,
                    Request::get_header,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_enum_accessor(
                    "cmd_type",
                    Request::has_cmd_type,
                    Request::get_cmd_type,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "tso",
                    Request::has_tso,
                    Request::get_tso,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "bootstrap",
                    Request::has_bootstrap,
                    Request::get_bootstrap,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "is_bootstrapped",
                    Request::has_is_bootstrapped,
                    Request::get_is_bootstrapped,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "alloc_id",
                    Request::has_alloc_id,
                    Request::get_alloc_id,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "get_meta",
                    Request::has_get_meta,
                    Request::get_get_meta,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "put_meta",
                    Request::has_put_meta,
                    Request::get_put_meta,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "delete_meta",
                    Request::has_delete_meta,
                    Request::get_delete_meta,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "ask_change_peer",
                    Request::has_ask_change_peer,
                    Request::get_ask_change_peer,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "ask_split",
                    Request::has_ask_split,
                    Request::get_ask_split,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Request>(
                    "Request",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Request {
    fn clear(&mut self) {
        self.clear_header();
        self.clear_cmd_type();
        self.clear_tso();
        self.clear_bootstrap();
        self.clear_is_bootstrapped();
        self.clear_alloc_id();
        self.clear_get_meta();
        self.clear_put_meta();
        self.clear_delete_meta();
        self.clear_ask_change_peer();
        self.clear_ask_split();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for Request {
    fn eq(&self, other: &Request) -> bool {
        self.header == other.header &&
        self.cmd_type == other.cmd_type &&
        self.tso == other.tso &&
        self.bootstrap == other.bootstrap &&
        self.is_bootstrapped == other.is_bootstrapped &&
        self.alloc_id == other.alloc_id &&
        self.get_meta == other.get_meta &&
        self.put_meta == other.put_meta &&
        self.delete_meta == other.delete_meta &&
        self.ask_change_peer == other.ask_change_peer &&
        self.ask_split == other.ask_split &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for Request {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct Response {
    // message fields
    header: ::protobuf::SingularPtrField<ResponseHeader>,
    cmd_type: ::std::option::Option<CommandType>,
    tso: ::protobuf::SingularPtrField<TsoResponse>,
    bootstrap: ::protobuf::SingularPtrField<BootstrapResponse>,
    is_bootstrapped: ::protobuf::SingularPtrField<IsBootstrappedResponse>,
    alloc_id: ::protobuf::SingularPtrField<AllocIdResponse>,
    get_meta: ::protobuf::SingularPtrField<GetMetaResponse>,
    put_meta: ::protobuf::SingularPtrField<PutMetaResponse>,
    delete_meta: ::protobuf::SingularPtrField<DeleteMetaResponse>,
    ask_change_peer: ::protobuf::SingularPtrField<AskChangePeerResponse>,
    ask_split: ::protobuf::SingularPtrField<AskSplitResponse>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Response {}

impl Response {
    pub fn new() -> Response {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Response {
        static mut instance: ::protobuf::lazy::Lazy<Response> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Response,
        };
        unsafe {
            instance.get(|| {
                Response {
                    header: ::protobuf::SingularPtrField::none(),
                    cmd_type: ::std::option::Option::None,
                    tso: ::protobuf::SingularPtrField::none(),
                    bootstrap: ::protobuf::SingularPtrField::none(),
                    is_bootstrapped: ::protobuf::SingularPtrField::none(),
                    alloc_id: ::protobuf::SingularPtrField::none(),
                    get_meta: ::protobuf::SingularPtrField::none(),
                    put_meta: ::protobuf::SingularPtrField::none(),
                    delete_meta: ::protobuf::SingularPtrField::none(),
                    ask_change_peer: ::protobuf::SingularPtrField::none(),
                    ask_split: ::protobuf::SingularPtrField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional .pdpb.ResponseHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: ResponseHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header<'a>(&'a mut self) -> &'a mut ResponseHeader {
        if self.header.is_none() {
            self.header.set_default();
        };
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> ResponseHeader {
        self.header.take().unwrap_or_else(|| ResponseHeader::new())
    }

    pub fn get_header<'a>(&'a self) -> &'a ResponseHeader {
        self.header.as_ref().unwrap_or_else(|| ResponseHeader::default_instance())
    }

    // optional .pdpb.CommandType cmd_type = 2;

    pub fn clear_cmd_type(&mut self) {
        self.cmd_type = ::std::option::Option::None;
    }

    pub fn has_cmd_type(&self) -> bool {
        self.cmd_type.is_some()
    }

    // Param is passed by value, moved
    pub fn set_cmd_type(&mut self, v: CommandType) {
        self.cmd_type = ::std::option::Option::Some(v);
    }

    pub fn get_cmd_type<'a>(&self) -> CommandType {
        self.cmd_type.unwrap_or(CommandType::Invalid)
    }

    // optional .pdpb.TsoResponse tso = 3;

    pub fn clear_tso(&mut self) {
        self.tso.clear();
    }

    pub fn has_tso(&self) -> bool {
        self.tso.is_some()
    }

    // Param is passed by value, moved
    pub fn set_tso(&mut self, v: TsoResponse) {
        self.tso = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_tso<'a>(&'a mut self) -> &'a mut TsoResponse {
        if self.tso.is_none() {
            self.tso.set_default();
        };
        self.tso.as_mut().unwrap()
    }

    // Take field
    pub fn take_tso(&mut self) -> TsoResponse {
        self.tso.take().unwrap_or_else(|| TsoResponse::new())
    }

    pub fn get_tso<'a>(&'a self) -> &'a TsoResponse {
        self.tso.as_ref().unwrap_or_else(|| TsoResponse::default_instance())
    }

    // optional .pdpb.BootstrapResponse bootstrap = 4;

    pub fn clear_bootstrap(&mut self) {
        self.bootstrap.clear();
    }

    pub fn has_bootstrap(&self) -> bool {
        self.bootstrap.is_some()
    }

    // Param is passed by value, moved
    pub fn set_bootstrap(&mut self, v: BootstrapResponse) {
        self.bootstrap = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_bootstrap<'a>(&'a mut self) -> &'a mut BootstrapResponse {
        if self.bootstrap.is_none() {
            self.bootstrap.set_default();
        };
        self.bootstrap.as_mut().unwrap()
    }

    // Take field
    pub fn take_bootstrap(&mut self) -> BootstrapResponse {
        self.bootstrap.take().unwrap_or_else(|| BootstrapResponse::new())
    }

    pub fn get_bootstrap<'a>(&'a self) -> &'a BootstrapResponse {
        self.bootstrap.as_ref().unwrap_or_else(|| BootstrapResponse::default_instance())
    }

    // optional .pdpb.IsBootstrappedResponse is_bootstrapped = 5;

    pub fn clear_is_bootstrapped(&mut self) {
        self.is_bootstrapped.clear();
    }

    pub fn has_is_bootstrapped(&self) -> bool {
        self.is_bootstrapped.is_some()
    }

    // Param is passed by value, moved
    pub fn set_is_bootstrapped(&mut self, v: IsBootstrappedResponse) {
        self.is_bootstrapped = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_is_bootstrapped<'a>(&'a mut self) -> &'a mut IsBootstrappedResponse {
        if self.is_bootstrapped.is_none() {
            self.is_bootstrapped.set_default();
        };
        self.is_bootstrapped.as_mut().unwrap()
    }

    // Take field
    pub fn take_is_bootstrapped(&mut self) -> IsBootstrappedResponse {
        self.is_bootstrapped.take().unwrap_or_else(|| IsBootstrappedResponse::new())
    }

    pub fn get_is_bootstrapped<'a>(&'a self) -> &'a IsBootstrappedResponse {
        self.is_bootstrapped.as_ref().unwrap_or_else(|| IsBootstrappedResponse::default_instance())
    }

    // optional .pdpb.AllocIdResponse alloc_id = 6;

    pub fn clear_alloc_id(&mut self) {
        self.alloc_id.clear();
    }

    pub fn has_alloc_id(&self) -> bool {
        self.alloc_id.is_some()
    }

    // Param is passed by value, moved
    pub fn set_alloc_id(&mut self, v: AllocIdResponse) {
        self.alloc_id = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_alloc_id<'a>(&'a mut self) -> &'a mut AllocIdResponse {
        if self.alloc_id.is_none() {
            self.alloc_id.set_default();
        };
        self.alloc_id.as_mut().unwrap()
    }

    // Take field
    pub fn take_alloc_id(&mut self) -> AllocIdResponse {
        self.alloc_id.take().unwrap_or_else(|| AllocIdResponse::new())
    }

    pub fn get_alloc_id<'a>(&'a self) -> &'a AllocIdResponse {
        self.alloc_id.as_ref().unwrap_or_else(|| AllocIdResponse::default_instance())
    }

    // optional .pdpb.GetMetaResponse get_meta = 7;

    pub fn clear_get_meta(&mut self) {
        self.get_meta.clear();
    }

    pub fn has_get_meta(&self) -> bool {
        self.get_meta.is_some()
    }

    // Param is passed by value, moved
    pub fn set_get_meta(&mut self, v: GetMetaResponse) {
        self.get_meta = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_get_meta<'a>(&'a mut self) -> &'a mut GetMetaResponse {
        if self.get_meta.is_none() {
            self.get_meta.set_default();
        };
        self.get_meta.as_mut().unwrap()
    }

    // Take field
    pub fn take_get_meta(&mut self) -> GetMetaResponse {
        self.get_meta.take().unwrap_or_else(|| GetMetaResponse::new())
    }

    pub fn get_get_meta<'a>(&'a self) -> &'a GetMetaResponse {
        self.get_meta.as_ref().unwrap_or_else(|| GetMetaResponse::default_instance())
    }

    // optional .pdpb.PutMetaResponse put_meta = 8;

    pub fn clear_put_meta(&mut self) {
        self.put_meta.clear();
    }

    pub fn has_put_meta(&self) -> bool {
        self.put_meta.is_some()
    }

    // Param is passed by value, moved
    pub fn set_put_meta(&mut self, v: PutMetaResponse) {
        self.put_meta = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_put_meta<'a>(&'a mut self) -> &'a mut PutMetaResponse {
        if self.put_meta.is_none() {
            self.put_meta.set_default();
        };
        self.put_meta.as_mut().unwrap()
    }

    // Take field
    pub fn take_put_meta(&mut self) -> PutMetaResponse {
        self.put_meta.take().unwrap_or_else(|| PutMetaResponse::new())
    }

    pub fn get_put_meta<'a>(&'a self) -> &'a PutMetaResponse {
        self.put_meta.as_ref().unwrap_or_else(|| PutMetaResponse::default_instance())
    }

    // optional .pdpb.DeleteMetaResponse delete_meta = 9;

    pub fn clear_delete_meta(&mut self) {
        self.delete_meta.clear();
    }

    pub fn has_delete_meta(&self) -> bool {
        self.delete_meta.is_some()
    }

    // Param is passed by value, moved
    pub fn set_delete_meta(&mut self, v: DeleteMetaResponse) {
        self.delete_meta = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_delete_meta<'a>(&'a mut self) -> &'a mut DeleteMetaResponse {
        if self.delete_meta.is_none() {
            self.delete_meta.set_default();
        };
        self.delete_meta.as_mut().unwrap()
    }

    // Take field
    pub fn take_delete_meta(&mut self) -> DeleteMetaResponse {
        self.delete_meta.take().unwrap_or_else(|| DeleteMetaResponse::new())
    }

    pub fn get_delete_meta<'a>(&'a self) -> &'a DeleteMetaResponse {
        self.delete_meta.as_ref().unwrap_or_else(|| DeleteMetaResponse::default_instance())
    }

    // optional .pdpb.AskChangePeerResponse ask_change_peer = 10;

    pub fn clear_ask_change_peer(&mut self) {
        self.ask_change_peer.clear();
    }

    pub fn has_ask_change_peer(&self) -> bool {
        self.ask_change_peer.is_some()
    }

    // Param is passed by value, moved
    pub fn set_ask_change_peer(&mut self, v: AskChangePeerResponse) {
        self.ask_change_peer = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_ask_change_peer<'a>(&'a mut self) -> &'a mut AskChangePeerResponse {
        if self.ask_change_peer.is_none() {
            self.ask_change_peer.set_default();
        };
        self.ask_change_peer.as_mut().unwrap()
    }

    // Take field
    pub fn take_ask_change_peer(&mut self) -> AskChangePeerResponse {
        self.ask_change_peer.take().unwrap_or_else(|| AskChangePeerResponse::new())
    }

    pub fn get_ask_change_peer<'a>(&'a self) -> &'a AskChangePeerResponse {
        self.ask_change_peer.as_ref().unwrap_or_else(|| AskChangePeerResponse::default_instance())
    }

    // optional .pdpb.AskSplitResponse ask_split = 11;

    pub fn clear_ask_split(&mut self) {
        self.ask_split.clear();
    }

    pub fn has_ask_split(&self) -> bool {
        self.ask_split.is_some()
    }

    // Param is passed by value, moved
    pub fn set_ask_split(&mut self, v: AskSplitResponse) {
        self.ask_split = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_ask_split<'a>(&'a mut self) -> &'a mut AskSplitResponse {
        if self.ask_split.is_none() {
            self.ask_split.set_default();
        };
        self.ask_split.as_mut().unwrap()
    }

    // Take field
    pub fn take_ask_split(&mut self) -> AskSplitResponse {
        self.ask_split.take().unwrap_or_else(|| AskSplitResponse::new())
    }

    pub fn get_ask_split<'a>(&'a self) -> &'a AskSplitResponse {
        self.ask_split.as_ref().unwrap_or_else(|| AskSplitResponse::default_instance())
    }
}

impl ::protobuf::Message for Response {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.header));
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    let tmp = try!(is.read_enum());
                    self.cmd_type = ::std::option::Option::Some(tmp);
                },
                3 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.tso));
                },
                4 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.bootstrap));
                },
                5 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.is_bootstrapped));
                },
                6 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.alloc_id));
                },
                7 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.get_meta));
                },
                8 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.put_meta));
                },
                9 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.delete_meta));
                },
                10 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.ask_change_peer));
                },
                11 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.ask_split));
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in self.header.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.cmd_type.iter() {
            my_size += ::protobuf::rt::enum_size(2, *value);
        };
        for value in self.tso.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.bootstrap.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.is_bootstrapped.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.alloc_id.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.get_meta.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.put_meta.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.delete_meta.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.ask_change_peer.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.ask_split.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.header.as_ref() {
            try!(os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.cmd_type {
            try!(os.write_enum(2, v.value()));
        };
        if let Some(v) = self.tso.as_ref() {
            try!(os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.bootstrap.as_ref() {
            try!(os.write_tag(4, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.is_bootstrapped.as_ref() {
            try!(os.write_tag(5, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.alloc_id.as_ref() {
            try!(os.write_tag(6, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.get_meta.as_ref() {
            try!(os.write_tag(7, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.put_meta.as_ref() {
            try!(os.write_tag(8, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.delete_meta.as_ref() {
            try!(os.write_tag(9, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.ask_change_peer.as_ref() {
            try!(os.write_tag(10, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.ask_split.as_ref() {
            try!(os.write_tag(11, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<Response>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Response {
    fn new() -> Response {
        Response::new()
    }

    fn descriptor_static(_: ::std::option::Option<Response>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "header",
                    Response::has_header,
                    Response::get_header,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_enum_accessor(
                    "cmd_type",
                    Response::has_cmd_type,
                    Response::get_cmd_type,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "tso",
                    Response::has_tso,
                    Response::get_tso,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "bootstrap",
                    Response::has_bootstrap,
                    Response::get_bootstrap,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "is_bootstrapped",
                    Response::has_is_bootstrapped,
                    Response::get_is_bootstrapped,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "alloc_id",
                    Response::has_alloc_id,
                    Response::get_alloc_id,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "get_meta",
                    Response::has_get_meta,
                    Response::get_get_meta,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "put_meta",
                    Response::has_put_meta,
                    Response::get_put_meta,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "delete_meta",
                    Response::has_delete_meta,
                    Response::get_delete_meta,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "ask_change_peer",
                    Response::has_ask_change_peer,
                    Response::get_ask_change_peer,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "ask_split",
                    Response::has_ask_split,
                    Response::get_ask_split,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Response>(
                    "Response",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Response {
    fn clear(&mut self) {
        self.clear_header();
        self.clear_cmd_type();
        self.clear_tso();
        self.clear_bootstrap();
        self.clear_is_bootstrapped();
        self.clear_alloc_id();
        self.clear_get_meta();
        self.clear_put_meta();
        self.clear_delete_meta();
        self.clear_ask_change_peer();
        self.clear_ask_split();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for Response {
    fn eq(&self, other: &Response) -> bool {
        self.header == other.header &&
        self.cmd_type == other.cmd_type &&
        self.tso == other.tso &&
        self.bootstrap == other.bootstrap &&
        self.is_bootstrapped == other.is_bootstrapped &&
        self.alloc_id == other.alloc_id &&
        self.get_meta == other.get_meta &&
        self.put_meta == other.put_meta &&
        self.delete_meta == other.delete_meta &&
        self.ask_change_peer == other.ask_change_peer &&
        self.ask_split == other.ask_split &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for Response {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct BootstrappedError {
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for BootstrappedError {}

impl BootstrappedError {
    pub fn new() -> BootstrappedError {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static BootstrappedError {
        static mut instance: ::protobuf::lazy::Lazy<BootstrappedError> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const BootstrappedError,
        };
        unsafe {
            instance.get(|| {
                BootstrappedError {
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }
}

impl ::protobuf::Message for BootstrappedError {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<BootstrappedError>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for BootstrappedError {
    fn new() -> BootstrappedError {
        BootstrappedError::new()
    }

    fn descriptor_static(_: ::std::option::Option<BootstrappedError>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let fields = ::std::vec::Vec::new();
                ::protobuf::reflect::MessageDescriptor::new::<BootstrappedError>(
                    "BootstrappedError",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for BootstrappedError {
    fn clear(&mut self) {
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for BootstrappedError {
    fn eq(&self, other: &BootstrappedError) -> bool {
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for BootstrappedError {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct Error {
    // message fields
    message: ::protobuf::SingularField<::std::string::String>,
    bootstrapped: ::protobuf::SingularPtrField<BootstrappedError>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for Error {}

impl Error {
    pub fn new() -> Error {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Error {
        static mut instance: ::protobuf::lazy::Lazy<Error> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Error,
        };
        unsafe {
            instance.get(|| {
                Error {
                    message: ::protobuf::SingularField::none(),
                    bootstrapped: ::protobuf::SingularPtrField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional string message = 1;

    pub fn clear_message(&mut self) {
        self.message.clear();
    }

    pub fn has_message(&self) -> bool {
        self.message.is_some()
    }

    // Param is passed by value, moved
    pub fn set_message(&mut self, v: ::std::string::String) {
        self.message = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_message<'a>(&'a mut self) -> &'a mut ::std::string::String {
        if self.message.is_none() {
            self.message.set_default();
        };
        self.message.as_mut().unwrap()
    }

    // Take field
    pub fn take_message(&mut self) -> ::std::string::String {
        self.message.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_message<'a>(&'a self) -> &'a str {
        match self.message.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }

    // optional .pdpb.BootstrappedError bootstrapped = 2;

    pub fn clear_bootstrapped(&mut self) {
        self.bootstrapped.clear();
    }

    pub fn has_bootstrapped(&self) -> bool {
        self.bootstrapped.is_some()
    }

    // Param is passed by value, moved
    pub fn set_bootstrapped(&mut self, v: BootstrappedError) {
        self.bootstrapped = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_bootstrapped<'a>(&'a mut self) -> &'a mut BootstrappedError {
        if self.bootstrapped.is_none() {
            self.bootstrapped.set_default();
        };
        self.bootstrapped.as_mut().unwrap()
    }

    // Take field
    pub fn take_bootstrapped(&mut self) -> BootstrappedError {
        self.bootstrapped.take().unwrap_or_else(|| BootstrappedError::new())
    }

    pub fn get_bootstrapped<'a>(&'a self) -> &'a BootstrappedError {
        self.bootstrapped.as_ref().unwrap_or_else(|| BootstrappedError::default_instance())
    }
}

impl ::protobuf::Message for Error {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    try!(::protobuf::rt::read_singular_string_into(wire_type, is, &mut self.message));
                },
                2 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.bootstrapped));
                },
                _ => {
                    try!(::protobuf::rt::read_unknown_or_skip_group(field_number, wire_type, is, self.mut_unknown_fields()));
                },
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in self.message.iter() {
            my_size += ::protobuf::rt::string_size(1, &value);
        };
        for value in self.bootstrapped.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.message.as_ref() {
            try!(os.write_string(1, &v));
        };
        if let Some(v) = self.bootstrapped.as_ref() {
            try!(os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields(&self) -> &::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields(&mut self) -> &mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<Error>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Error {
    fn new() -> Error {
        Error::new()
    }

    fn descriptor_static(_: ::std::option::Option<Error>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_string_accessor(
                    "message",
                    Error::has_message,
                    Error::get_message,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "bootstrapped",
                    Error::has_bootstrapped,
                    Error::get_bootstrapped,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Error>(
                    "Error",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for Error {
    fn clear(&mut self) {
        self.clear_message();
        self.clear_bootstrapped();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for Error {
    fn eq(&self, other: &Error) -> bool {
        self.message == other.message &&
        self.bootstrapped == other.bootstrapped &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for Error {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,PartialEq,Eq,Debug,Hash)]
pub enum CommandType {
    Invalid = 0,
    Tso = 1,
    Bootstrap = 2,
    IsBootstrapped = 3,
    AllocId = 4,
    GetMeta = 5,
    PutMeta = 6,
    DeleteMeta = 7,
    AskChangePeer = 8,
    AskSplit = 9,
}

impl ::protobuf::ProtobufEnum for CommandType {
    fn value(&self) -> i32 {
        *self as i32
    }

    fn from_i32(value: i32) -> ::std::option::Option<CommandType> {
        match value {
            0 => ::std::option::Option::Some(CommandType::Invalid),
            1 => ::std::option::Option::Some(CommandType::Tso),
            2 => ::std::option::Option::Some(CommandType::Bootstrap),
            3 => ::std::option::Option::Some(CommandType::IsBootstrapped),
            4 => ::std::option::Option::Some(CommandType::AllocId),
            5 => ::std::option::Option::Some(CommandType::GetMeta),
            6 => ::std::option::Option::Some(CommandType::PutMeta),
            7 => ::std::option::Option::Some(CommandType::DeleteMeta),
            8 => ::std::option::Option::Some(CommandType::AskChangePeer),
            9 => ::std::option::Option::Some(CommandType::AskSplit),
            _ => ::std::option::Option::None
        }
    }

    fn values() -> &'static [Self] {
        static values: &'static [CommandType] = &[
            CommandType::Invalid,
            CommandType::Tso,
            CommandType::Bootstrap,
            CommandType::IsBootstrapped,
            CommandType::AllocId,
            CommandType::GetMeta,
            CommandType::PutMeta,
            CommandType::DeleteMeta,
            CommandType::AskChangePeer,
            CommandType::AskSplit,
        ];
        values
    }

    fn enum_descriptor_static(_: Option<CommandType>) -> &'static ::protobuf::reflect::EnumDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::EnumDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::EnumDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                ::protobuf::reflect::EnumDescriptor::new("CommandType", file_descriptor_proto())
            })
        }
    }
}

impl ::std::marker::Copy for CommandType {
}

#[derive(Clone,PartialEq,Eq,Debug,Hash)]
pub enum MetaType {
    InvalidMeta = 0,
    NodeType = 1,
    StoreType = 2,
    RegionType = 3,
    PeerType = 4,
}

impl ::protobuf::ProtobufEnum for MetaType {
    fn value(&self) -> i32 {
        *self as i32
    }

    fn from_i32(value: i32) -> ::std::option::Option<MetaType> {
        match value {
            0 => ::std::option::Option::Some(MetaType::InvalidMeta),
            1 => ::std::option::Option::Some(MetaType::NodeType),
            2 => ::std::option::Option::Some(MetaType::StoreType),
            3 => ::std::option::Option::Some(MetaType::RegionType),
            4 => ::std::option::Option::Some(MetaType::PeerType),
            _ => ::std::option::Option::None
        }
    }

    fn values() -> &'static [Self] {
        static values: &'static [MetaType] = &[
            MetaType::InvalidMeta,
            MetaType::NodeType,
            MetaType::StoreType,
            MetaType::RegionType,
            MetaType::PeerType,
        ];
        values
    }

    fn enum_descriptor_static(_: Option<MetaType>) -> &'static ::protobuf::reflect::EnumDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::EnumDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::EnumDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                ::protobuf::reflect::EnumDescriptor::new("MetaType", file_descriptor_proto())
            })
        }
    }
}

impl ::std::marker::Copy for MetaType {
}

static file_descriptor_proto_data: &'static [u8] = &[
    0x0a, 0x0a, 0x70, 0x64, 0x70, 0x62, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12, 0x04, 0x70, 0x64,
    0x70, 0x62, 0x1a, 0x0c, 0x6d, 0x65, 0x74, 0x61, 0x70, 0x62, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
    0x22, 0x23, 0x0a, 0x06, 0x4c, 0x65, 0x61, 0x64, 0x65, 0x72, 0x12, 0x0c, 0x0a, 0x04, 0x61, 0x64,
    0x64, 0x72, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x12, 0x0b, 0x0a, 0x03, 0x70, 0x69, 0x64, 0x18,
    0x02, 0x20, 0x01, 0x28, 0x03, 0x22, 0x1c, 0x0a, 0x0a, 0x54, 0x73, 0x6f, 0x52, 0x65, 0x71, 0x75,
    0x65, 0x73, 0x74, 0x12, 0x0e, 0x0a, 0x06, 0x6e, 0x75, 0x6d, 0x62, 0x65, 0x72, 0x18, 0x01, 0x20,
    0x01, 0x28, 0x0d, 0x22, 0x2e, 0x0a, 0x09, 0x54, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70,
    0x12, 0x10, 0x0a, 0x08, 0x70, 0x68, 0x79, 0x73, 0x69, 0x63, 0x61, 0x6c, 0x18, 0x01, 0x20, 0x01,
    0x28, 0x03, 0x12, 0x0f, 0x0a, 0x07, 0x6c, 0x6f, 0x67, 0x69, 0x63, 0x61, 0x6c, 0x18, 0x02, 0x20,
    0x01, 0x28, 0x03, 0x22, 0x32, 0x0a, 0x0b, 0x54, 0x73, 0x6f, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e,
    0x73, 0x65, 0x12, 0x23, 0x0a, 0x0a, 0x74, 0x69, 0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x73,
    0x18, 0x01, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x0f, 0x2e, 0x70, 0x64, 0x70, 0x62, 0x2e, 0x54, 0x69,
    0x6d, 0x65, 0x73, 0x74, 0x61, 0x6d, 0x70, 0x22, 0x6d, 0x0a, 0x10, 0x42, 0x6f, 0x6f, 0x74, 0x73,
    0x74, 0x72, 0x61, 0x70, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x1a, 0x0a, 0x04, 0x6e,
    0x6f, 0x64, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0c, 0x2e, 0x6d, 0x65, 0x74, 0x61,
    0x70, 0x62, 0x2e, 0x4e, 0x6f, 0x64, 0x65, 0x12, 0x1d, 0x0a, 0x06, 0x73, 0x74, 0x6f, 0x72, 0x65,
    0x73, 0x18, 0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x0d, 0x2e, 0x6d, 0x65, 0x74, 0x61, 0x70, 0x62,
    0x2e, 0x53, 0x74, 0x6f, 0x72, 0x65, 0x12, 0x1e, 0x0a, 0x06, 0x72, 0x65, 0x67, 0x69, 0x6f, 0x6e,
    0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0e, 0x2e, 0x6d, 0x65, 0x74, 0x61, 0x70, 0x62, 0x2e,
    0x52, 0x65, 0x67, 0x69, 0x6f, 0x6e, 0x22, 0x13, 0x0a, 0x11, 0x42, 0x6f, 0x6f, 0x74, 0x73, 0x74,
    0x72, 0x61, 0x70, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x17, 0x0a, 0x15, 0x49,
    0x73, 0x42, 0x6f, 0x6f, 0x74, 0x73, 0x74, 0x72, 0x61, 0x70, 0x70, 0x65, 0x64, 0x52, 0x65, 0x71,
    0x75, 0x65, 0x73, 0x74, 0x22, 0x2e, 0x0a, 0x16, 0x49, 0x73, 0x42, 0x6f, 0x6f, 0x74, 0x73, 0x74,
    0x72, 0x61, 0x70, 0x70, 0x65, 0x64, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x14,
    0x0a, 0x0c, 0x62, 0x6f, 0x6f, 0x74, 0x73, 0x74, 0x72, 0x61, 0x70, 0x70, 0x65, 0x64, 0x18, 0x01,
    0x20, 0x01, 0x28, 0x08, 0x22, 0x10, 0x0a, 0x0e, 0x41, 0x6c, 0x6c, 0x6f, 0x63, 0x49, 0x64, 0x52,
    0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x22, 0x1d, 0x0a, 0x0f, 0x41, 0x6c, 0x6c, 0x6f, 0x63, 0x49,
    0x64, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x0a, 0x0a, 0x02, 0x69, 0x64, 0x18,
    0x02, 0x20, 0x01, 0x28, 0x04, 0x22, 0x6a, 0x0a, 0x0e, 0x47, 0x65, 0x74, 0x4d, 0x65, 0x74, 0x61,
    0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x21, 0x0a, 0x09, 0x6d, 0x65, 0x74, 0x61, 0x5f,
    0x74, 0x79, 0x70, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x0e, 0x2e, 0x70, 0x64, 0x70,
    0x62, 0x2e, 0x4d, 0x65, 0x74, 0x61, 0x54, 0x79, 0x70, 0x65, 0x12, 0x0f, 0x0a, 0x07, 0x6e, 0x6f,
    0x64, 0x65, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x12, 0x10, 0x0a, 0x08, 0x73,
    0x74, 0x6f, 0x72, 0x65, 0x5f, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01, 0x28, 0x04, 0x12, 0x12, 0x0a,
    0x0a, 0x72, 0x65, 0x67, 0x69, 0x6f, 0x6e, 0x5f, 0x6b, 0x65, 0x79, 0x18, 0x04, 0x20, 0x01, 0x28,
    0x09, 0x22, 0x8e, 0x01, 0x0a, 0x0f, 0x47, 0x65, 0x74, 0x4d, 0x65, 0x74, 0x61, 0x52, 0x65, 0x73,
    0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x21, 0x0a, 0x09, 0x6d, 0x65, 0x74, 0x61, 0x5f, 0x74, 0x79,
    0x70, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x0e, 0x2e, 0x70, 0x64, 0x70, 0x62, 0x2e,
    0x4d, 0x65, 0x74, 0x61, 0x54, 0x79, 0x70, 0x65, 0x12, 0x1a, 0x0a, 0x04, 0x6e, 0x6f, 0x64, 0x65,
    0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0c, 0x2e, 0x6d, 0x65, 0x74, 0x61, 0x70, 0x62, 0x2e,
    0x4e, 0x6f, 0x64, 0x65, 0x12, 0x1c, 0x0a, 0x05, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x18, 0x03, 0x20,
    0x01, 0x28, 0x0b, 0x32, 0x0d, 0x2e, 0x6d, 0x65, 0x74, 0x61, 0x70, 0x62, 0x2e, 0x53, 0x74, 0x6f,
    0x72, 0x65, 0x12, 0x1e, 0x0a, 0x06, 0x72, 0x65, 0x67, 0x69, 0x6f, 0x6e, 0x18, 0x04, 0x20, 0x01,
    0x28, 0x0b, 0x32, 0x0e, 0x2e, 0x6d, 0x65, 0x74, 0x61, 0x70, 0x62, 0x2e, 0x52, 0x65, 0x67, 0x69,
    0x6f, 0x6e, 0x22, 0x6d, 0x0a, 0x0e, 0x50, 0x75, 0x74, 0x4d, 0x65, 0x74, 0x61, 0x52, 0x65, 0x71,
    0x75, 0x65, 0x73, 0x74, 0x12, 0x21, 0x0a, 0x09, 0x6d, 0x65, 0x74, 0x61, 0x5f, 0x74, 0x79, 0x70,
    0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x0e, 0x2e, 0x70, 0x64, 0x70, 0x62, 0x2e, 0x4d,
    0x65, 0x74, 0x61, 0x54, 0x79, 0x70, 0x65, 0x12, 0x1a, 0x0a, 0x04, 0x6e, 0x6f, 0x64, 0x65, 0x18,
    0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0c, 0x2e, 0x6d, 0x65, 0x74, 0x61, 0x70, 0x62, 0x2e, 0x4e,
    0x6f, 0x64, 0x65, 0x12, 0x1c, 0x0a, 0x05, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x18, 0x03, 0x20, 0x01,
    0x28, 0x0b, 0x32, 0x0d, 0x2e, 0x6d, 0x65, 0x74, 0x61, 0x70, 0x62, 0x2e, 0x53, 0x74, 0x6f, 0x72,
    0x65, 0x22, 0x34, 0x0a, 0x0f, 0x50, 0x75, 0x74, 0x4d, 0x65, 0x74, 0x61, 0x52, 0x65, 0x73, 0x70,
    0x6f, 0x6e, 0x73, 0x65, 0x12, 0x21, 0x0a, 0x09, 0x6d, 0x65, 0x74, 0x61, 0x5f, 0x74, 0x79, 0x70,
    0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x0e, 0x2e, 0x70, 0x64, 0x70, 0x62, 0x2e, 0x4d,
    0x65, 0x74, 0x61, 0x54, 0x79, 0x70, 0x65, 0x22, 0x59, 0x0a, 0x11, 0x44, 0x65, 0x6c, 0x65, 0x74,
    0x65, 0x4d, 0x65, 0x74, 0x61, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x21, 0x0a, 0x09,
    0x6d, 0x65, 0x74, 0x61, 0x5f, 0x74, 0x79, 0x70, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32,
    0x0e, 0x2e, 0x70, 0x64, 0x70, 0x62, 0x2e, 0x4d, 0x65, 0x74, 0x61, 0x54, 0x79, 0x70, 0x65, 0x12,
    0x0f, 0x0a, 0x07, 0x6e, 0x6f, 0x64, 0x65, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04,
    0x12, 0x10, 0x0a, 0x08, 0x73, 0x74, 0x6f, 0x72, 0x65, 0x5f, 0x69, 0x64, 0x18, 0x03, 0x20, 0x01,
    0x28, 0x04, 0x22, 0x37, 0x0a, 0x12, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x4d, 0x65, 0x74, 0x61,
    0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x21, 0x0a, 0x09, 0x6d, 0x65, 0x74, 0x61,
    0x5f, 0x74, 0x79, 0x70, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x0e, 0x2e, 0x70, 0x64,
    0x70, 0x62, 0x2e, 0x4d, 0x65, 0x74, 0x61, 0x54, 0x79, 0x70, 0x65, 0x22, 0x36, 0x0a, 0x14, 0x41,
    0x73, 0x6b, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x50, 0x65, 0x65, 0x72, 0x52, 0x65, 0x71, 0x75,
    0x65, 0x73, 0x74, 0x12, 0x1e, 0x0a, 0x06, 0x72, 0x65, 0x67, 0x69, 0x6f, 0x6e, 0x18, 0x01, 0x20,
    0x01, 0x28, 0x0b, 0x32, 0x0e, 0x2e, 0x6d, 0x65, 0x74, 0x61, 0x70, 0x62, 0x2e, 0x52, 0x65, 0x67,
    0x69, 0x6f, 0x6e, 0x22, 0x17, 0x0a, 0x15, 0x41, 0x73, 0x6b, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65,
    0x50, 0x65, 0x65, 0x72, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x44, 0x0a, 0x0f,
    0x41, 0x73, 0x6b, 0x53, 0x70, 0x6c, 0x69, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12,
    0x1e, 0x0a, 0x06, 0x72, 0x65, 0x67, 0x69, 0x6f, 0x6e, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32,
    0x0e, 0x2e, 0x6d, 0x65, 0x74, 0x61, 0x70, 0x62, 0x2e, 0x52, 0x65, 0x67, 0x69, 0x6f, 0x6e, 0x12,
    0x11, 0x0a, 0x09, 0x73, 0x70, 0x6c, 0x69, 0x74, 0x5f, 0x6b, 0x65, 0x79, 0x18, 0x02, 0x20, 0x01,
    0x28, 0x0c, 0x22, 0x12, 0x0a, 0x10, 0x41, 0x73, 0x6b, 0x53, 0x70, 0x6c, 0x69, 0x74, 0x52, 0x65,
    0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x31, 0x0a, 0x0d, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
    0x74, 0x48, 0x65, 0x61, 0x64, 0x65, 0x72, 0x12, 0x0c, 0x0a, 0x04, 0x75, 0x75, 0x69, 0x64, 0x18,
    0x01, 0x20, 0x01, 0x28, 0x0c, 0x12, 0x12, 0x0a, 0x0a, 0x63, 0x6c, 0x75, 0x73, 0x74, 0x65, 0x72,
    0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x22, 0x4e, 0x0a, 0x0e, 0x52, 0x65, 0x73,
    0x70, 0x6f, 0x6e, 0x73, 0x65, 0x48, 0x65, 0x61, 0x64, 0x65, 0x72, 0x12, 0x0c, 0x0a, 0x04, 0x75,
    0x75, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x12, 0x12, 0x0a, 0x0a, 0x63, 0x6c, 0x75,
    0x73, 0x74, 0x65, 0x72, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x04, 0x12, 0x1a, 0x0a,
    0x05, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0b, 0x2e, 0x70,
    0x64, 0x70, 0x62, 0x2e, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x22, 0xd8, 0x03, 0x0a, 0x07, 0x52, 0x65,
    0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x23, 0x0a, 0x06, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72, 0x18,
    0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x13, 0x2e, 0x70, 0x64, 0x70, 0x62, 0x2e, 0x52, 0x65, 0x71,
    0x75, 0x65, 0x73, 0x74, 0x48, 0x65, 0x61, 0x64, 0x65, 0x72, 0x12, 0x23, 0x0a, 0x08, 0x63, 0x6d,
    0x64, 0x5f, 0x74, 0x79, 0x70, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x11, 0x2e, 0x70,
    0x64, 0x70, 0x62, 0x2e, 0x43, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x54, 0x79, 0x70, 0x65, 0x12,
    0x1d, 0x0a, 0x03, 0x74, 0x73, 0x6f, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x10, 0x2e, 0x70,
    0x64, 0x70, 0x62, 0x2e, 0x54, 0x73, 0x6f, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x29,
    0x0a, 0x09, 0x62, 0x6f, 0x6f, 0x74, 0x73, 0x74, 0x72, 0x61, 0x70, 0x18, 0x04, 0x20, 0x01, 0x28,
    0x0b, 0x32, 0x16, 0x2e, 0x70, 0x64, 0x70, 0x62, 0x2e, 0x42, 0x6f, 0x6f, 0x74, 0x73, 0x74, 0x72,
    0x61, 0x70, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x34, 0x0a, 0x0f, 0x69, 0x73, 0x5f,
    0x62, 0x6f, 0x6f, 0x74, 0x73, 0x74, 0x72, 0x61, 0x70, 0x70, 0x65, 0x64, 0x18, 0x05, 0x20, 0x01,
    0x28, 0x0b, 0x32, 0x1b, 0x2e, 0x70, 0x64, 0x70, 0x62, 0x2e, 0x49, 0x73, 0x42, 0x6f, 0x6f, 0x74,
    0x73, 0x74, 0x72, 0x61, 0x70, 0x70, 0x65, 0x64, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12,
    0x26, 0x0a, 0x08, 0x61, 0x6c, 0x6c, 0x6f, 0x63, 0x5f, 0x69, 0x64, 0x18, 0x06, 0x20, 0x01, 0x28,
    0x0b, 0x32, 0x14, 0x2e, 0x70, 0x64, 0x70, 0x62, 0x2e, 0x41, 0x6c, 0x6c, 0x6f, 0x63, 0x49, 0x64,
    0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x26, 0x0a, 0x08, 0x67, 0x65, 0x74, 0x5f, 0x6d,
    0x65, 0x74, 0x61, 0x18, 0x07, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x14, 0x2e, 0x70, 0x64, 0x70, 0x62,
    0x2e, 0x47, 0x65, 0x74, 0x4d, 0x65, 0x74, 0x61, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12,
    0x26, 0x0a, 0x08, 0x70, 0x75, 0x74, 0x5f, 0x6d, 0x65, 0x74, 0x61, 0x18, 0x08, 0x20, 0x01, 0x28,
    0x0b, 0x32, 0x14, 0x2e, 0x70, 0x64, 0x70, 0x62, 0x2e, 0x50, 0x75, 0x74, 0x4d, 0x65, 0x74, 0x61,
    0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x2c, 0x0a, 0x0b, 0x64, 0x65, 0x6c, 0x65, 0x74,
    0x65, 0x5f, 0x6d, 0x65, 0x74, 0x61, 0x18, 0x09, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x17, 0x2e, 0x70,
    0x64, 0x70, 0x62, 0x2e, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x4d, 0x65, 0x74, 0x61, 0x52, 0x65,
    0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x33, 0x0a, 0x0f, 0x61, 0x73, 0x6b, 0x5f, 0x63, 0x68, 0x61,
    0x6e, 0x67, 0x65, 0x5f, 0x70, 0x65, 0x65, 0x72, 0x18, 0x0a, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1a,
    0x2e, 0x70, 0x64, 0x70, 0x62, 0x2e, 0x41, 0x73, 0x6b, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x50,
    0x65, 0x65, 0x72, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x28, 0x0a, 0x09, 0x61, 0x73,
    0x6b, 0x5f, 0x73, 0x70, 0x6c, 0x69, 0x74, 0x18, 0x0b, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x15, 0x2e,
    0x70, 0x64, 0x70, 0x62, 0x2e, 0x41, 0x73, 0x6b, 0x53, 0x70, 0x6c, 0x69, 0x74, 0x52, 0x65, 0x71,
    0x75, 0x65, 0x73, 0x74, 0x22, 0xe3, 0x03, 0x0a, 0x08, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
    0x65, 0x12, 0x24, 0x0a, 0x06, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72, 0x18, 0x01, 0x20, 0x01, 0x28,
    0x0b, 0x32, 0x14, 0x2e, 0x70, 0x64, 0x70, 0x62, 0x2e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
    0x65, 0x48, 0x65, 0x61, 0x64, 0x65, 0x72, 0x12, 0x23, 0x0a, 0x08, 0x63, 0x6d, 0x64, 0x5f, 0x74,
    0x79, 0x70, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x11, 0x2e, 0x70, 0x64, 0x70, 0x62,
    0x2e, 0x43, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x54, 0x79, 0x70, 0x65, 0x12, 0x1e, 0x0a, 0x03,
    0x74, 0x73, 0x6f, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x11, 0x2e, 0x70, 0x64, 0x70, 0x62,
    0x2e, 0x54, 0x73, 0x6f, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x2a, 0x0a, 0x09,
    0x62, 0x6f, 0x6f, 0x74, 0x73, 0x74, 0x72, 0x61, 0x70, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32,
    0x17, 0x2e, 0x70, 0x64, 0x70, 0x62, 0x2e, 0x42, 0x6f, 0x6f, 0x74, 0x73, 0x74, 0x72, 0x61, 0x70,
    0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x35, 0x0a, 0x0f, 0x69, 0x73, 0x5f, 0x62,
    0x6f, 0x6f, 0x74, 0x73, 0x74, 0x72, 0x61, 0x70, 0x70, 0x65, 0x64, 0x18, 0x05, 0x20, 0x01, 0x28,
    0x0b, 0x32, 0x1c, 0x2e, 0x70, 0x64, 0x70, 0x62, 0x2e, 0x49, 0x73, 0x42, 0x6f, 0x6f, 0x74, 0x73,
    0x74, 0x72, 0x61, 0x70, 0x70, 0x65, 0x64, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12,
    0x27, 0x0a, 0x08, 0x61, 0x6c, 0x6c, 0x6f, 0x63, 0x5f, 0x69, 0x64, 0x18, 0x06, 0x20, 0x01, 0x28,
    0x0b, 0x32, 0x15, 0x2e, 0x70, 0x64, 0x70, 0x62, 0x2e, 0x41, 0x6c, 0x6c, 0x6f, 0x63, 0x49, 0x64,
    0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x27, 0x0a, 0x08, 0x67, 0x65, 0x74, 0x5f,
    0x6d, 0x65, 0x74, 0x61, 0x18, 0x07, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x15, 0x2e, 0x70, 0x64, 0x70,
    0x62, 0x2e, 0x47, 0x65, 0x74, 0x4d, 0x65, 0x74, 0x61, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
    0x65, 0x12, 0x27, 0x0a, 0x08, 0x70, 0x75, 0x74, 0x5f, 0x6d, 0x65, 0x74, 0x61, 0x18, 0x08, 0x20,
    0x01, 0x28, 0x0b, 0x32, 0x15, 0x2e, 0x70, 0x64, 0x70, 0x62, 0x2e, 0x50, 0x75, 0x74, 0x4d, 0x65,
    0x74, 0x61, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x2d, 0x0a, 0x0b, 0x64, 0x65,
    0x6c, 0x65, 0x74, 0x65, 0x5f, 0x6d, 0x65, 0x74, 0x61, 0x18, 0x09, 0x20, 0x01, 0x28, 0x0b, 0x32,
    0x18, 0x2e, 0x70, 0x64, 0x70, 0x62, 0x2e, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x4d, 0x65, 0x74,
    0x61, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x34, 0x0a, 0x0f, 0x61, 0x73, 0x6b,
    0x5f, 0x63, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x5f, 0x70, 0x65, 0x65, 0x72, 0x18, 0x0a, 0x20, 0x01,
    0x28, 0x0b, 0x32, 0x1b, 0x2e, 0x70, 0x64, 0x70, 0x62, 0x2e, 0x41, 0x73, 0x6b, 0x43, 0x68, 0x61,
    0x6e, 0x67, 0x65, 0x50, 0x65, 0x65, 0x72, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12,
    0x29, 0x0a, 0x09, 0x61, 0x73, 0x6b, 0x5f, 0x73, 0x70, 0x6c, 0x69, 0x74, 0x18, 0x0b, 0x20, 0x01,
    0x28, 0x0b, 0x32, 0x16, 0x2e, 0x70, 0x64, 0x70, 0x62, 0x2e, 0x41, 0x73, 0x6b, 0x53, 0x70, 0x6c,
    0x69, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x13, 0x0a, 0x11, 0x42, 0x6f,
    0x6f, 0x74, 0x73, 0x74, 0x72, 0x61, 0x70, 0x70, 0x65, 0x64, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x22,
    0x47, 0x0a, 0x05, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x12, 0x0f, 0x0a, 0x07, 0x6d, 0x65, 0x73, 0x73,
    0x61, 0x67, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x09, 0x12, 0x2d, 0x0a, 0x0c, 0x62, 0x6f, 0x6f,
    0x74, 0x73, 0x74, 0x72, 0x61, 0x70, 0x70, 0x65, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32,
    0x17, 0x2e, 0x70, 0x64, 0x70, 0x62, 0x2e, 0x42, 0x6f, 0x6f, 0x74, 0x73, 0x74, 0x72, 0x61, 0x70,
    0x70, 0x65, 0x64, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x2a, 0x9e, 0x01, 0x0a, 0x0b, 0x43, 0x6f, 0x6d,
    0x6d, 0x61, 0x6e, 0x64, 0x54, 0x79, 0x70, 0x65, 0x12, 0x0b, 0x0a, 0x07, 0x49, 0x6e, 0x76, 0x61,
    0x6c, 0x69, 0x64, 0x10, 0x00, 0x12, 0x07, 0x0a, 0x03, 0x54, 0x73, 0x6f, 0x10, 0x01, 0x12, 0x0d,
    0x0a, 0x09, 0x42, 0x6f, 0x6f, 0x74, 0x73, 0x74, 0x72, 0x61, 0x70, 0x10, 0x02, 0x12, 0x12, 0x0a,
    0x0e, 0x49, 0x73, 0x42, 0x6f, 0x6f, 0x74, 0x73, 0x74, 0x72, 0x61, 0x70, 0x70, 0x65, 0x64, 0x10,
    0x03, 0x12, 0x0b, 0x0a, 0x07, 0x41, 0x6c, 0x6c, 0x6f, 0x63, 0x49, 0x64, 0x10, 0x04, 0x12, 0x0b,
    0x0a, 0x07, 0x47, 0x65, 0x74, 0x4d, 0x65, 0x74, 0x61, 0x10, 0x05, 0x12, 0x0b, 0x0a, 0x07, 0x50,
    0x75, 0x74, 0x4d, 0x65, 0x74, 0x61, 0x10, 0x06, 0x12, 0x0e, 0x0a, 0x0a, 0x44, 0x65, 0x6c, 0x65,
    0x74, 0x65, 0x4d, 0x65, 0x74, 0x61, 0x10, 0x07, 0x12, 0x11, 0x0a, 0x0d, 0x41, 0x73, 0x6b, 0x43,
    0x68, 0x61, 0x6e, 0x67, 0x65, 0x50, 0x65, 0x65, 0x72, 0x10, 0x08, 0x12, 0x0c, 0x0a, 0x08, 0x41,
    0x73, 0x6b, 0x53, 0x70, 0x6c, 0x69, 0x74, 0x10, 0x09, 0x2a, 0x56, 0x0a, 0x08, 0x4d, 0x65, 0x74,
    0x61, 0x54, 0x79, 0x70, 0x65, 0x12, 0x0f, 0x0a, 0x0b, 0x49, 0x6e, 0x76, 0x61, 0x6c, 0x69, 0x64,
    0x4d, 0x65, 0x74, 0x61, 0x10, 0x00, 0x12, 0x0c, 0x0a, 0x08, 0x4e, 0x6f, 0x64, 0x65, 0x54, 0x79,
    0x70, 0x65, 0x10, 0x01, 0x12, 0x0d, 0x0a, 0x09, 0x53, 0x74, 0x6f, 0x72, 0x65, 0x54, 0x79, 0x70,
    0x65, 0x10, 0x02, 0x12, 0x0e, 0x0a, 0x0a, 0x52, 0x65, 0x67, 0x69, 0x6f, 0x6e, 0x54, 0x79, 0x70,
    0x65, 0x10, 0x03, 0x12, 0x0c, 0x0a, 0x08, 0x50, 0x65, 0x65, 0x72, 0x54, 0x79, 0x70, 0x65, 0x10,
    0x04, 0x4a, 0xf0, 0x2b, 0x0a, 0x07, 0x12, 0x05, 0x00, 0x00, 0xaa, 0x01, 0x01, 0x0a, 0x08, 0x0a,
    0x01, 0x02, 0x12, 0x03, 0x01, 0x08, 0x0c, 0x0a, 0x09, 0x0a, 0x02, 0x03, 0x00, 0x12, 0x03, 0x03,
    0x07, 0x15, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x00, 0x12, 0x04, 0x05, 0x00, 0x08, 0x01, 0x0a, 0x0a,
    0x0a, 0x03, 0x04, 0x00, 0x01, 0x12, 0x03, 0x05, 0x08, 0x0e, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x00,
    0x02, 0x00, 0x12, 0x03, 0x06, 0x04, 0x1e, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02, 0x00, 0x04,
    0x12, 0x03, 0x06, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02, 0x00, 0x05, 0x12, 0x03,
    0x06, 0x0d, 0x13, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02, 0x00, 0x01, 0x12, 0x03, 0x06, 0x14,
    0x18, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02, 0x00, 0x03, 0x12, 0x03, 0x06, 0x1c, 0x1d, 0x0a,
    0x0b, 0x0a, 0x04, 0x04, 0x00, 0x02, 0x01, 0x12, 0x03, 0x07, 0x04, 0x1e, 0x0a, 0x0c, 0x0a, 0x05,
    0x04, 0x00, 0x02, 0x01, 0x04, 0x12, 0x03, 0x07, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00,
    0x02, 0x01, 0x05, 0x12, 0x03, 0x07, 0x0d, 0x12, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02, 0x01,
    0x01, 0x12, 0x03, 0x07, 0x13, 0x16, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02, 0x01, 0x03, 0x12,
    0x03, 0x07, 0x1c, 0x1d, 0x0a, 0x0a, 0x0a, 0x02, 0x05, 0x00, 0x12, 0x04, 0x0a, 0x00, 0x15, 0x01,
    0x0a, 0x0a, 0x0a, 0x03, 0x05, 0x00, 0x01, 0x12, 0x03, 0x0a, 0x05, 0x10, 0x0a, 0x0b, 0x0a, 0x04,
    0x05, 0x00, 0x02, 0x00, 0x12, 0x03, 0x0b, 0x04, 0x17, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x00, 0x02,
    0x00, 0x01, 0x12, 0x03, 0x0b, 0x04, 0x0b, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x00, 0x02, 0x00, 0x02,
    0x12, 0x03, 0x0b, 0x15, 0x16, 0x0a, 0x0b, 0x0a, 0x04, 0x05, 0x00, 0x02, 0x01, 0x12, 0x03, 0x0c,
    0x04, 0x17, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x00, 0x02, 0x01, 0x01, 0x12, 0x03, 0x0c, 0x04, 0x07,
    0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x00, 0x02, 0x01, 0x02, 0x12, 0x03, 0x0c, 0x15, 0x16, 0x0a, 0x0b,
    0x0a, 0x04, 0x05, 0x00, 0x02, 0x02, 0x12, 0x03, 0x0d, 0x04, 0x17, 0x0a, 0x0c, 0x0a, 0x05, 0x05,
    0x00, 0x02, 0x02, 0x01, 0x12, 0x03, 0x0d, 0x04, 0x0d, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x00, 0x02,
    0x02, 0x02, 0x12, 0x03, 0x0d, 0x15, 0x16, 0x0a, 0x0b, 0x0a, 0x04, 0x05, 0x00, 0x02, 0x03, 0x12,
    0x03, 0x0e, 0x04, 0x17, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x00, 0x02, 0x03, 0x01, 0x12, 0x03, 0x0e,
    0x04, 0x12, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x00, 0x02, 0x03, 0x02, 0x12, 0x03, 0x0e, 0x15, 0x16,
    0x0a, 0x0b, 0x0a, 0x04, 0x05, 0x00, 0x02, 0x04, 0x12, 0x03, 0x0f, 0x04, 0x17, 0x0a, 0x0c, 0x0a,
    0x05, 0x05, 0x00, 0x02, 0x04, 0x01, 0x12, 0x03, 0x0f, 0x04, 0x0b, 0x0a, 0x0c, 0x0a, 0x05, 0x05,
    0x00, 0x02, 0x04, 0x02, 0x12, 0x03, 0x0f, 0x15, 0x16, 0x0a, 0x0b, 0x0a, 0x04, 0x05, 0x00, 0x02,
    0x05, 0x12, 0x03, 0x10, 0x04, 0x17, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x00, 0x02, 0x05, 0x01, 0x12,
    0x03, 0x10, 0x04, 0x0b, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x00, 0x02, 0x05, 0x02, 0x12, 0x03, 0x10,
    0x15, 0x16, 0x0a, 0x0b, 0x0a, 0x04, 0x05, 0x00, 0x02, 0x06, 0x12, 0x03, 0x11, 0x04, 0x17, 0x0a,
    0x0c, 0x0a, 0x05, 0x05, 0x00, 0x02, 0x06, 0x01, 0x12, 0x03, 0x11, 0x04, 0x0b, 0x0a, 0x0c, 0x0a,
    0x05, 0x05, 0x00, 0x02, 0x06, 0x02, 0x12, 0x03, 0x11, 0x15, 0x16, 0x0a, 0x0b, 0x0a, 0x04, 0x05,
    0x00, 0x02, 0x07, 0x12, 0x03, 0x12, 0x04, 0x17, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x00, 0x02, 0x07,
    0x01, 0x12, 0x03, 0x12, 0x04, 0x0e, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x00, 0x02, 0x07, 0x02, 0x12,
    0x03, 0x12, 0x15, 0x16, 0x0a, 0x0b, 0x0a, 0x04, 0x05, 0x00, 0x02, 0x08, 0x12, 0x03, 0x13, 0x04,
    0x17, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x00, 0x02, 0x08, 0x01, 0x12, 0x03, 0x13, 0x04, 0x11, 0x0a,
    0x0c, 0x0a, 0x05, 0x05, 0x00, 0x02, 0x08, 0x02, 0x12, 0x03, 0x13, 0x15, 0x16, 0x0a, 0x0b, 0x0a,
    0x04, 0x05, 0x00, 0x02, 0x09, 0x12, 0x03, 0x14, 0x04, 0x17, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x00,
    0x02, 0x09, 0x01, 0x12, 0x03, 0x14, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x00, 0x02, 0x09,
    0x02, 0x12, 0x03, 0x14, 0x15, 0x16, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x01, 0x12, 0x04, 0x17, 0x00,
    0x19, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x01, 0x01, 0x12, 0x03, 0x17, 0x08, 0x12, 0x0a, 0x0b,
    0x0a, 0x04, 0x04, 0x01, 0x02, 0x00, 0x12, 0x03, 0x18, 0x04, 0x1f, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x01, 0x02, 0x00, 0x04, 0x12, 0x03, 0x18, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x01, 0x02,
    0x00, 0x05, 0x12, 0x03, 0x18, 0x0d, 0x13, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x01, 0x02, 0x00, 0x01,
    0x12, 0x03, 0x18, 0x14, 0x1a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x01, 0x02, 0x00, 0x03, 0x12, 0x03,
    0x18, 0x1d, 0x1e, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x02, 0x12, 0x04, 0x1b, 0x00, 0x1e, 0x01, 0x0a,
    0x0a, 0x0a, 0x03, 0x04, 0x02, 0x01, 0x12, 0x03, 0x1b, 0x08, 0x11, 0x0a, 0x0b, 0x0a, 0x04, 0x04,
    0x02, 0x02, 0x00, 0x12, 0x03, 0x1c, 0x04, 0x20, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x02, 0x02, 0x00,
    0x04, 0x12, 0x03, 0x1c, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x02, 0x02, 0x00, 0x05, 0x12,
    0x03, 0x1c, 0x0d, 0x12, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x02, 0x02, 0x00, 0x01, 0x12, 0x03, 0x1c,
    0x13, 0x1b, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x02, 0x02, 0x00, 0x03, 0x12, 0x03, 0x1c, 0x1e, 0x1f,
    0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x02, 0x02, 0x01, 0x12, 0x03, 0x1d, 0x04, 0x20, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x02, 0x02, 0x01, 0x04, 0x12, 0x03, 0x1d, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x02, 0x02, 0x01, 0x05, 0x12, 0x03, 0x1d, 0x0d, 0x12, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x02, 0x02,
    0x01, 0x01, 0x12, 0x03, 0x1d, 0x13, 0x1a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x02, 0x02, 0x01, 0x03,
    0x12, 0x03, 0x1d, 0x1e, 0x1f, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x03, 0x12, 0x04, 0x20, 0x00, 0x22,
    0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x03, 0x01, 0x12, 0x03, 0x20, 0x08, 0x13, 0x0a, 0x0b, 0x0a,
    0x04, 0x04, 0x03, 0x02, 0x00, 0x12, 0x03, 0x21, 0x04, 0x26, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03,
    0x02, 0x00, 0x04, 0x12, 0x03, 0x21, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x00,
    0x06, 0x12, 0x03, 0x21, 0x0d, 0x16, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x00, 0x01, 0x12,
    0x03, 0x21, 0x17, 0x21, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x00, 0x03, 0x12, 0x03, 0x21,
    0x24, 0x25, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x04, 0x12, 0x04, 0x24, 0x00, 0x28, 0x01, 0x0a, 0x0a,
    0x0a, 0x03, 0x04, 0x04, 0x01, 0x12, 0x03, 0x24, 0x08, 0x18, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x04,
    0x02, 0x00, 0x12, 0x03, 0x25, 0x04, 0x26, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04, 0x02, 0x00, 0x04,
    0x12, 0x03, 0x25, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04, 0x02, 0x00, 0x06, 0x12, 0x03,
    0x25, 0x0d, 0x18, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04, 0x02, 0x00, 0x01, 0x12, 0x03, 0x25, 0x19,
    0x1d, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04, 0x02, 0x00, 0x03, 0x12, 0x03, 0x25, 0x24, 0x25, 0x0a,
    0x0b, 0x0a, 0x04, 0x04, 0x04, 0x02, 0x01, 0x12, 0x03, 0x26, 0x04, 0x26, 0x0a, 0x0c, 0x0a, 0x05,
    0x04, 0x04, 0x02, 0x01, 0x04, 0x12, 0x03, 0x26, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04,
    0x02, 0x01, 0x06, 0x12, 0x03, 0x26, 0x0d, 0x19, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04, 0x02, 0x01,
    0x01, 0x12, 0x03, 0x26, 0x1a, 0x20, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04, 0x02, 0x01, 0x03, 0x12,
    0x03, 0x26, 0x24, 0x25, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x04, 0x02, 0x02, 0x12, 0x03, 0x27, 0x04,
    0x26, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04, 0x02, 0x02, 0x04, 0x12, 0x03, 0x27, 0x04, 0x0c, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x04, 0x02, 0x02, 0x06, 0x12, 0x03, 0x27, 0x0d, 0x1a, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x04, 0x02, 0x02, 0x01, 0x12, 0x03, 0x27, 0x1b, 0x21, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x04, 0x02, 0x02, 0x03, 0x12, 0x03, 0x27, 0x24, 0x25, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x05, 0x12,
    0x04, 0x2a, 0x00, 0x2c, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x05, 0x01, 0x12, 0x03, 0x2a, 0x08,
    0x19, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x06, 0x12, 0x04, 0x2e, 0x00, 0x30, 0x01, 0x0a, 0x0a, 0x0a,
    0x03, 0x04, 0x06, 0x01, 0x12, 0x03, 0x2e, 0x08, 0x1d, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x07, 0x12,
    0x04, 0x32, 0x00, 0x34, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x07, 0x01, 0x12, 0x03, 0x32, 0x08,
    0x1e, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x07, 0x02, 0x00, 0x12, 0x03, 0x33, 0x04, 0x23, 0x0a, 0x0c,
    0x0a, 0x05, 0x04, 0x07, 0x02, 0x00, 0x04, 0x12, 0x03, 0x33, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05,
    0x04, 0x07, 0x02, 0x00, 0x05, 0x12, 0x03, 0x33, 0x0d, 0x11, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x07,
    0x02, 0x00, 0x01, 0x12, 0x03, 0x33, 0x12, 0x1e, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x07, 0x02, 0x00,
    0x03, 0x12, 0x03, 0x33, 0x21, 0x22, 0x0a, 0x0a, 0x0a, 0x02, 0x05, 0x01, 0x12, 0x04, 0x36, 0x00,
    0x3c, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x05, 0x01, 0x01, 0x12, 0x03, 0x36, 0x05, 0x0d, 0x0a, 0x0b,
    0x0a, 0x04, 0x05, 0x01, 0x02, 0x00, 0x12, 0x03, 0x37, 0x04, 0x19, 0x0a, 0x0c, 0x0a, 0x05, 0x05,
    0x01, 0x02, 0x00, 0x01, 0x12, 0x03, 0x37, 0x04, 0x0f, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x01, 0x02,
    0x00, 0x02, 0x12, 0x03, 0x37, 0x17, 0x18, 0x0a, 0x0b, 0x0a, 0x04, 0x05, 0x01, 0x02, 0x01, 0x12,
    0x03, 0x38, 0x04, 0x19, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x01, 0x02, 0x01, 0x01, 0x12, 0x03, 0x38,
    0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x01, 0x02, 0x01, 0x02, 0x12, 0x03, 0x38, 0x17, 0x18,
    0x0a, 0x0b, 0x0a, 0x04, 0x05, 0x01, 0x02, 0x02, 0x12, 0x03, 0x39, 0x04, 0x19, 0x0a, 0x0c, 0x0a,
    0x05, 0x05, 0x01, 0x02, 0x02, 0x01, 0x12, 0x03, 0x39, 0x04, 0x0d, 0x0a, 0x0c, 0x0a, 0x05, 0x05,
    0x01, 0x02, 0x02, 0x02, 0x12, 0x03, 0x39, 0x17, 0x18, 0x0a, 0x0b, 0x0a, 0x04, 0x05, 0x01, 0x02,
    0x03, 0x12, 0x03, 0x3a, 0x04, 0x19, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x01, 0x02, 0x03, 0x01, 0x12,
    0x03, 0x3a, 0x04, 0x0e, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x01, 0x02, 0x03, 0x02, 0x12, 0x03, 0x3a,
    0x17, 0x18, 0x0a, 0x0b, 0x0a, 0x04, 0x05, 0x01, 0x02, 0x04, 0x12, 0x03, 0x3b, 0x04, 0x19, 0x0a,
    0x0c, 0x0a, 0x05, 0x05, 0x01, 0x02, 0x04, 0x01, 0x12, 0x03, 0x3b, 0x04, 0x0c, 0x0a, 0x0c, 0x0a,
    0x05, 0x05, 0x01, 0x02, 0x04, 0x02, 0x12, 0x03, 0x3b, 0x17, 0x18, 0x0a, 0x0a, 0x0a, 0x02, 0x04,
    0x08, 0x12, 0x04, 0x3e, 0x00, 0x40, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x08, 0x01, 0x12, 0x03,
    0x3e, 0x08, 0x16, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x09, 0x12, 0x04, 0x42, 0x00, 0x44, 0x01, 0x0a,
    0x0a, 0x0a, 0x03, 0x04, 0x09, 0x01, 0x12, 0x03, 0x42, 0x08, 0x17, 0x0a, 0x0b, 0x0a, 0x04, 0x04,
    0x09, 0x02, 0x00, 0x12, 0x03, 0x43, 0x04, 0x26, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x09, 0x02, 0x00,
    0x04, 0x12, 0x03, 0x43, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x09, 0x02, 0x00, 0x05, 0x12,
    0x03, 0x43, 0x0d, 0x13, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x09, 0x02, 0x00, 0x01, 0x12, 0x03, 0x43,
    0x14, 0x16, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x09, 0x02, 0x00, 0x03, 0x12, 0x03, 0x43, 0x24, 0x25,
    0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x0a, 0x12, 0x04, 0x46, 0x00, 0x4b, 0x01, 0x0a, 0x0a, 0x0a, 0x03,
    0x04, 0x0a, 0x01, 0x12, 0x03, 0x46, 0x08, 0x16, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x0a, 0x02, 0x00,
    0x12, 0x03, 0x47, 0x04, 0x27, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0a, 0x02, 0x00, 0x04, 0x12, 0x03,
    0x47, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0a, 0x02, 0x00, 0x06, 0x12, 0x03, 0x47, 0x0d,
    0x15, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0a, 0x02, 0x00, 0x01, 0x12, 0x03, 0x47, 0x16, 0x1f, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x0a, 0x02, 0x00, 0x03, 0x12, 0x03, 0x47, 0x25, 0x26, 0x0a, 0x0b, 0x0a,
    0x04, 0x04, 0x0a, 0x02, 0x01, 0x12, 0x03, 0x48, 0x04, 0x27, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0a,
    0x02, 0x01, 0x04, 0x12, 0x03, 0x48, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0a, 0x02, 0x01,
    0x05, 0x12, 0x03, 0x48, 0x0d, 0x13, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0a, 0x02, 0x01, 0x01, 0x12,
    0x03, 0x48, 0x14, 0x1b, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0a, 0x02, 0x01, 0x03, 0x12, 0x03, 0x48,
    0x25, 0x26, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x0a, 0x02, 0x02, 0x12, 0x03, 0x49, 0x04, 0x27, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x0a, 0x02, 0x02, 0x04, 0x12, 0x03, 0x49, 0x04, 0x0c, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x0a, 0x02, 0x02, 0x05, 0x12, 0x03, 0x49, 0x0d, 0x13, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x0a, 0x02, 0x02, 0x01, 0x12, 0x03, 0x49, 0x14, 0x1c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0a, 0x02,
    0x02, 0x03, 0x12, 0x03, 0x49, 0x25, 0x26, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x0a, 0x02, 0x03, 0x12,
    0x03, 0x4a, 0x04, 0x27, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0a, 0x02, 0x03, 0x04, 0x12, 0x03, 0x4a,
    0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0a, 0x02, 0x03, 0x05, 0x12, 0x03, 0x4a, 0x0d, 0x13,
    0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0a, 0x02, 0x03, 0x01, 0x12, 0x03, 0x4a, 0x14, 0x1e, 0x0a, 0x0c,
    0x0a, 0x05, 0x04, 0x0a, 0x02, 0x03, 0x03, 0x12, 0x03, 0x4a, 0x25, 0x26, 0x0a, 0x0a, 0x0a, 0x02,
    0x04, 0x0b, 0x12, 0x04, 0x4d, 0x00, 0x53, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x0b, 0x01, 0x12,
    0x03, 0x4d, 0x08, 0x17, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x0b, 0x02, 0x00, 0x12, 0x03, 0x4e, 0x04,
    0x27, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0b, 0x02, 0x00, 0x04, 0x12, 0x03, 0x4e, 0x04, 0x0c, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x0b, 0x02, 0x00, 0x06, 0x12, 0x03, 0x4e, 0x0d, 0x15, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x0b, 0x02, 0x00, 0x01, 0x12, 0x03, 0x4e, 0x16, 0x1f, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x0b, 0x02, 0x00, 0x03, 0x12, 0x03, 0x4e, 0x25, 0x26, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x0b, 0x02,
    0x01, 0x12, 0x03, 0x4f, 0x04, 0x27, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0b, 0x02, 0x01, 0x04, 0x12,
    0x03, 0x4f, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0b, 0x02, 0x01, 0x06, 0x12, 0x03, 0x4f,
    0x0d, 0x18, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0b, 0x02, 0x01, 0x01, 0x12, 0x03, 0x4f, 0x19, 0x1d,
    0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0b, 0x02, 0x01, 0x03, 0x12, 0x03, 0x4f, 0x25, 0x26, 0x0a, 0x0b,
    0x0a, 0x04, 0x04, 0x0b, 0x02, 0x02, 0x12, 0x03, 0x50, 0x04, 0x27, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x0b, 0x02, 0x02, 0x04, 0x12, 0x03, 0x50, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0b, 0x02,
    0x02, 0x06, 0x12, 0x03, 0x50, 0x0d, 0x19, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0b, 0x02, 0x02, 0x01,
    0x12, 0x03, 0x50, 0x1a, 0x1f, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0b, 0x02, 0x02, 0x03, 0x12, 0x03,
    0x50, 0x25, 0x26, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x0b, 0x02, 0x03, 0x12, 0x03, 0x51, 0x04, 0x27,
    0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0b, 0x02, 0x03, 0x04, 0x12, 0x03, 0x51, 0x04, 0x0c, 0x0a, 0x0c,
    0x0a, 0x05, 0x04, 0x0b, 0x02, 0x03, 0x06, 0x12, 0x03, 0x51, 0x0d, 0x1a, 0x0a, 0x0c, 0x0a, 0x05,
    0x04, 0x0b, 0x02, 0x03, 0x01, 0x12, 0x03, 0x51, 0x1b, 0x21, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0b,
    0x02, 0x03, 0x03, 0x12, 0x03, 0x51, 0x25, 0x26, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x0c, 0x12, 0x04,
    0x55, 0x00, 0x59, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x0c, 0x01, 0x12, 0x03, 0x55, 0x08, 0x16,
    0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x0c, 0x02, 0x00, 0x12, 0x03, 0x56, 0x04, 0x25, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x0c, 0x02, 0x00, 0x04, 0x12, 0x03, 0x56, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x0c, 0x02, 0x00, 0x06, 0x12, 0x03, 0x56, 0x0d, 0x15, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0c, 0x02,
    0x00, 0x01, 0x12, 0x03, 0x56, 0x16, 0x1f, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0c, 0x02, 0x00, 0x03,
    0x12, 0x03, 0x56, 0x23, 0x24, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x0c, 0x02, 0x01, 0x12, 0x03, 0x57,
    0x04, 0x25, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0c, 0x02, 0x01, 0x04, 0x12, 0x03, 0x57, 0x04, 0x0c,
    0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0c, 0x02, 0x01, 0x06, 0x12, 0x03, 0x57, 0x0d, 0x18, 0x0a, 0x0c,
    0x0a, 0x05, 0x04, 0x0c, 0x02, 0x01, 0x01, 0x12, 0x03, 0x57, 0x19, 0x1d, 0x0a, 0x0c, 0x0a, 0x05,
    0x04, 0x0c, 0x02, 0x01, 0x03, 0x12, 0x03, 0x57, 0x23, 0x24, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x0c,
    0x02, 0x02, 0x12, 0x03, 0x58, 0x04, 0x25, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0c, 0x02, 0x02, 0x04,
    0x12, 0x03, 0x58, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0c, 0x02, 0x02, 0x06, 0x12, 0x03,
    0x58, 0x0d, 0x19, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0c, 0x02, 0x02, 0x01, 0x12, 0x03, 0x58, 0x1a,
    0x1f, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0c, 0x02, 0x02, 0x03, 0x12, 0x03, 0x58, 0x23, 0x24, 0x0a,
    0x0a, 0x0a, 0x02, 0x04, 0x0d, 0x12, 0x04, 0x5b, 0x00, 0x5d, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04,
    0x0d, 0x01, 0x12, 0x03, 0x5b, 0x08, 0x17, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x0d, 0x02, 0x00, 0x12,
    0x03, 0x5c, 0x04, 0x24, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0d, 0x02, 0x00, 0x04, 0x12, 0x03, 0x5c,
    0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0d, 0x02, 0x00, 0x06, 0x12, 0x03, 0x5c, 0x0d, 0x15,
    0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0d, 0x02, 0x00, 0x01, 0x12, 0x03, 0x5c, 0x16, 0x1f, 0x0a, 0x0c,
    0x0a, 0x05, 0x04, 0x0d, 0x02, 0x00, 0x03, 0x12, 0x03, 0x5c, 0x22, 0x23, 0x0a, 0x0a, 0x0a, 0x02,
    0x04, 0x0e, 0x12, 0x04, 0x5f, 0x00, 0x63, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x0e, 0x01, 0x12,
    0x03, 0x5f, 0x08, 0x19, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x0e, 0x02, 0x00, 0x12, 0x03, 0x60, 0x04,
    0x25, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0e, 0x02, 0x00, 0x04, 0x12, 0x03, 0x60, 0x04, 0x0c, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x0e, 0x02, 0x00, 0x06, 0x12, 0x03, 0x60, 0x0d, 0x15, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x0e, 0x02, 0x00, 0x01, 0x12, 0x03, 0x60, 0x16, 0x1f, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x0e, 0x02, 0x00, 0x03, 0x12, 0x03, 0x60, 0x23, 0x24, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x0e, 0x02,
    0x01, 0x12, 0x03, 0x61, 0x04, 0x25, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0e, 0x02, 0x01, 0x04, 0x12,
    0x03, 0x61, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0e, 0x02, 0x01, 0x05, 0x12, 0x03, 0x61,
    0x0d, 0x13, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0e, 0x02, 0x01, 0x01, 0x12, 0x03, 0x61, 0x14, 0x1b,
    0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0e, 0x02, 0x01, 0x03, 0x12, 0x03, 0x61, 0x23, 0x24, 0x0a, 0x0b,
    0x0a, 0x04, 0x04, 0x0e, 0x02, 0x02, 0x12, 0x03, 0x62, 0x04, 0x25, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x0e, 0x02, 0x02, 0x04, 0x12, 0x03, 0x62, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0e, 0x02,
    0x02, 0x05, 0x12, 0x03, 0x62, 0x0d, 0x13, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0e, 0x02, 0x02, 0x01,
    0x12, 0x03, 0x62, 0x14, 0x1c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0e, 0x02, 0x02, 0x03, 0x12, 0x03,
    0x62, 0x23, 0x24, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x0f, 0x12, 0x04, 0x65, 0x00, 0x67, 0x01, 0x0a,
    0x0a, 0x0a, 0x03, 0x04, 0x0f, 0x01, 0x12, 0x03, 0x65, 0x08, 0x1a, 0x0a, 0x0b, 0x0a, 0x04, 0x04,
    0x0f, 0x02, 0x00, 0x12, 0x03, 0x66, 0x04, 0x24, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0f, 0x02, 0x00,
    0x04, 0x12, 0x03, 0x66, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0f, 0x02, 0x00, 0x06, 0x12,
    0x03, 0x66, 0x0d, 0x15, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0f, 0x02, 0x00, 0x01, 0x12, 0x03, 0x66,
    0x16, 0x1f, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0f, 0x02, 0x00, 0x03, 0x12, 0x03, 0x66, 0x22, 0x23,
    0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x10, 0x12, 0x04, 0x69, 0x00, 0x6b, 0x01, 0x0a, 0x0a, 0x0a, 0x03,
    0x04, 0x10, 0x01, 0x12, 0x03, 0x69, 0x08, 0x1c, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x10, 0x02, 0x00,
    0x12, 0x03, 0x6a, 0x04, 0x26, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x10, 0x02, 0x00, 0x04, 0x12, 0x03,
    0x6a, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x10, 0x02, 0x00, 0x06, 0x12, 0x03, 0x6a, 0x0d,
    0x1a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x10, 0x02, 0x00, 0x01, 0x12, 0x03, 0x6a, 0x1b, 0x21, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x10, 0x02, 0x00, 0x03, 0x12, 0x03, 0x6a, 0x24, 0x25, 0x0a, 0x0a, 0x0a,
    0x02, 0x04, 0x11, 0x12, 0x04, 0x6d, 0x00, 0x6f, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x11, 0x01,
    0x12, 0x03, 0x6d, 0x08, 0x1d, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x12, 0x12, 0x04, 0x71, 0x00, 0x74,
    0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x12, 0x01, 0x12, 0x03, 0x71, 0x08, 0x17, 0x0a, 0x0b, 0x0a,
    0x04, 0x04, 0x12, 0x02, 0x00, 0x12, 0x03, 0x72, 0x04, 0x26, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x12,
    0x02, 0x00, 0x04, 0x12, 0x03, 0x72, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x12, 0x02, 0x00,
    0x06, 0x12, 0x03, 0x72, 0x0d, 0x1a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x12, 0x02, 0x00, 0x01, 0x12,
    0x03, 0x72, 0x1b, 0x21, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x12, 0x02, 0x00, 0x03, 0x12, 0x03, 0x72,
    0x24, 0x25, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x12, 0x02, 0x01, 0x12, 0x03, 0x73, 0x04, 0x26, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x12, 0x02, 0x01, 0x04, 0x12, 0x03, 0x73, 0x04, 0x0c, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x12, 0x02, 0x01, 0x05, 0x12, 0x03, 0x73, 0x0d, 0x12, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x12, 0x02, 0x01, 0x01, 0x12, 0x03, 0x73, 0x13, 0x1c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x12, 0x02,
    0x01, 0x03, 0x12, 0x03, 0x73, 0x24, 0x25, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x13, 0x12, 0x04, 0x76,
    0x00, 0x78, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x13, 0x01, 0x12, 0x03, 0x76, 0x08, 0x18, 0x0a,
    0x0a, 0x0a, 0x02, 0x04, 0x14, 0x12, 0x04, 0x7a, 0x00, 0x7e, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04,
    0x14, 0x01, 0x12, 0x03, 0x7a, 0x08, 0x15, 0x0a, 0x32, 0x0a, 0x04, 0x04, 0x14, 0x02, 0x00, 0x12,
    0x03, 0x7c, 0x04, 0x2b, 0x1a, 0x25, 0x20, 0x31, 0x36, 0x20, 0x62, 0x79, 0x74, 0x65, 0x73, 0x2c,
    0x20, 0x74, 0x6f, 0x20, 0x64, 0x69, 0x73, 0x74, 0x69, 0x6e, 0x67, 0x75, 0x69, 0x73, 0x68, 0x20,
    0x72, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x2e, 0x20, 0x20, 0x0a, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x14, 0x02, 0x00, 0x04, 0x12, 0x03, 0x7c, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x14, 0x02,
    0x00, 0x05, 0x12, 0x03, 0x7c, 0x0d, 0x12, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x14, 0x02, 0x00, 0x01,
    0x12, 0x03, 0x7c, 0x13, 0x17, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x14, 0x02, 0x00, 0x03, 0x12, 0x03,
    0x7c, 0x29, 0x2a, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x14, 0x02, 0x01, 0x12, 0x03, 0x7d, 0x04, 0x2b,
    0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x14, 0x02, 0x01, 0x04, 0x12, 0x03, 0x7d, 0x04, 0x0c, 0x0a, 0x0c,
    0x0a, 0x05, 0x04, 0x14, 0x02, 0x01, 0x05, 0x12, 0x03, 0x7d, 0x0d, 0x13, 0x0a, 0x0c, 0x0a, 0x05,
    0x04, 0x14, 0x02, 0x01, 0x01, 0x12, 0x03, 0x7d, 0x14, 0x1e, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x14,
    0x02, 0x01, 0x03, 0x12, 0x03, 0x7d, 0x29, 0x2a, 0x0a, 0x0c, 0x0a, 0x02, 0x04, 0x15, 0x12, 0x06,
    0x80, 0x01, 0x00, 0x85, 0x01, 0x01, 0x0a, 0x0b, 0x0a, 0x03, 0x04, 0x15, 0x01, 0x12, 0x04, 0x80,
    0x01, 0x08, 0x16, 0x0a, 0x33, 0x0a, 0x04, 0x04, 0x15, 0x02, 0x00, 0x12, 0x04, 0x82, 0x01, 0x04,
    0x2b, 0x1a, 0x25, 0x20, 0x31, 0x36, 0x20, 0x62, 0x79, 0x74, 0x65, 0x73, 0x2c, 0x20, 0x74, 0x6f,
    0x20, 0x64, 0x69, 0x73, 0x74, 0x69, 0x6e, 0x67, 0x75, 0x69, 0x73, 0x68, 0x20, 0x72, 0x65, 0x71,
    0x75, 0x65, 0x73, 0x74, 0x2e, 0x20, 0x20, 0x0a, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x15, 0x02, 0x00,
    0x04, 0x12, 0x04, 0x82, 0x01, 0x04, 0x0c, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x15, 0x02, 0x00, 0x05,
    0x12, 0x04, 0x82, 0x01, 0x0d, 0x12, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x15, 0x02, 0x00, 0x01, 0x12,
    0x04, 0x82, 0x01, 0x13, 0x17, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x15, 0x02, 0x00, 0x03, 0x12, 0x04,
    0x82, 0x01, 0x29, 0x2a, 0x0a, 0x0c, 0x0a, 0x04, 0x04, 0x15, 0x02, 0x01, 0x12, 0x04, 0x83, 0x01,
    0x04, 0x2b, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x15, 0x02, 0x01, 0x04, 0x12, 0x04, 0x83, 0x01, 0x04,
    0x0c, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x15, 0x02, 0x01, 0x05, 0x12, 0x04, 0x83, 0x01, 0x0d, 0x13,
    0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x15, 0x02, 0x01, 0x01, 0x12, 0x04, 0x83, 0x01, 0x14, 0x1e, 0x0a,
    0x0d, 0x0a, 0x05, 0x04, 0x15, 0x02, 0x01, 0x03, 0x12, 0x04, 0x83, 0x01, 0x29, 0x2a, 0x0a, 0x0c,
    0x0a, 0x04, 0x04, 0x15, 0x02, 0x02, 0x12, 0x04, 0x84, 0x01, 0x04, 0x2b, 0x0a, 0x0d, 0x0a, 0x05,
    0x04, 0x15, 0x02, 0x02, 0x04, 0x12, 0x04, 0x84, 0x01, 0x04, 0x0c, 0x0a, 0x0d, 0x0a, 0x05, 0x04,
    0x15, 0x02, 0x02, 0x06, 0x12, 0x04, 0x84, 0x01, 0x0d, 0x12, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x15,
    0x02, 0x02, 0x01, 0x12, 0x04, 0x84, 0x01, 0x13, 0x18, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x15, 0x02,
    0x02, 0x03, 0x12, 0x04, 0x84, 0x01, 0x29, 0x2a, 0x0a, 0x0c, 0x0a, 0x02, 0x04, 0x16, 0x12, 0x06,
    0x87, 0x01, 0x00, 0x93, 0x01, 0x01, 0x0a, 0x0b, 0x0a, 0x03, 0x04, 0x16, 0x01, 0x12, 0x04, 0x87,
    0x01, 0x08, 0x0f, 0x0a, 0x0c, 0x0a, 0x04, 0x04, 0x16, 0x02, 0x00, 0x12, 0x04, 0x88, 0x01, 0x04,
    0x37, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x16, 0x02, 0x00, 0x04, 0x12, 0x04, 0x88, 0x01, 0x04, 0x0c,
    0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x16, 0x02, 0x00, 0x06, 0x12, 0x04, 0x88, 0x01, 0x0d, 0x1a, 0x0a,
    0x0d, 0x0a, 0x05, 0x04, 0x16, 0x02, 0x00, 0x01, 0x12, 0x04, 0x88, 0x01, 0x1b, 0x21, 0x0a, 0x0d,
    0x0a, 0x05, 0x04, 0x16, 0x02, 0x00, 0x03, 0x12, 0x04, 0x88, 0x01, 0x35, 0x36, 0x0a, 0x0c, 0x0a,
    0x04, 0x04, 0x16, 0x02, 0x01, 0x12, 0x04, 0x89, 0x01, 0x04, 0x37, 0x0a, 0x0d, 0x0a, 0x05, 0x04,
    0x16, 0x02, 0x01, 0x04, 0x12, 0x04, 0x89, 0x01, 0x04, 0x0c, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x16,
    0x02, 0x01, 0x06, 0x12, 0x04, 0x89, 0x01, 0x0d, 0x18, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x16, 0x02,
    0x01, 0x01, 0x12, 0x04, 0x89, 0x01, 0x19, 0x21, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x16, 0x02, 0x01,
    0x03, 0x12, 0x04, 0x89, 0x01, 0x35, 0x36, 0x0a, 0x0c, 0x0a, 0x04, 0x04, 0x16, 0x02, 0x02, 0x12,
    0x04, 0x8a, 0x01, 0x04, 0x37, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x16, 0x02, 0x02, 0x04, 0x12, 0x04,
    0x8a, 0x01, 0x04, 0x0c, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x16, 0x02, 0x02, 0x06, 0x12, 0x04, 0x8a,
    0x01, 0x0d, 0x17, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x16, 0x02, 0x02, 0x01, 0x12, 0x04, 0x8a, 0x01,
    0x18, 0x1b, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x16, 0x02, 0x02, 0x03, 0x12, 0x04, 0x8a, 0x01, 0x35,
    0x36, 0x0a, 0x0c, 0x0a, 0x04, 0x04, 0x16, 0x02, 0x03, 0x12, 0x04, 0x8b, 0x01, 0x04, 0x37, 0x0a,
    0x0d, 0x0a, 0x05, 0x04, 0x16, 0x02, 0x03, 0x04, 0x12, 0x04, 0x8b, 0x01, 0x04, 0x0c, 0x0a, 0x0d,
    0x0a, 0x05, 0x04, 0x16, 0x02, 0x03, 0x06, 0x12, 0x04, 0x8b, 0x01, 0x0d, 0x1d, 0x0a, 0x0d, 0x0a,
    0x05, 0x04, 0x16, 0x02, 0x03, 0x01, 0x12, 0x04, 0x8b, 0x01, 0x1e, 0x27, 0x0a, 0x0d, 0x0a, 0x05,
    0x04, 0x16, 0x02, 0x03, 0x03, 0x12, 0x04, 0x8b, 0x01, 0x35, 0x36, 0x0a, 0x0c, 0x0a, 0x04, 0x04,
    0x16, 0x02, 0x04, 0x12, 0x04, 0x8c, 0x01, 0x04, 0x37, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x16, 0x02,
    0x04, 0x04, 0x12, 0x04, 0x8c, 0x01, 0x04, 0x0c, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x16, 0x02, 0x04,
    0x06, 0x12, 0x04, 0x8c, 0x01, 0x0d, 0x22, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x16, 0x02, 0x04, 0x01,
    0x12, 0x04, 0x8c, 0x01, 0x23, 0x32, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x16, 0x02, 0x04, 0x03, 0x12,
    0x04, 0x8c, 0x01, 0x35, 0x36, 0x0a, 0x0c, 0x0a, 0x04, 0x04, 0x16, 0x02, 0x05, 0x12, 0x04, 0x8d,
    0x01, 0x04, 0x37, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x16, 0x02, 0x05, 0x04, 0x12, 0x04, 0x8d, 0x01,
    0x04, 0x0c, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x16, 0x02, 0x05, 0x06, 0x12, 0x04, 0x8d, 0x01, 0x0d,
    0x1b, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x16, 0x02, 0x05, 0x01, 0x12, 0x04, 0x8d, 0x01, 0x1c, 0x24,
    0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x16, 0x02, 0x05, 0x03, 0x12, 0x04, 0x8d, 0x01, 0x35, 0x36, 0x0a,
    0x0c, 0x0a, 0x04, 0x04, 0x16, 0x02, 0x06, 0x12, 0x04, 0x8e, 0x01, 0x04, 0x37, 0x0a, 0x0d, 0x0a,
    0x05, 0x04, 0x16, 0x02, 0x06, 0x04, 0x12, 0x04, 0x8e, 0x01, 0x04, 0x0c, 0x0a, 0x0d, 0x0a, 0x05,
    0x04, 0x16, 0x02, 0x06, 0x06, 0x12, 0x04, 0x8e, 0x01, 0x0d, 0x1b, 0x0a, 0x0d, 0x0a, 0x05, 0x04,
    0x16, 0x02, 0x06, 0x01, 0x12, 0x04, 0x8e, 0x01, 0x1c, 0x24, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x16,
    0x02, 0x06, 0x03, 0x12, 0x04, 0x8e, 0x01, 0x35, 0x36, 0x0a, 0x0c, 0x0a, 0x04, 0x04, 0x16, 0x02,
    0x07, 0x12, 0x04, 0x8f, 0x01, 0x04, 0x37, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x16, 0x02, 0x07, 0x04,
    0x12, 0x04, 0x8f, 0x01, 0x04, 0x0c, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x16, 0x02, 0x07, 0x06, 0x12,
    0x04, 0x8f, 0x01, 0x0d, 0x1b, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x16, 0x02, 0x07, 0x01, 0x12, 0x04,
    0x8f, 0x01, 0x1c, 0x24, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x16, 0x02, 0x07, 0x03, 0x12, 0x04, 0x8f,
    0x01, 0x35, 0x36, 0x0a, 0x0c, 0x0a, 0x04, 0x04, 0x16, 0x02, 0x08, 0x12, 0x04, 0x90, 0x01, 0x04,
    0x37, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x16, 0x02, 0x08, 0x04, 0x12, 0x04, 0x90, 0x01, 0x04, 0x0c,
    0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x16, 0x02, 0x08, 0x06, 0x12, 0x04, 0x90, 0x01, 0x0d, 0x1e, 0x0a,
    0x0d, 0x0a, 0x05, 0x04, 0x16, 0x02, 0x08, 0x01, 0x12, 0x04, 0x90, 0x01, 0x1f, 0x2a, 0x0a, 0x0d,
    0x0a, 0x05, 0x04, 0x16, 0x02, 0x08, 0x03, 0x12, 0x04, 0x90, 0x01, 0x35, 0x36, 0x0a, 0x0c, 0x0a,
    0x04, 0x04, 0x16, 0x02, 0x09, 0x12, 0x04, 0x91, 0x01, 0x04, 0x38, 0x0a, 0x0d, 0x0a, 0x05, 0x04,
    0x16, 0x02, 0x09, 0x04, 0x12, 0x04, 0x91, 0x01, 0x04, 0x0c, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x16,
    0x02, 0x09, 0x06, 0x12, 0x04, 0x91, 0x01, 0x0d, 0x21, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x16, 0x02,
    0x09, 0x01, 0x12, 0x04, 0x91, 0x01, 0x22, 0x31, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x16, 0x02, 0x09,
    0x03, 0x12, 0x04, 0x91, 0x01, 0x35, 0x37, 0x0a, 0x0c, 0x0a, 0x04, 0x04, 0x16, 0x02, 0x0a, 0x12,
    0x04, 0x92, 0x01, 0x04, 0x38, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x16, 0x02, 0x0a, 0x04, 0x12, 0x04,
    0x92, 0x01, 0x04, 0x0c, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x16, 0x02, 0x0a, 0x06, 0x12, 0x04, 0x92,
    0x01, 0x0d, 0x1c, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x16, 0x02, 0x0a, 0x01, 0x12, 0x04, 0x92, 0x01,
    0x1d, 0x26, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x16, 0x02, 0x0a, 0x03, 0x12, 0x04, 0x92, 0x01, 0x35,
    0x37, 0x0a, 0x0c, 0x0a, 0x02, 0x04, 0x17, 0x12, 0x06, 0x95, 0x01, 0x00, 0xa1, 0x01, 0x01, 0x0a,
    0x0b, 0x0a, 0x03, 0x04, 0x17, 0x01, 0x12, 0x04, 0x95, 0x01, 0x08, 0x10, 0x0a, 0x0c, 0x0a, 0x04,
    0x04, 0x17, 0x02, 0x00, 0x12, 0x04, 0x96, 0x01, 0x04, 0x38, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x17,
    0x02, 0x00, 0x04, 0x12, 0x04, 0x96, 0x01, 0x04, 0x0c, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x17, 0x02,
    0x00, 0x06, 0x12, 0x04, 0x96, 0x01, 0x0d, 0x1b, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x17, 0x02, 0x00,
    0x01, 0x12, 0x04, 0x96, 0x01, 0x1c, 0x22, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x17, 0x02, 0x00, 0x03,
    0x12, 0x04, 0x96, 0x01, 0x36, 0x37, 0x0a, 0x0c, 0x0a, 0x04, 0x04, 0x17, 0x02, 0x01, 0x12, 0x04,
    0x97, 0x01, 0x04, 0x38, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x17, 0x02, 0x01, 0x04, 0x12, 0x04, 0x97,
    0x01, 0x04, 0x0c, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x17, 0x02, 0x01, 0x06, 0x12, 0x04, 0x97, 0x01,
    0x0d, 0x18, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x17, 0x02, 0x01, 0x01, 0x12, 0x04, 0x97, 0x01, 0x19,
    0x21, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x17, 0x02, 0x01, 0x03, 0x12, 0x04, 0x97, 0x01, 0x36, 0x37,
    0x0a, 0x0c, 0x0a, 0x04, 0x04, 0x17, 0x02, 0x02, 0x12, 0x04, 0x98, 0x01, 0x04, 0x38, 0x0a, 0x0d,
    0x0a, 0x05, 0x04, 0x17, 0x02, 0x02, 0x04, 0x12, 0x04, 0x98, 0x01, 0x04, 0x0c, 0x0a, 0x0d, 0x0a,
    0x05, 0x04, 0x17, 0x02, 0x02, 0x06, 0x12, 0x04, 0x98, 0x01, 0x0d, 0x18, 0x0a, 0x0d, 0x0a, 0x05,
    0x04, 0x17, 0x02, 0x02, 0x01, 0x12, 0x04, 0x98, 0x01, 0x19, 0x1c, 0x0a, 0x0d, 0x0a, 0x05, 0x04,
    0x17, 0x02, 0x02, 0x03, 0x12, 0x04, 0x98, 0x01, 0x36, 0x37, 0x0a, 0x0c, 0x0a, 0x04, 0x04, 0x17,
    0x02, 0x03, 0x12, 0x04, 0x99, 0x01, 0x04, 0x38, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x17, 0x02, 0x03,
    0x04, 0x12, 0x04, 0x99, 0x01, 0x04, 0x0c, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x17, 0x02, 0x03, 0x06,
    0x12, 0x04, 0x99, 0x01, 0x0d, 0x1e, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x17, 0x02, 0x03, 0x01, 0x12,
    0x04, 0x99, 0x01, 0x1f, 0x28, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x17, 0x02, 0x03, 0x03, 0x12, 0x04,
    0x99, 0x01, 0x36, 0x37, 0x0a, 0x0c, 0x0a, 0x04, 0x04, 0x17, 0x02, 0x04, 0x12, 0x04, 0x9a, 0x01,
    0x04, 0x38, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x17, 0x02, 0x04, 0x04, 0x12, 0x04, 0x9a, 0x01, 0x04,
    0x0c, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x17, 0x02, 0x04, 0x06, 0x12, 0x04, 0x9a, 0x01, 0x0d, 0x23,
    0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x17, 0x02, 0x04, 0x01, 0x12, 0x04, 0x9a, 0x01, 0x24, 0x33, 0x0a,
    0x0d, 0x0a, 0x05, 0x04, 0x17, 0x02, 0x04, 0x03, 0x12, 0x04, 0x9a, 0x01, 0x36, 0x37, 0x0a, 0x0c,
    0x0a, 0x04, 0x04, 0x17, 0x02, 0x05, 0x12, 0x04, 0x9b, 0x01, 0x04, 0x38, 0x0a, 0x0d, 0x0a, 0x05,
    0x04, 0x17, 0x02, 0x05, 0x04, 0x12, 0x04, 0x9b, 0x01, 0x04, 0x0c, 0x0a, 0x0d, 0x0a, 0x05, 0x04,
    0x17, 0x02, 0x05, 0x06, 0x12, 0x04, 0x9b, 0x01, 0x0d, 0x1c, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x17,
    0x02, 0x05, 0x01, 0x12, 0x04, 0x9b, 0x01, 0x1d, 0x25, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x17, 0x02,
    0x05, 0x03, 0x12, 0x04, 0x9b, 0x01, 0x36, 0x37, 0x0a, 0x0c, 0x0a, 0x04, 0x04, 0x17, 0x02, 0x06,
    0x12, 0x04, 0x9c, 0x01, 0x04, 0x38, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x17, 0x02, 0x06, 0x04, 0x12,
    0x04, 0x9c, 0x01, 0x04, 0x0c, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x17, 0x02, 0x06, 0x06, 0x12, 0x04,
    0x9c, 0x01, 0x0d, 0x1c, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x17, 0x02, 0x06, 0x01, 0x12, 0x04, 0x9c,
    0x01, 0x1d, 0x25, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x17, 0x02, 0x06, 0x03, 0x12, 0x04, 0x9c, 0x01,
    0x36, 0x37, 0x0a, 0x0c, 0x0a, 0x04, 0x04, 0x17, 0x02, 0x07, 0x12, 0x04, 0x9d, 0x01, 0x04, 0x38,
    0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x17, 0x02, 0x07, 0x04, 0x12, 0x04, 0x9d, 0x01, 0x04, 0x0c, 0x0a,
    0x0d, 0x0a, 0x05, 0x04, 0x17, 0x02, 0x07, 0x06, 0x12, 0x04, 0x9d, 0x01, 0x0d, 0x1c, 0x0a, 0x0d,
    0x0a, 0x05, 0x04, 0x17, 0x02, 0x07, 0x01, 0x12, 0x04, 0x9d, 0x01, 0x1d, 0x25, 0x0a, 0x0d, 0x0a,
    0x05, 0x04, 0x17, 0x02, 0x07, 0x03, 0x12, 0x04, 0x9d, 0x01, 0x36, 0x37, 0x0a, 0x0c, 0x0a, 0x04,
    0x04, 0x17, 0x02, 0x08, 0x12, 0x04, 0x9e, 0x01, 0x04, 0x38, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x17,
    0x02, 0x08, 0x04, 0x12, 0x04, 0x9e, 0x01, 0x04, 0x0c, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x17, 0x02,
    0x08, 0x06, 0x12, 0x04, 0x9e, 0x01, 0x0d, 0x1f, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x17, 0x02, 0x08,
    0x01, 0x12, 0x04, 0x9e, 0x01, 0x20, 0x2b, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x17, 0x02, 0x08, 0x03,
    0x12, 0x04, 0x9e, 0x01, 0x36, 0x37, 0x0a, 0x0c, 0x0a, 0x04, 0x04, 0x17, 0x02, 0x09, 0x12, 0x04,
    0x9f, 0x01, 0x04, 0x39, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x17, 0x02, 0x09, 0x04, 0x12, 0x04, 0x9f,
    0x01, 0x04, 0x0c, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x17, 0x02, 0x09, 0x06, 0x12, 0x04, 0x9f, 0x01,
    0x0d, 0x22, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x17, 0x02, 0x09, 0x01, 0x12, 0x04, 0x9f, 0x01, 0x23,
    0x32, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x17, 0x02, 0x09, 0x03, 0x12, 0x04, 0x9f, 0x01, 0x36, 0x38,
    0x0a, 0x0c, 0x0a, 0x04, 0x04, 0x17, 0x02, 0x0a, 0x12, 0x04, 0xa0, 0x01, 0x04, 0x39, 0x0a, 0x0d,
    0x0a, 0x05, 0x04, 0x17, 0x02, 0x0a, 0x04, 0x12, 0x04, 0xa0, 0x01, 0x04, 0x0c, 0x0a, 0x0d, 0x0a,
    0x05, 0x04, 0x17, 0x02, 0x0a, 0x06, 0x12, 0x04, 0xa0, 0x01, 0x0d, 0x1d, 0x0a, 0x0d, 0x0a, 0x05,
    0x04, 0x17, 0x02, 0x0a, 0x01, 0x12, 0x04, 0xa0, 0x01, 0x1e, 0x27, 0x0a, 0x0d, 0x0a, 0x05, 0x04,
    0x17, 0x02, 0x0a, 0x03, 0x12, 0x04, 0xa0, 0x01, 0x36, 0x38, 0x0a, 0x0c, 0x0a, 0x02, 0x04, 0x18,
    0x12, 0x06, 0xa3, 0x01, 0x00, 0xa5, 0x01, 0x01, 0x0a, 0x0b, 0x0a, 0x03, 0x04, 0x18, 0x01, 0x12,
    0x04, 0xa3, 0x01, 0x08, 0x19, 0x0a, 0x0c, 0x0a, 0x02, 0x04, 0x19, 0x12, 0x06, 0xa7, 0x01, 0x00,
    0xaa, 0x01, 0x01, 0x0a, 0x0b, 0x0a, 0x03, 0x04, 0x19, 0x01, 0x12, 0x04, 0xa7, 0x01, 0x08, 0x0d,
    0x0a, 0x0c, 0x0a, 0x04, 0x04, 0x19, 0x02, 0x00, 0x12, 0x04, 0xa8, 0x01, 0x04, 0x30, 0x0a, 0x0d,
    0x0a, 0x05, 0x04, 0x19, 0x02, 0x00, 0x04, 0x12, 0x04, 0xa8, 0x01, 0x04, 0x0c, 0x0a, 0x0d, 0x0a,
    0x05, 0x04, 0x19, 0x02, 0x00, 0x05, 0x12, 0x04, 0xa8, 0x01, 0x0d, 0x13, 0x0a, 0x0d, 0x0a, 0x05,
    0x04, 0x19, 0x02, 0x00, 0x01, 0x12, 0x04, 0xa8, 0x01, 0x14, 0x1b, 0x0a, 0x0d, 0x0a, 0x05, 0x04,
    0x19, 0x02, 0x00, 0x03, 0x12, 0x04, 0xa8, 0x01, 0x2e, 0x2f, 0x0a, 0x0c, 0x0a, 0x04, 0x04, 0x19,
    0x02, 0x01, 0x12, 0x04, 0xa9, 0x01, 0x04, 0x30, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x19, 0x02, 0x01,
    0x04, 0x12, 0x04, 0xa9, 0x01, 0x04, 0x0c, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x19, 0x02, 0x01, 0x06,
    0x12, 0x04, 0xa9, 0x01, 0x0d, 0x1e, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x19, 0x02, 0x01, 0x01, 0x12,
    0x04, 0xa9, 0x01, 0x1f, 0x2b, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x19, 0x02, 0x01, 0x03, 0x12, 0x04,
    0xa9, 0x01, 0x2e, 0x2f,
];

static mut file_descriptor_proto_lazy: ::protobuf::lazy::Lazy<::protobuf::descriptor::FileDescriptorProto> = ::protobuf::lazy::Lazy {
    lock: ::protobuf::lazy::ONCE_INIT,
    ptr: 0 as *const ::protobuf::descriptor::FileDescriptorProto,
};

fn parse_descriptor_proto() -> ::protobuf::descriptor::FileDescriptorProto {
    ::protobuf::parse_from_bytes(file_descriptor_proto_data).unwrap()
}

pub fn file_descriptor_proto() -> &'static ::protobuf::descriptor::FileDescriptorProto {
    unsafe {
        file_descriptor_proto_lazy.get(|| {
            parse_descriptor_proto()
        })
    }
}
