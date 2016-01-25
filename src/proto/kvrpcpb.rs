// This file is generated. Do not edit
// @generated

#![allow(dead_code)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(unused_imports)]

use protobuf::Message as Message_imported_for_functions;
use protobuf::ProtobufEnum as ProtobufEnum_imported_for_functions;

#[derive(Clone,Default)]
pub struct CmdGetRequest {
    // message fields
    version: ::std::option::Option<u64>,
    key: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

impl CmdGetRequest {
    pub fn new() -> CmdGetRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static CmdGetRequest {
        static mut instance: ::protobuf::lazy::Lazy<CmdGetRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const CmdGetRequest,
        };
        unsafe {
            instance.get(|| {
                CmdGetRequest {
                    version: ::std::option::Option::None,
                    key: ::protobuf::SingularField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // required uint64 version = 1;

    pub fn clear_version(&mut self) {
        self.version = ::std::option::Option::None;
    }

    pub fn has_version(&self) -> bool {
        self.version.is_some()
    }

    // Param is passed by value, moved
    pub fn set_version(&mut self, v: u64) {
        self.version = ::std::option::Option::Some(v);
    }

    pub fn get_version<'a>(&self) -> u64 {
        self.version.unwrap_or(0)
    }

    // required bytes key = 2;

    pub fn clear_key(&mut self) {
        self.key.clear();
    }

    pub fn has_key(&self) -> bool {
        self.key.is_some()
    }

    // Param is passed by value, moved
    pub fn set_key(&mut self, v: ::std::vec::Vec<u8>) {
        self.key = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_key<'a>(&'a mut self) -> &'a mut ::std::vec::Vec<u8> {
        if self.key.is_none() {
            self.key.set_default();
        };
        self.key.as_mut().unwrap()
    }

    // Take field
    pub fn take_key(&mut self) -> ::std::vec::Vec<u8> {
        self.key.take().unwrap_or_else(|| ::std::vec::Vec::new())
    }

    pub fn get_key<'a>(&'a self) -> &'a [u8] {
        match self.key.as_ref() {
            Some(v) => &v,
            None => &[],
        }
    }
}

impl ::protobuf::Message for CmdGetRequest {
    fn is_initialized(&self) -> bool {
        if self.version.is_none() {
            return false;
        };
        if self.key.is_none() {
            return false;
        };
        true
    }

    fn merge_from(&mut self,
                  is: &mut ::protobuf::CodedInputStream)
                  -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::ProtobufError::WireError("unexpected wire type".to_string()));
                    };
                    let tmp = try!(is.read_uint64());
                    self.version = ::std::option::Option::Some(tmp);
                }
                2 => {
                    try!(::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.key));
                }
                _ => {
                    let unknown = try!(is.read_unknown(wire_type));
                    self.mut_unknown_fields().add_value(field_number, unknown);
                }
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in self.version.iter() {
            my_size += ::protobuf::rt::value_size(1,
                                                  *value,
                                                  ::protobuf::wire_format::WireTypeVarint);
        }
        for value in self.key.iter() {
            my_size += ::protobuf::rt::bytes_size(2, &value);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self,
                                  os: &mut ::protobuf::CodedOutputStream)
                                  -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.version {
            try!(os.write_uint64(1, v));
        };
        if let Some(v) = self.key.as_ref() {
            try!(os.write_bytes(2, &v));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields<'s>(&'s self) -> &'s ::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields<'s>(&'s mut self) -> &'s mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<CmdGetRequest>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for CmdGetRequest {
    fn new() -> CmdGetRequest {
        CmdGetRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<CmdGetRequest>)
                         -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> =
            ::protobuf::lazy::Lazy {
                lock: ::protobuf::lazy::ONCE_INIT,
                ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
            };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_u64_accessor(
                    "version",
                    CmdGetRequest::has_version,
                    CmdGetRequest::get_version,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_bytes_accessor(
                    "key",
                    CmdGetRequest::has_key,
                    CmdGetRequest::get_key,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<CmdGetRequest>(
                    "CmdGetRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for CmdGetRequest {
    fn clear(&mut self) {
        self.clear_version();
        self.clear_key();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for CmdGetRequest {
    fn eq(&self, other: &CmdGetRequest) -> bool {
        self.version == other.version && self.key == other.key &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for CmdGetRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct CmdGetResponse {
    // message fields
    ok: ::std::option::Option<bool>,
    value: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

impl CmdGetResponse {
    pub fn new() -> CmdGetResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static CmdGetResponse {
        static mut instance: ::protobuf::lazy::Lazy<CmdGetResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const CmdGetResponse,
        };
        unsafe {
            instance.get(|| {
                CmdGetResponse {
                    ok: ::std::option::Option::None,
                    value: ::protobuf::SingularField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // required bool ok = 1;

    pub fn clear_ok(&mut self) {
        self.ok = ::std::option::Option::None;
    }

    pub fn has_ok(&self) -> bool {
        self.ok.is_some()
    }

    // Param is passed by value, moved
    pub fn set_ok(&mut self, v: bool) {
        self.ok = ::std::option::Option::Some(v);
    }

    pub fn get_ok<'a>(&self) -> bool {
        self.ok.unwrap_or(false)
    }

    // required bytes value = 2;

    pub fn clear_value(&mut self) {
        self.value.clear();
    }

    pub fn has_value(&self) -> bool {
        self.value.is_some()
    }

    // Param is passed by value, moved
    pub fn set_value(&mut self, v: ::std::vec::Vec<u8>) {
        self.value = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_value<'a>(&'a mut self) -> &'a mut ::std::vec::Vec<u8> {
        if self.value.is_none() {
            self.value.set_default();
        };
        self.value.as_mut().unwrap()
    }

    // Take field
    pub fn take_value(&mut self) -> ::std::vec::Vec<u8> {
        self.value.take().unwrap_or_else(|| ::std::vec::Vec::new())
    }

    pub fn get_value<'a>(&'a self) -> &'a [u8] {
        match self.value.as_ref() {
            Some(v) => &v,
            None => &[],
        }
    }
}

impl ::protobuf::Message for CmdGetResponse {
    fn is_initialized(&self) -> bool {
        if self.ok.is_none() {
            return false;
        };
        if self.value.is_none() {
            return false;
        };
        true
    }

    fn merge_from(&mut self,
                  is: &mut ::protobuf::CodedInputStream)
                  -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::ProtobufError::WireError("unexpected wire type".to_string()));
                    };
                    let tmp = try!(is.read_bool());
                    self.ok = ::std::option::Option::Some(tmp);
                }
                2 => {
                    try!(::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.value));
                }
                _ => {
                    let unknown = try!(is.read_unknown(wire_type));
                    self.mut_unknown_fields().add_value(field_number, unknown);
                }
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        if self.ok.is_some() {
            my_size += 2;
        };
        for value in self.value.iter() {
            my_size += ::protobuf::rt::bytes_size(2, &value);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self,
                                  os: &mut ::protobuf::CodedOutputStream)
                                  -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.ok {
            try!(os.write_bool(1, v));
        };
        if let Some(v) = self.value.as_ref() {
            try!(os.write_bytes(2, &v));
        };
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields<'s>(&'s self) -> &'s ::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields<'s>(&'s mut self) -> &'s mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<CmdGetResponse>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for CmdGetResponse {
    fn new() -> CmdGetResponse {
        CmdGetResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<CmdGetResponse>)
                         -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> =
            ::protobuf::lazy::Lazy {
                lock: ::protobuf::lazy::ONCE_INIT,
                ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
            };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_bool_accessor(
                    "ok",
                    CmdGetResponse::has_ok,
                    CmdGetResponse::get_ok,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_bytes_accessor(
                    "value",
                    CmdGetResponse::has_value,
                    CmdGetResponse::get_value,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<CmdGetResponse>(
                    "CmdGetResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for CmdGetResponse {
    fn clear(&mut self) {
        self.clear_ok();
        self.clear_value();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for CmdGetResponse {
    fn eq(&self, other: &CmdGetResponse) -> bool {
        self.ok == other.ok && self.value == other.value &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for CmdGetResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct CmdScanRequest {
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

impl CmdScanRequest {
    pub fn new() -> CmdScanRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static CmdScanRequest {
        static mut instance: ::protobuf::lazy::Lazy<CmdScanRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const CmdScanRequest,
        };
        unsafe {
            instance.get(|| {
                CmdScanRequest {
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }
}

impl ::protobuf::Message for CmdScanRequest {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self,
                  is: &mut ::protobuf::CodedInputStream)
                  -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                _ => {
                    let unknown = try!(is.read_unknown(wire_type));
                    self.mut_unknown_fields().add_value(field_number, unknown);
                }
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

    fn write_to_with_cached_sizes(&self,
                                  os: &mut ::protobuf::CodedOutputStream)
                                  -> ::protobuf::ProtobufResult<()> {
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields<'s>(&'s self) -> &'s ::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields<'s>(&'s mut self) -> &'s mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<CmdScanRequest>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for CmdScanRequest {
    fn new() -> CmdScanRequest {
        CmdScanRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<CmdScanRequest>)
                         -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> =
            ::protobuf::lazy::Lazy {
                lock: ::protobuf::lazy::ONCE_INIT,
                ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
            };
        unsafe {
            descriptor.get(|| {
                let fields = ::std::vec::Vec::new();
                ::protobuf::reflect::MessageDescriptor::new::<CmdScanRequest>(
                    "CmdScanRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for CmdScanRequest {
    fn clear(&mut self) {
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for CmdScanRequest {
    fn eq(&self, other: &CmdScanRequest) -> bool {
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for CmdScanRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct CmdScanResponse {
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

impl CmdScanResponse {
    pub fn new() -> CmdScanResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static CmdScanResponse {
        static mut instance: ::protobuf::lazy::Lazy<CmdScanResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const CmdScanResponse,
        };
        unsafe {
            instance.get(|| {
                CmdScanResponse {
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }
}

impl ::protobuf::Message for CmdScanResponse {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self,
                  is: &mut ::protobuf::CodedInputStream)
                  -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                _ => {
                    let unknown = try!(is.read_unknown(wire_type));
                    self.mut_unknown_fields().add_value(field_number, unknown);
                }
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

    fn write_to_with_cached_sizes(&self,
                                  os: &mut ::protobuf::CodedOutputStream)
                                  -> ::protobuf::ProtobufResult<()> {
        try!(os.write_unknown_fields(self.get_unknown_fields()));
        ::std::result::Result::Ok(())
    }

    fn get_cached_size(&self) -> u32 {
        self.cached_size.get()
    }

    fn get_unknown_fields<'s>(&'s self) -> &'s ::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields<'s>(&'s mut self) -> &'s mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<CmdScanResponse>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for CmdScanResponse {
    fn new() -> CmdScanResponse {
        CmdScanResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<CmdScanResponse>)
                         -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> =
            ::protobuf::lazy::Lazy {
                lock: ::protobuf::lazy::ONCE_INIT,
                ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
            };
        unsafe {
            descriptor.get(|| {
                let fields = ::std::vec::Vec::new();
                ::protobuf::reflect::MessageDescriptor::new::<CmdScanResponse>(
                    "CmdScanResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for CmdScanResponse {
    fn clear(&mut self) {
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for CmdScanResponse {
    fn eq(&self, other: &CmdScanResponse) -> bool {
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for CmdScanResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct Request {
    // message fields
    field_type: ::std::option::Option<MessageType>,
    cmd_get_req: ::protobuf::SingularPtrField<CmdGetRequest>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

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
                    field_type: ::std::option::Option::None,
                    cmd_get_req: ::protobuf::SingularPtrField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // required .kvrpcpb.MessageType type = 1;

    pub fn clear_field_type(&mut self) {
        self.field_type = ::std::option::Option::None;
    }

    pub fn has_field_type(&self) -> bool {
        self.field_type.is_some()
    }

    // Param is passed by value, moved
    pub fn set_field_type(&mut self, v: MessageType) {
        self.field_type = ::std::option::Option::Some(v);
    }

    pub fn get_field_type<'a>(&self) -> MessageType {
        self.field_type.unwrap_or(MessageType::CmdGet)
    }

    // optional .kvrpcpb.CmdGetRequest cmd_get_req = 2;

    pub fn clear_cmd_get_req(&mut self) {
        self.cmd_get_req.clear();
    }

    pub fn has_cmd_get_req(&self) -> bool {
        self.cmd_get_req.is_some()
    }

    // Param is passed by value, moved
    pub fn set_cmd_get_req(&mut self, v: CmdGetRequest) {
        self.cmd_get_req = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_cmd_get_req<'a>(&'a mut self) -> &'a mut CmdGetRequest {
        if self.cmd_get_req.is_none() {
            self.cmd_get_req.set_default();
        };
        self.cmd_get_req.as_mut().unwrap()
    }

    // Take field
    pub fn take_cmd_get_req(&mut self) -> CmdGetRequest {
        self.cmd_get_req.take().unwrap_or_else(|| CmdGetRequest::new())
    }

    pub fn get_cmd_get_req<'a>(&'a self) -> &'a CmdGetRequest {
        self.cmd_get_req.as_ref().unwrap_or_else(|| CmdGetRequest::default_instance())
    }
}

impl ::protobuf::Message for Request {
    fn is_initialized(&self) -> bool {
        if self.field_type.is_none() {
            return false;
        };
        true
    }

    fn merge_from(&mut self,
                  is: &mut ::protobuf::CodedInputStream)
                  -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::ProtobufError::WireError("unexpected wire type".to_string()));
                    };
                    let tmp = try!(is.read_enum());
                    self.field_type = ::std::option::Option::Some(tmp);
                }
                2 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type,
                                                                    is,
                                                                    &mut self.cmd_get_req));
                }
                _ => {
                    let unknown = try!(is.read_unknown(wire_type));
                    self.mut_unknown_fields().add_value(field_number, unknown);
                }
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in self.field_type.iter() {
            my_size += ::protobuf::rt::enum_size(1, *value);
        }
        for value in self.cmd_get_req.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self,
                                  os: &mut ::protobuf::CodedOutputStream)
                                  -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.field_type {
            try!(os.write_enum(1, v as i32));
        };
        if let Some(v) = self.cmd_get_req.as_ref() {
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

    fn get_unknown_fields<'s>(&'s self) -> &'s ::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields<'s>(&'s mut self) -> &'s mut ::protobuf::UnknownFields {
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

    fn descriptor_static(_: ::std::option::Option<Request>)
                         -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> =
            ::protobuf::lazy::Lazy {
                lock: ::protobuf::lazy::ONCE_INIT,
                ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
            };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_enum_accessor(
                    "type",
                    Request::has_field_type,
                    Request::get_field_type,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "cmd_get_req",
                    Request::has_cmd_get_req,
                    Request::get_cmd_get_req,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Request>("Request",
                                                                       fields,
                                                                       file_descriptor_proto())
            })
        }
    }
}

impl ::protobuf::Clear for Request {
    fn clear(&mut self) {
        self.clear_field_type();
        self.clear_cmd_get_req();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for Request {
    fn eq(&self, other: &Request) -> bool {
        self.field_type == other.field_type && self.cmd_get_req == other.cmd_get_req &&
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
    field_type: ::std::option::Option<MessageType>,
    cmd_get_resp: ::protobuf::SingularPtrField<CmdGetResponse>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

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
                    field_type: ::std::option::Option::None,
                    cmd_get_resp: ::protobuf::SingularPtrField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // required .kvrpcpb.MessageType type = 1;

    pub fn clear_field_type(&mut self) {
        self.field_type = ::std::option::Option::None;
    }

    pub fn has_field_type(&self) -> bool {
        self.field_type.is_some()
    }

    // Param is passed by value, moved
    pub fn set_field_type(&mut self, v: MessageType) {
        self.field_type = ::std::option::Option::Some(v);
    }

    pub fn get_field_type<'a>(&self) -> MessageType {
        self.field_type.unwrap_or(MessageType::CmdGet)
    }

    // optional .kvrpcpb.CmdGetResponse cmd_get_resp = 2;

    pub fn clear_cmd_get_resp(&mut self) {
        self.cmd_get_resp.clear();
    }

    pub fn has_cmd_get_resp(&self) -> bool {
        self.cmd_get_resp.is_some()
    }

    // Param is passed by value, moved
    pub fn set_cmd_get_resp(&mut self, v: CmdGetResponse) {
        self.cmd_get_resp = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_cmd_get_resp<'a>(&'a mut self) -> &'a mut CmdGetResponse {
        if self.cmd_get_resp.is_none() {
            self.cmd_get_resp.set_default();
        };
        self.cmd_get_resp.as_mut().unwrap()
    }

    // Take field
    pub fn take_cmd_get_resp(&mut self) -> CmdGetResponse {
        self.cmd_get_resp.take().unwrap_or_else(|| CmdGetResponse::new())
    }

    pub fn get_cmd_get_resp<'a>(&'a self) -> &'a CmdGetResponse {
        self.cmd_get_resp.as_ref().unwrap_or_else(|| CmdGetResponse::default_instance())
    }
}

impl ::protobuf::Message for Response {
    fn is_initialized(&self) -> bool {
        if self.field_type.is_none() {
            return false;
        };
        true
    }

    fn merge_from(&mut self,
                  is: &mut ::protobuf::CodedInputStream)
                  -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::ProtobufError::WireError("unexpected wire type".to_string()));
                    };
                    let tmp = try!(is.read_enum());
                    self.field_type = ::std::option::Option::Some(tmp);
                }
                2 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type,
                                                                    is,
                                                                    &mut self.cmd_get_resp));
                }
                _ => {
                    let unknown = try!(is.read_unknown(wire_type));
                    self.mut_unknown_fields().add_value(field_number, unknown);
                }
            };
        }
        ::std::result::Result::Ok(())
    }

    // Compute sizes of nested messages
    #[allow(unused_variables)]
    fn compute_size(&self) -> u32 {
        let mut my_size = 0;
        for value in self.field_type.iter() {
            my_size += ::protobuf::rt::enum_size(1, *value);
        }
        for value in self.cmd_get_resp.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self,
                                  os: &mut ::protobuf::CodedOutputStream)
                                  -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.field_type {
            try!(os.write_enum(1, v as i32));
        };
        if let Some(v) = self.cmd_get_resp.as_ref() {
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

    fn get_unknown_fields<'s>(&'s self) -> &'s ::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields<'s>(&'s mut self) -> &'s mut ::protobuf::UnknownFields {
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

    fn descriptor_static(_: ::std::option::Option<Response>)
                         -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> =
            ::protobuf::lazy::Lazy {
                lock: ::protobuf::lazy::ONCE_INIT,
                ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
            };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_enum_accessor(
                    "type",
                    Response::has_field_type,
                    Response::get_field_type,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "cmd_get_resp",
                    Response::has_cmd_get_resp,
                    Response::get_cmd_get_resp,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Response>("Response",
                                                                        fields,
                                                                        file_descriptor_proto())
            })
        }
    }
}

impl ::protobuf::Clear for Response {
    fn clear(&mut self) {
        self.clear_field_type();
        self.clear_cmd_get_resp();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for Response {
    fn eq(&self, other: &Response) -> bool {
        self.field_type == other.field_type && self.cmd_get_resp == other.cmd_get_resp &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for Response {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,PartialEq,Eq,Debug,Hash)]
pub enum MessageType {
    CmdGet = 1,
    CmdScan = 2,
    CmdPut = 3,
    CmdDel = 4,
    CmdLockKeys = 5,
    CmdCommit = 6,
}

impl ::protobuf::ProtobufEnum for MessageType {
    fn value(&self) -> i32 {
        *self as i32
    }

    fn from_i32(value: i32) -> ::std::option::Option<MessageType> {
        match value {
            1 => ::std::option::Option::Some(MessageType::CmdGet),
            2 => ::std::option::Option::Some(MessageType::CmdScan),
            3 => ::std::option::Option::Some(MessageType::CmdPut),
            4 => ::std::option::Option::Some(MessageType::CmdDel),
            5 => ::std::option::Option::Some(MessageType::CmdLockKeys),
            6 => ::std::option::Option::Some(MessageType::CmdCommit),
            _ => ::std::option::Option::None,
        }
    }

    fn values() -> &'static [Self] {
        static values: &'static [MessageType] = &[MessageType::CmdGet,
                                                  MessageType::CmdScan,
                                                  MessageType::CmdPut,
                                                  MessageType::CmdDel,
                                                  MessageType::CmdLockKeys,
                                                  MessageType::CmdCommit];
        values
    }

    fn enum_descriptor_static(_: Option<MessageType>) -> &'static ::protobuf::reflect::EnumDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::EnumDescriptor> =
            ::protobuf::lazy::Lazy {
                lock: ::protobuf::lazy::ONCE_INIT,
                ptr: 0 as *const ::protobuf::reflect::EnumDescriptor,
            };
        unsafe {
            descriptor.get(|| {
                ::protobuf::reflect::EnumDescriptor::new("MessageType", file_descriptor_proto())
            })
        }
    }
}

impl ::std::marker::Copy for MessageType {}

static file_descriptor_proto_data: &'static [u8] = &[0x0a, 0x0d, 0x6b, 0x76, 0x72, 0x70, 0x63,
                                                     0x70, 0x62, 0x2e, 0x70, 0x72, 0x6f, 0x74,
                                                     0x6f, 0x12, 0x07, 0x6b, 0x76, 0x72, 0x70,
                                                     0x63, 0x70, 0x62, 0x22, 0x2d, 0x0a, 0x0d,
                                                     0x43, 0x6d, 0x64, 0x47, 0x65, 0x74, 0x52,
                                                     0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12,
                                                     0x0f, 0x0a, 0x07, 0x76, 0x65, 0x72, 0x73,
                                                     0x69, 0x6f, 0x6e, 0x18, 0x01, 0x20, 0x02,
                                                     0x28, 0x04, 0x12, 0x0b, 0x0a, 0x03, 0x6b,
                                                     0x65, 0x79, 0x18, 0x02, 0x20, 0x02, 0x28,
                                                     0x0c, 0x22, 0x2b, 0x0a, 0x0e, 0x43, 0x6d,
                                                     0x64, 0x47, 0x65, 0x74, 0x52, 0x65, 0x73,
                                                     0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x0a,
                                                     0x0a, 0x02, 0x6f, 0x6b, 0x18, 0x01, 0x20,
                                                     0x02, 0x28, 0x08, 0x12, 0x0d, 0x0a, 0x05,
                                                     0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02,
                                                     0x20, 0x02, 0x28, 0x0c, 0x22, 0x10, 0x0a,
                                                     0x0e, 0x43, 0x6d, 0x64, 0x53, 0x63, 0x61,
                                                     0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
                                                     0x74, 0x22, 0x11, 0x0a, 0x0f, 0x43, 0x6d,
                                                     0x64, 0x53, 0x63, 0x61, 0x6e, 0x52, 0x65,
                                                     0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22,
                                                     0x5a, 0x0a, 0x07, 0x52, 0x65, 0x71, 0x75,
                                                     0x65, 0x73, 0x74, 0x12, 0x22, 0x0a, 0x04,
                                                     0x74, 0x79, 0x70, 0x65, 0x18, 0x01, 0x20,
                                                     0x02, 0x28, 0x0e, 0x32, 0x14, 0x2e, 0x6b,
                                                     0x76, 0x72, 0x70, 0x63, 0x70, 0x62, 0x2e,
                                                     0x4d, 0x65, 0x73, 0x73, 0x61, 0x67, 0x65,
                                                     0x54, 0x79, 0x70, 0x65, 0x12, 0x2b, 0x0a,
                                                     0x0b, 0x63, 0x6d, 0x64, 0x5f, 0x67, 0x65,
                                                     0x74, 0x5f, 0x72, 0x65, 0x71, 0x18, 0x02,
                                                     0x20, 0x01, 0x28, 0x0b, 0x32, 0x16, 0x2e,
                                                     0x6b, 0x76, 0x72, 0x70, 0x63, 0x70, 0x62,
                                                     0x2e, 0x43, 0x6d, 0x64, 0x47, 0x65, 0x74,
                                                     0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
                                                     0x22, 0x5d, 0x0a, 0x08, 0x52, 0x65, 0x73,
                                                     0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x22,
                                                     0x0a, 0x04, 0x74, 0x79, 0x70, 0x65, 0x18,
                                                     0x01, 0x20, 0x02, 0x28, 0x0e, 0x32, 0x14,
                                                     0x2e, 0x6b, 0x76, 0x72, 0x70, 0x63, 0x70,
                                                     0x62, 0x2e, 0x4d, 0x65, 0x73, 0x73, 0x61,
                                                     0x67, 0x65, 0x54, 0x79, 0x70, 0x65, 0x12,
                                                     0x2d, 0x0a, 0x0c, 0x63, 0x6d, 0x64, 0x5f,
                                                     0x67, 0x65, 0x74, 0x5f, 0x72, 0x65, 0x73,
                                                     0x70, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b,
                                                     0x32, 0x17, 0x2e, 0x6b, 0x76, 0x72, 0x70,
                                                     0x63, 0x70, 0x62, 0x2e, 0x43, 0x6d, 0x64,
                                                     0x47, 0x65, 0x74, 0x52, 0x65, 0x73, 0x70,
                                                     0x6f, 0x6e, 0x73, 0x65, 0x2a, 0x5e, 0x0a,
                                                     0x0b, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67,
                                                     0x65, 0x54, 0x79, 0x70, 0x65, 0x12, 0x0a,
                                                     0x0a, 0x06, 0x43, 0x6d, 0x64, 0x47, 0x65,
                                                     0x74, 0x10, 0x01, 0x12, 0x0b, 0x0a, 0x07,
                                                     0x43, 0x6d, 0x64, 0x53, 0x63, 0x61, 0x6e,
                                                     0x10, 0x02, 0x12, 0x0a, 0x0a, 0x06, 0x43,
                                                     0x6d, 0x64, 0x50, 0x75, 0x74, 0x10, 0x03,
                                                     0x12, 0x0a, 0x0a, 0x06, 0x43, 0x6d, 0x64,
                                                     0x44, 0x65, 0x6c, 0x10, 0x04, 0x12, 0x0f,
                                                     0x0a, 0x0b, 0x43, 0x6d, 0x64, 0x4c, 0x6f,
                                                     0x63, 0x6b, 0x4b, 0x65, 0x79, 0x73, 0x10,
                                                     0x05, 0x12, 0x0d, 0x0a, 0x09, 0x43, 0x6d,
                                                     0x64, 0x43, 0x6f, 0x6d, 0x6d, 0x69, 0x74,
                                                     0x10, 0x06, 0x4a, 0xd8, 0x07, 0x0a, 0x06,
                                                     0x12, 0x04, 0x00, 0x00, 0x1e, 0x01, 0x0a,
                                                     0x08, 0x0a, 0x01, 0x02, 0x12, 0x03, 0x01,
                                                     0x08, 0x0f, 0x0a, 0x0a, 0x0a, 0x02, 0x05,
                                                     0x00, 0x12, 0x04, 0x03, 0x00, 0x0a, 0x01,
                                                     0x0a, 0x0a, 0x0a, 0x03, 0x05, 0x00, 0x01,
                                                     0x12, 0x03, 0x03, 0x05, 0x10, 0x0a, 0x0b,
                                                     0x0a, 0x04, 0x05, 0x00, 0x02, 0x00, 0x12,
                                                     0x03, 0x04, 0x04, 0x0f, 0x0a, 0x0c, 0x0a,
                                                     0x05, 0x05, 0x00, 0x02, 0x00, 0x01, 0x12,
                                                     0x03, 0x04, 0x04, 0x0a, 0x0a, 0x0c, 0x0a,
                                                     0x05, 0x05, 0x00, 0x02, 0x00, 0x02, 0x12,
                                                     0x03, 0x04, 0x0d, 0x0e, 0x0a, 0x0b, 0x0a,
                                                     0x04, 0x05, 0x00, 0x02, 0x01, 0x12, 0x03,
                                                     0x05, 0x04, 0x10, 0x0a, 0x0c, 0x0a, 0x05,
                                                     0x05, 0x00, 0x02, 0x01, 0x01, 0x12, 0x03,
                                                     0x05, 0x04, 0x0b, 0x0a, 0x0c, 0x0a, 0x05,
                                                     0x05, 0x00, 0x02, 0x01, 0x02, 0x12, 0x03,
                                                     0x05, 0x0e, 0x0f, 0x0a, 0x0b, 0x0a, 0x04,
                                                     0x05, 0x00, 0x02, 0x02, 0x12, 0x03, 0x06,
                                                     0x04, 0x0f, 0x0a, 0x0c, 0x0a, 0x05, 0x05,
                                                     0x00, 0x02, 0x02, 0x01, 0x12, 0x03, 0x06,
                                                     0x04, 0x0a, 0x0a, 0x0c, 0x0a, 0x05, 0x05,
                                                     0x00, 0x02, 0x02, 0x02, 0x12, 0x03, 0x06,
                                                     0x0d, 0x0e, 0x0a, 0x0b, 0x0a, 0x04, 0x05,
                                                     0x00, 0x02, 0x03, 0x12, 0x03, 0x07, 0x04,
                                                     0x0f, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x00,
                                                     0x02, 0x03, 0x01, 0x12, 0x03, 0x07, 0x04,
                                                     0x0a, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x00,
                                                     0x02, 0x03, 0x02, 0x12, 0x03, 0x07, 0x0d,
                                                     0x0e, 0x0a, 0x0b, 0x0a, 0x04, 0x05, 0x00,
                                                     0x02, 0x04, 0x12, 0x03, 0x08, 0x04, 0x14,
                                                     0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x00, 0x02,
                                                     0x04, 0x01, 0x12, 0x03, 0x08, 0x04, 0x0f,
                                                     0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x00, 0x02,
                                                     0x04, 0x02, 0x12, 0x03, 0x08, 0x12, 0x13,
                                                     0x0a, 0x0b, 0x0a, 0x04, 0x05, 0x00, 0x02,
                                                     0x05, 0x12, 0x03, 0x09, 0x04, 0x12, 0x0a,
                                                     0x0c, 0x0a, 0x05, 0x05, 0x00, 0x02, 0x05,
                                                     0x01, 0x12, 0x03, 0x09, 0x04, 0x0d, 0x0a,
                                                     0x0c, 0x0a, 0x05, 0x05, 0x00, 0x02, 0x05,
                                                     0x02, 0x12, 0x03, 0x09, 0x10, 0x11, 0x0a,
                                                     0x0a, 0x0a, 0x02, 0x04, 0x00, 0x12, 0x04,
                                                     0x0b, 0x00, 0x0e, 0x01, 0x0a, 0x0a, 0x0a,
                                                     0x03, 0x04, 0x00, 0x01, 0x12, 0x03, 0x0b,
                                                     0x08, 0x15, 0x0a, 0x0b, 0x0a, 0x04, 0x04,
                                                     0x00, 0x02, 0x00, 0x12, 0x03, 0x0c, 0x04,
                                                     0x20, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00,
                                                     0x02, 0x00, 0x04, 0x12, 0x03, 0x0c, 0x04,
                                                     0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00,
                                                     0x02, 0x00, 0x05, 0x12, 0x03, 0x0c, 0x0d,
                                                     0x13, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00,
                                                     0x02, 0x00, 0x01, 0x12, 0x03, 0x0c, 0x14,
                                                     0x1b, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00,
                                                     0x02, 0x00, 0x03, 0x12, 0x03, 0x0c, 0x1e,
                                                     0x1f, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x00,
                                                     0x02, 0x01, 0x12, 0x03, 0x0d, 0x04, 0x1b,
                                                     0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02,
                                                     0x01, 0x04, 0x12, 0x03, 0x0d, 0x04, 0x0c,
                                                     0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02,
                                                     0x01, 0x05, 0x12, 0x03, 0x0d, 0x0d, 0x12,
                                                     0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02,
                                                     0x01, 0x01, 0x12, 0x03, 0x0d, 0x13, 0x16,
                                                     0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02,
                                                     0x01, 0x03, 0x12, 0x03, 0x0d, 0x19, 0x1a,
                                                     0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x01, 0x12,
                                                     0x04, 0x0f, 0x00, 0x12, 0x01, 0x0a, 0x0a,
                                                     0x0a, 0x03, 0x04, 0x01, 0x01, 0x12, 0x03,
                                                     0x0f, 0x08, 0x16, 0x0a, 0x0b, 0x0a, 0x04,
                                                     0x04, 0x01, 0x02, 0x00, 0x12, 0x03, 0x10,
                                                     0x04, 0x19, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
                                                     0x01, 0x02, 0x00, 0x04, 0x12, 0x03, 0x10,
                                                     0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
                                                     0x01, 0x02, 0x00, 0x05, 0x12, 0x03, 0x10,
                                                     0x0d, 0x11, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
                                                     0x01, 0x02, 0x00, 0x01, 0x12, 0x03, 0x10,
                                                     0x12, 0x14, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
                                                     0x01, 0x02, 0x00, 0x03, 0x12, 0x03, 0x10,
                                                     0x17, 0x18, 0x0a, 0x0b, 0x0a, 0x04, 0x04,
                                                     0x01, 0x02, 0x01, 0x12, 0x03, 0x11, 0x04,
                                                     0x1d, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x01,
                                                     0x02, 0x01, 0x04, 0x12, 0x03, 0x11, 0x04,
                                                     0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x01,
                                                     0x02, 0x01, 0x05, 0x12, 0x03, 0x11, 0x0d,
                                                     0x12, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x01,
                                                     0x02, 0x01, 0x01, 0x12, 0x03, 0x11, 0x13,
                                                     0x18, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x01,
                                                     0x02, 0x01, 0x03, 0x12, 0x03, 0x11, 0x1b,
                                                     0x1c, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x02,
                                                     0x12, 0x04, 0x13, 0x00, 0x14, 0x01, 0x0a,
                                                     0x0a, 0x0a, 0x03, 0x04, 0x02, 0x01, 0x12,
                                                     0x03, 0x13, 0x08, 0x16, 0x0a, 0x0a, 0x0a,
                                                     0x02, 0x04, 0x03, 0x12, 0x04, 0x15, 0x00,
                                                     0x16, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04,
                                                     0x03, 0x01, 0x12, 0x03, 0x15, 0x08, 0x17,
                                                     0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x04, 0x12,
                                                     0x04, 0x17, 0x00, 0x1a, 0x01, 0x0a, 0x0a,
                                                     0x0a, 0x03, 0x04, 0x04, 0x01, 0x12, 0x03,
                                                     0x17, 0x08, 0x0f, 0x0a, 0x0b, 0x0a, 0x04,
                                                     0x04, 0x04, 0x02, 0x00, 0x12, 0x03, 0x18,
                                                     0x04, 0x22, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
                                                     0x04, 0x02, 0x00, 0x04, 0x12, 0x03, 0x18,
                                                     0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
                                                     0x04, 0x02, 0x00, 0x06, 0x12, 0x03, 0x18,
                                                     0x0d, 0x18, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
                                                     0x04, 0x02, 0x00, 0x01, 0x12, 0x03, 0x18,
                                                     0x19, 0x1d, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
                                                     0x04, 0x02, 0x00, 0x03, 0x12, 0x03, 0x18,
                                                     0x20, 0x21, 0x0a, 0x0b, 0x0a, 0x04, 0x04,
                                                     0x04, 0x02, 0x01, 0x12, 0x03, 0x19, 0x04,
                                                     0x2b, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04,
                                                     0x02, 0x01, 0x04, 0x12, 0x03, 0x19, 0x04,
                                                     0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04,
                                                     0x02, 0x01, 0x06, 0x12, 0x03, 0x19, 0x0d,
                                                     0x1a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04,
                                                     0x02, 0x01, 0x01, 0x12, 0x03, 0x19, 0x1b,
                                                     0x26, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04,
                                                     0x02, 0x01, 0x03, 0x12, 0x03, 0x19, 0x29,
                                                     0x2a, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x05,
                                                     0x12, 0x04, 0x1b, 0x00, 0x1e, 0x01, 0x0a,
                                                     0x0a, 0x0a, 0x03, 0x04, 0x05, 0x01, 0x12,
                                                     0x03, 0x1b, 0x08, 0x10, 0x0a, 0x0b, 0x0a,
                                                     0x04, 0x04, 0x05, 0x02, 0x00, 0x12, 0x03,
                                                     0x1c, 0x04, 0x22, 0x0a, 0x0c, 0x0a, 0x05,
                                                     0x04, 0x05, 0x02, 0x00, 0x04, 0x12, 0x03,
                                                     0x1c, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05,
                                                     0x04, 0x05, 0x02, 0x00, 0x06, 0x12, 0x03,
                                                     0x1c, 0x0d, 0x18, 0x0a, 0x0c, 0x0a, 0x05,
                                                     0x04, 0x05, 0x02, 0x00, 0x01, 0x12, 0x03,
                                                     0x1c, 0x19, 0x1d, 0x0a, 0x0c, 0x0a, 0x05,
                                                     0x04, 0x05, 0x02, 0x00, 0x03, 0x12, 0x03,
                                                     0x1c, 0x20, 0x21, 0x0a, 0x0b, 0x0a, 0x04,
                                                     0x04, 0x05, 0x02, 0x01, 0x12, 0x03, 0x1d,
                                                     0x04, 0x2d, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
                                                     0x05, 0x02, 0x01, 0x04, 0x12, 0x03, 0x1d,
                                                     0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
                                                     0x05, 0x02, 0x01, 0x06, 0x12, 0x03, 0x1d,
                                                     0x0d, 0x1b, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
                                                     0x05, 0x02, 0x01, 0x01, 0x12, 0x03, 0x1d,
                                                     0x1c, 0x28, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
                                                     0x05, 0x02, 0x01, 0x03, 0x12, 0x03, 0x1d,
                                                     0x2b, 0x2c];

static mut file_descriptor_proto_lazy: ::protobuf::lazy::Lazy<::protobuf::descriptor::FileDescriptorProto> = ::protobuf::lazy::Lazy {
    lock: ::protobuf::lazy::ONCE_INIT,
    ptr: 0 as *const ::protobuf::descriptor::FileDescriptorProto,
};

fn parse_descriptor_proto() -> ::protobuf::descriptor::FileDescriptorProto {
    ::protobuf::parse_from_bytes(file_descriptor_proto_data).unwrap()
}

pub fn file_descriptor_proto() -> &'static ::protobuf::descriptor::FileDescriptorProto {
    unsafe { file_descriptor_proto_lazy.get(|| parse_descriptor_proto()) }
}
