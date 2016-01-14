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
pub struct FooRequest {
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

impl FooRequest {
    pub fn new() -> FooRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static FooRequest {
        static mut instance: ::protobuf::lazy::Lazy<FooRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const FooRequest,
        };
        unsafe {
            instance.get(|| {
                FooRequest {
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }
}

impl ::protobuf::Message for FooRequest {
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
        ::std::any::TypeId::of::<FooRequest>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for FooRequest {
    fn new() -> FooRequest {
        FooRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<FooRequest>)
                         -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> =
            ::protobuf::lazy::Lazy {
                lock: ::protobuf::lazy::ONCE_INIT,
                ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
            };
        unsafe {
            descriptor.get(|| {
                let fields = ::std::vec::Vec::new();
                ::protobuf::reflect::MessageDescriptor::new::<FooRequest>("FooRequest",
                                                                          fields,
                                                                          file_descriptor_proto())
            })
        }
    }
}

impl ::protobuf::Clear for FooRequest {
    fn clear(&mut self) {
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for FooRequest {
    fn eq(&self, other: &FooRequest) -> bool {
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for FooRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct FooResponse {
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

impl FooResponse {
    pub fn new() -> FooResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static FooResponse {
        static mut instance: ::protobuf::lazy::Lazy<FooResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const FooResponse,
        };
        unsafe {
            instance.get(|| {
                FooResponse {
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }
}

impl ::protobuf::Message for FooResponse {
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
        ::std::any::TypeId::of::<FooResponse>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for FooResponse {
    fn new() -> FooResponse {
        FooResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<FooResponse>)
                         -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> =
            ::protobuf::lazy::Lazy {
                lock: ::protobuf::lazy::ONCE_INIT,
                ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
            };
        unsafe {
            descriptor.get(|| {
                let fields = ::std::vec::Vec::new();
                ::protobuf::reflect::MessageDescriptor::new::<FooResponse>("FooResponse",
                                                                           fields,
                                                                           file_descriptor_proto())
            })
        }
    }
}

impl ::protobuf::Clear for FooResponse {
    fn clear(&mut self) {
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for FooResponse {
    fn eq(&self, other: &FooResponse) -> bool {
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for FooResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct Message {
    // message oneof groups
    body: ::std::option::Option<Message_oneof_body>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

#[derive(Clone,PartialEq)]
pub enum Message_oneof_body {
    fooReq(FooRequest),
    fooResp(FooResponse),
}

impl Message {
    pub fn new() -> Message {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static Message {
        static mut instance: ::protobuf::lazy::Lazy<Message> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const Message,
        };
        unsafe {
            instance.get(|| {
                Message {
                    body: ::std::option::Option::None,
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional .kvrpc.FooRequest fooReq = 1;

    pub fn clear_fooReq(&mut self) {
        self.body = ::std::option::Option::None;
    }

    pub fn has_fooReq(&self) -> bool {
        match self.body {
            ::std::option::Option::Some(Message_oneof_body::fooReq(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_fooReq(&mut self, v: FooRequest) {
        self.body = ::std::option::Option::Some(Message_oneof_body::fooReq(v))
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_fooReq<'a>(&'a mut self) -> &'a mut FooRequest {
        if let ::std::option::Option::Some(Message_oneof_body::fooReq(_)) = self.body {
        } else {
            self.body = ::std::option::Option::Some(Message_oneof_body::fooReq(FooRequest::new()));
        }
        match self.body {
            ::std::option::Option::Some(Message_oneof_body::fooReq(ref mut v)) => v,
            _ => panic!(),
        }
    }

    // Take field
    pub fn take_fooReq(&mut self) -> FooRequest {
        if self.has_fooReq() {
            match self.body.take() {
                ::std::option::Option::Some(Message_oneof_body::fooReq(v)) => v,
                _ => panic!(),
            }
        } else {
            FooRequest::new()
        }
    }

    pub fn get_fooReq<'a>(&'a self) -> &'a FooRequest {
        match self.body {
            ::std::option::Option::Some(Message_oneof_body::fooReq(ref v)) => v,
            _ => FooRequest::default_instance(),
        }
    }

    // optional .kvrpc.FooResponse fooResp = 2;

    pub fn clear_fooResp(&mut self) {
        self.body = ::std::option::Option::None;
    }

    pub fn has_fooResp(&self) -> bool {
        match self.body {
            ::std::option::Option::Some(Message_oneof_body::fooResp(..)) => true,
            _ => false,
        }
    }

    // Param is passed by value, moved
    pub fn set_fooResp(&mut self, v: FooResponse) {
        self.body = ::std::option::Option::Some(Message_oneof_body::fooResp(v))
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_fooResp<'a>(&'a mut self) -> &'a mut FooResponse {
        if let ::std::option::Option::Some(Message_oneof_body::fooResp(_)) = self.body {
        } else {
            self.body =
                ::std::option::Option::Some(Message_oneof_body::fooResp(FooResponse::new()));
        }
        match self.body {
            ::std::option::Option::Some(Message_oneof_body::fooResp(ref mut v)) => v,
            _ => panic!(),
        }
    }

    // Take field
    pub fn take_fooResp(&mut self) -> FooResponse {
        if self.has_fooResp() {
            match self.body.take() {
                ::std::option::Option::Some(Message_oneof_body::fooResp(v)) => v,
                _ => panic!(),
            }
        } else {
            FooResponse::new()
        }
    }

    pub fn get_fooResp<'a>(&'a self) -> &'a FooResponse {
        match self.body {
            ::std::option::Option::Some(Message_oneof_body::fooResp(ref v)) => v,
            _ => FooResponse::default_instance(),
        }
    }
}

impl ::protobuf::Message for Message {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self,
                  is: &mut ::protobuf::CodedInputStream)
                  -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    if wire_type != ::protobuf::wire_format::WireTypeLengthDelimited {
                        return ::std::result::Result::Err(::protobuf::ProtobufError::WireError("unexpected wire type".to_string()));
                    };
                    self.body = ::std::option::Option::Some(Message_oneof_body::fooReq(try!(is.read_message())));
                }
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeLengthDelimited {
                        return ::std::result::Result::Err(::protobuf::ProtobufError::WireError("unexpected wire type".to_string()));
                    };
                    self.body = ::std::option::Option::Some(Message_oneof_body::fooResp(try!(is.read_message())));
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
        if let ::std::option::Option::Some(ref v) = self.body {
            match v {
                &Message_oneof_body::fooReq(ref v) => {
                    let len = v.compute_size();
                    my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
                }
                &Message_oneof_body::fooResp(ref v) => {
                    let len = v.compute_size();
                    my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
                }
            };
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self,
                                  os: &mut ::protobuf::CodedOutputStream)
                                  -> ::protobuf::ProtobufResult<()> {
        if let ::std::option::Option::Some(ref v) = self.body {
            match v {
                &Message_oneof_body::fooReq(ref v) => {
                    try!(os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited));
                    try!(os.write_raw_varint32(v.get_cached_size()));
                    try!(v.write_to_with_cached_sizes(os));
                }
                &Message_oneof_body::fooResp(ref v) => {
                    try!(os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited));
                    try!(os.write_raw_varint32(v.get_cached_size()));
                    try!(v.write_to_with_cached_sizes(os));
                }
            };
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
        ::std::any::TypeId::of::<Message>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for Message {
    fn new() -> Message {
        Message::new()
    }

    fn descriptor_static(_: ::std::option::Option<Message>)
                         -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> =
            ::protobuf::lazy::Lazy {
                lock: ::protobuf::lazy::ONCE_INIT,
                ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
            };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "fooReq",
                    Message::has_fooReq,
                    Message::get_fooReq,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "fooResp",
                    Message::has_fooResp,
                    Message::get_fooResp,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<Message>("Message",
                                                                       fields,
                                                                       file_descriptor_proto())
            })
        }
    }
}

impl ::protobuf::Clear for Message {
    fn clear(&mut self) {
        self.clear_fooReq();
        self.clear_fooResp();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for Message {
    fn eq(&self, other: &Message) -> bool {
        self.body == other.body && self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for Message {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

static file_descriptor_proto_data: &'static [u8] = &[0x0a, 0x0b, 0x6b, 0x76, 0x72, 0x70, 0x63,
                                                     0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x12,
                                                     0x05, 0x6b, 0x76, 0x72, 0x70, 0x63, 0x22,
                                                     0x0c, 0x0a, 0x0a, 0x46, 0x6f, 0x6f, 0x52,
                                                     0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x22,
                                                     0x0d, 0x0a, 0x0b, 0x46, 0x6f, 0x6f, 0x52,
                                                     0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65,
                                                     0x22, 0x5d, 0x0a, 0x07, 0x4d, 0x65, 0x73,
                                                     0x73, 0x61, 0x67, 0x65, 0x12, 0x23, 0x0a,
                                                     0x06, 0x66, 0x6f, 0x6f, 0x52, 0x65, 0x71,
                                                     0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32,
                                                     0x11, 0x2e, 0x6b, 0x76, 0x72, 0x70, 0x63,
                                                     0x2e, 0x46, 0x6f, 0x6f, 0x52, 0x65, 0x71,
                                                     0x75, 0x65, 0x73, 0x74, 0x48, 0x00, 0x12,
                                                     0x25, 0x0a, 0x07, 0x66, 0x6f, 0x6f, 0x52,
                                                     0x65, 0x73, 0x70, 0x18, 0x02, 0x20, 0x01,
                                                     0x28, 0x0b, 0x32, 0x12, 0x2e, 0x6b, 0x76,
                                                     0x72, 0x70, 0x63, 0x2e, 0x46, 0x6f, 0x6f,
                                                     0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73,
                                                     0x65, 0x48, 0x00, 0x42, 0x06, 0x0a, 0x04,
                                                     0x62, 0x6f, 0x64, 0x79, 0x4a, 0xe4, 0x01,
                                                     0x0a, 0x06, 0x12, 0x04, 0x00, 0x00, 0x0c,
                                                     0x01, 0x0a, 0x08, 0x0a, 0x01, 0x02, 0x12,
                                                     0x03, 0x01, 0x08, 0x0d, 0x0a, 0x0a, 0x0a,
                                                     0x02, 0x04, 0x00, 0x12, 0x04, 0x03, 0x00,
                                                     0x04, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04,
                                                     0x00, 0x01, 0x12, 0x03, 0x03, 0x08, 0x12,
                                                     0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x01, 0x12,
                                                     0x04, 0x05, 0x00, 0x06, 0x01, 0x0a, 0x0a,
                                                     0x0a, 0x03, 0x04, 0x01, 0x01, 0x12, 0x03,
                                                     0x05, 0x08, 0x13, 0x0a, 0x0a, 0x0a, 0x02,
                                                     0x04, 0x02, 0x12, 0x04, 0x07, 0x00, 0x0c,
                                                     0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x02,
                                                     0x01, 0x12, 0x03, 0x07, 0x08, 0x0f, 0x0a,
                                                     0x0c, 0x0a, 0x04, 0x04, 0x02, 0x08, 0x00,
                                                     0x12, 0x04, 0x08, 0x04, 0x0b, 0x05, 0x0a,
                                                     0x0c, 0x0a, 0x05, 0x04, 0x02, 0x08, 0x00,
                                                     0x01, 0x12, 0x03, 0x08, 0x0a, 0x0e, 0x0a,
                                                     0x0b, 0x0a, 0x04, 0x04, 0x02, 0x02, 0x00,
                                                     0x12, 0x03, 0x09, 0x08, 0x1e, 0x0a, 0x0c,
                                                     0x0a, 0x05, 0x04, 0x02, 0x02, 0x00, 0x06,
                                                     0x12, 0x03, 0x09, 0x08, 0x12, 0x0a, 0x0c,
                                                     0x0a, 0x05, 0x04, 0x02, 0x02, 0x00, 0x01,
                                                     0x12, 0x03, 0x09, 0x13, 0x19, 0x0a, 0x0c,
                                                     0x0a, 0x05, 0x04, 0x02, 0x02, 0x00, 0x03,
                                                     0x12, 0x03, 0x09, 0x1c, 0x1d, 0x0a, 0x0b,
                                                     0x0a, 0x04, 0x04, 0x02, 0x02, 0x01, 0x12,
                                                     0x03, 0x0a, 0x08, 0x20, 0x0a, 0x0c, 0x0a,
                                                     0x05, 0x04, 0x02, 0x02, 0x01, 0x06, 0x12,
                                                     0x03, 0x0a, 0x08, 0x13, 0x0a, 0x0c, 0x0a,
                                                     0x05, 0x04, 0x02, 0x02, 0x01, 0x01, 0x12,
                                                     0x03, 0x0a, 0x14, 0x1b, 0x0a, 0x0c, 0x0a,
                                                     0x05, 0x04, 0x02, 0x02, 0x01, 0x03, 0x12,
                                                     0x03, 0x0a, 0x1e, 0x1f];

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
