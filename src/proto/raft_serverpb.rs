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
pub struct RaftMessageRequest {
    // message fields
    message: ::protobuf::SingularPtrField<super::raftpb::Message>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

impl RaftMessageRequest {
    pub fn new() -> RaftMessageRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static RaftMessageRequest {
        static mut instance: ::protobuf::lazy::Lazy<RaftMessageRequest> =
            ::protobuf::lazy::Lazy {
                lock: ::protobuf::lazy::ONCE_INIT,
                ptr: 0 as *const RaftMessageRequest,
            };
        unsafe {
            instance.get(|| {
                RaftMessageRequest {
                    message: ::protobuf::SingularPtrField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional .raftpb.Message message = 1;

    pub fn clear_message(&mut self) {
        self.message.clear();
    }

    pub fn has_message(&self) -> bool {
        self.message.is_some()
    }

    // Param is passed by value, moved
    pub fn set_message(&mut self, v: super::raftpb::Message) {
        self.message = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_message<'a>(&'a mut self) -> &'a mut super::raftpb::Message {
        if self.message.is_none() {
            self.message.set_default();
        };
        self.message.as_mut().unwrap()
    }

    // Take field
    pub fn take_message(&mut self) -> super::raftpb::Message {
        self.message.take().unwrap_or_else(|| super::raftpb::Message::new())
    }

    pub fn get_message<'a>(&'a self) -> &'a super::raftpb::Message {
        self.message.as_ref().unwrap_or_else(|| super::raftpb::Message::default_instance())
    }
}

impl ::protobuf::Message for RaftMessageRequest {
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
                    try!(::protobuf::rt::read_singular_message_into(wire_type,
                                                                    is,
                                                                    &mut self.message));
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
        for value in self.message.iter() {
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
        if let Some(v) = self.message.as_ref() {
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

    fn get_unknown_fields<'s>(&'s self) -> &'s ::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields<'s>(&'s mut self) -> &'s mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<RaftMessageRequest>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for RaftMessageRequest {
    fn new() -> RaftMessageRequest {
        RaftMessageRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<RaftMessageRequest>)
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
                    "message",
                    RaftMessageRequest::has_message,
                    RaftMessageRequest::get_message,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<RaftMessageRequest>(
                    "RaftMessageRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for RaftMessageRequest {
    fn clear(&mut self) {
        self.clear_message();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for RaftMessageRequest {
    fn eq(&self, other: &RaftMessageRequest) -> bool {
        self.message == other.message && self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for RaftMessageRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct RaftMessageResponse {
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

impl RaftMessageResponse {
    pub fn new() -> RaftMessageResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static RaftMessageResponse {
        static mut instance: ::protobuf::lazy::Lazy<RaftMessageResponse> =
            ::protobuf::lazy::Lazy {
                lock: ::protobuf::lazy::ONCE_INIT,
                ptr: 0 as *const RaftMessageResponse,
            };
        unsafe {
            instance.get(|| {
                RaftMessageResponse {
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }
}

impl ::protobuf::Message for RaftMessageResponse {
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
        ::std::any::TypeId::of::<RaftMessageResponse>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for RaftMessageResponse {
    fn new() -> RaftMessageResponse {
        RaftMessageResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<RaftMessageResponse>)
                         -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> =
            ::protobuf::lazy::Lazy {
                lock: ::protobuf::lazy::ONCE_INIT,
                ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
            };
        unsafe {
            descriptor.get(|| {
                let fields = ::std::vec::Vec::new();
                ::protobuf::reflect::MessageDescriptor::new::<RaftMessageResponse>(
                    "RaftMessageResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for RaftMessageResponse {
    fn clear(&mut self) {
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for RaftMessageResponse {
    fn eq(&self, other: &RaftMessageResponse) -> bool {
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for RaftMessageResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct ConfChangeContext {
    // message fields
    command_id: ::protobuf::SingularField<::std::string::String>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

impl ConfChangeContext {
    pub fn new() -> ConfChangeContext {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static ConfChangeContext {
        static mut instance: ::protobuf::lazy::Lazy<ConfChangeContext> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ConfChangeContext,
        };
        unsafe {
            instance.get(|| {
                ConfChangeContext {
                    command_id: ::protobuf::SingularField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional string command_id = 1;

    pub fn clear_command_id(&mut self) {
        self.command_id.clear();
    }

    pub fn has_command_id(&self) -> bool {
        self.command_id.is_some()
    }

    // Param is passed by value, moved
    pub fn set_command_id(&mut self, v: ::std::string::String) {
        self.command_id = ::protobuf::SingularField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_command_id<'a>(&'a mut self) -> &'a mut ::std::string::String {
        if self.command_id.is_none() {
            self.command_id.set_default();
        };
        self.command_id.as_mut().unwrap()
    }

    // Take field
    pub fn take_command_id(&mut self) -> ::std::string::String {
        self.command_id.take().unwrap_or_else(|| ::std::string::String::new())
    }

    pub fn get_command_id<'a>(&'a self) -> &'a str {
        match self.command_id.as_ref() {
            Some(v) => &v,
            None => "",
        }
    }
}

impl ::protobuf::Message for ConfChangeContext {
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
                    try!(::protobuf::rt::read_singular_string_into(wire_type,
                                                                   is,
                                                                   &mut self.command_id));
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
        for value in self.command_id.iter() {
            my_size += ::protobuf::rt::string_size(1, &value);
        }
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self,
                                  os: &mut ::protobuf::CodedOutputStream)
                                  -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.command_id.as_ref() {
            try!(os.write_string(1, &v));
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
        ::std::any::TypeId::of::<ConfChangeContext>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for ConfChangeContext {
    fn new() -> ConfChangeContext {
        ConfChangeContext::new()
    }

    fn descriptor_static(_: ::std::option::Option<ConfChangeContext>)
                         -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> =
            ::protobuf::lazy::Lazy {
                lock: ::protobuf::lazy::ONCE_INIT,
                ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
            };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_string_accessor(
                    "command_id",
                    ConfChangeContext::has_command_id,
                    ConfChangeContext::get_command_id,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<ConfChangeContext>(
                    "ConfChangeContext",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for ConfChangeContext {
    fn clear(&mut self) {
        self.clear_command_id();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for ConfChangeContext {
    fn eq(&self, other: &ConfChangeContext) -> bool {
        self.command_id == other.command_id && self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for ConfChangeContext {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

static file_descriptor_proto_data: &'static [u8] = &[0x0a, 0x13, 0x72, 0x61, 0x66, 0x74, 0x5f,
                                                     0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x70,
                                                     0x62, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f,
                                                     0x12, 0x0d, 0x72, 0x61, 0x66, 0x74, 0x5f,
                                                     0x73, 0x65, 0x72, 0x76, 0x65, 0x72, 0x70,
                                                     0x62, 0x1a, 0x0c, 0x72, 0x61, 0x66, 0x74,
                                                     0x70, 0x62, 0x2e, 0x70, 0x72, 0x6f, 0x74,
                                                     0x6f, 0x22, 0x36, 0x0a, 0x12, 0x52, 0x61,
                                                     0x66, 0x74, 0x4d, 0x65, 0x73, 0x73, 0x61,
                                                     0x67, 0x65, 0x52, 0x65, 0x71, 0x75, 0x65,
                                                     0x73, 0x74, 0x12, 0x20, 0x0a, 0x07, 0x6d,
                                                     0x65, 0x73, 0x73, 0x61, 0x67, 0x65, 0x18,
                                                     0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0f,
                                                     0x2e, 0x72, 0x61, 0x66, 0x74, 0x70, 0x62,
                                                     0x2e, 0x4d, 0x65, 0x73, 0x73, 0x61, 0x67,
                                                     0x65, 0x22, 0x15, 0x0a, 0x13, 0x52, 0x61,
                                                     0x66, 0x74, 0x4d, 0x65, 0x73, 0x73, 0x61,
                                                     0x67, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f,
                                                     0x6e, 0x73, 0x65, 0x22, 0x27, 0x0a, 0x11,
                                                     0x43, 0x6f, 0x6e, 0x66, 0x43, 0x68, 0x61,
                                                     0x6e, 0x67, 0x65, 0x43, 0x6f, 0x6e, 0x74,
                                                     0x65, 0x78, 0x74, 0x12, 0x12, 0x0a, 0x0a,
                                                     0x63, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64,
                                                     0x5f, 0x69, 0x64, 0x18, 0x01, 0x20, 0x01,
                                                     0x28, 0x09, 0x4a, 0xb7, 0x02, 0x0a, 0x06,
                                                     0x12, 0x04, 0x00, 0x00, 0x0f, 0x01, 0x0a,
                                                     0x08, 0x0a, 0x01, 0x02, 0x12, 0x03, 0x01,
                                                     0x08, 0x15, 0x0a, 0x09, 0x0a, 0x02, 0x03,
                                                     0x00, 0x12, 0x03, 0x03, 0x07, 0x15, 0x0a,
                                                     0x0a, 0x0a, 0x02, 0x04, 0x00, 0x12, 0x04,
                                                     0x05, 0x00, 0x07, 0x01, 0x0a, 0x0a, 0x0a,
                                                     0x03, 0x04, 0x00, 0x01, 0x12, 0x03, 0x05,
                                                     0x08, 0x1a, 0x0a, 0x0b, 0x0a, 0x04, 0x04,
                                                     0x00, 0x02, 0x00, 0x12, 0x03, 0x06, 0x01,
                                                     0x25, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00,
                                                     0x02, 0x00, 0x04, 0x12, 0x03, 0x06, 0x01,
                                                     0x09, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00,
                                                     0x02, 0x00, 0x06, 0x12, 0x03, 0x06, 0x0a,
                                                     0x18, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00,
                                                     0x02, 0x00, 0x01, 0x12, 0x03, 0x06, 0x19,
                                                     0x20, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00,
                                                     0x02, 0x00, 0x03, 0x12, 0x03, 0x06, 0x23,
                                                     0x24, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x01,
                                                     0x12, 0x04, 0x09, 0x00, 0x0a, 0x01, 0x0a,
                                                     0x0a, 0x0a, 0x03, 0x04, 0x01, 0x01, 0x12,
                                                     0x03, 0x09, 0x08, 0x1b, 0x0a, 0x52, 0x0a,
                                                     0x02, 0x04, 0x02, 0x12, 0x04, 0x0d, 0x00,
                                                     0x0f, 0x01, 0x1a, 0x46, 0x20, 0x43, 0x6f,
                                                     0x6e, 0x66, 0x43, 0x68, 0x61, 0x6e, 0x67,
                                                     0x65, 0x43, 0x6f, 0x6e, 0x74, 0x65, 0x78,
                                                     0x74, 0x20, 0x69, 0x73, 0x20, 0x65, 0x6e,
                                                     0x63, 0x6f, 0x64, 0x65, 0x64, 0x20, 0x69,
                                                     0x6e, 0x20, 0x74, 0x68, 0x65, 0x20, 0x72,
                                                     0x61, 0x66, 0x74, 0x70, 0x62, 0x2e, 0x43,
                                                     0x6f, 0x6e, 0x66, 0x43, 0x68, 0x61, 0x6e,
                                                     0x67, 0x65, 0x2e, 0x43, 0x6f, 0x6e, 0x74,
                                                     0x65, 0x78, 0x74, 0x20, 0x66, 0x69, 0x65,
                                                     0x6c, 0x64, 0x2e, 0x0a, 0x0a, 0x0a, 0x0a,
                                                     0x03, 0x04, 0x02, 0x01, 0x12, 0x03, 0x0d,
                                                     0x08, 0x19, 0x0a, 0x0b, 0x0a, 0x04, 0x04,
                                                     0x02, 0x02, 0x00, 0x12, 0x03, 0x0e, 0x02,
                                                     0x21, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x02,
                                                     0x02, 0x00, 0x04, 0x12, 0x03, 0x0e, 0x02,
                                                     0x0a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x02,
                                                     0x02, 0x00, 0x05, 0x12, 0x03, 0x0e, 0x0b,
                                                     0x11, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x02,
                                                     0x02, 0x00, 0x01, 0x12, 0x03, 0x0e, 0x12,
                                                     0x1c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x02,
                                                     0x02, 0x00, 0x03, 0x12, 0x03, 0x0e, 0x1f,
                                                     0x20];

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
