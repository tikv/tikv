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
pub struct GetRequest {
    // message fields
    key: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for GetRequest {}

impl GetRequest {
    pub fn new() -> GetRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static GetRequest {
        static mut instance: ::protobuf::lazy::Lazy<GetRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const GetRequest,
        };
        unsafe {
            instance.get(|| {
                GetRequest {
                    key: ::protobuf::SingularField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional bytes key = 1;

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

impl ::protobuf::Message for GetRequest {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    try!(::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.key));
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
        for value in self.key.iter() {
            my_size += ::protobuf::rt::bytes_size(1, &value);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.key.as_ref() {
            try!(os.write_bytes(1, &v));
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
        ::std::any::TypeId::of::<GetRequest>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for GetRequest {
    fn new() -> GetRequest {
        GetRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<GetRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_bytes_accessor(
                    "key",
                    GetRequest::has_key,
                    GetRequest::get_key,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<GetRequest>(
                    "GetRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for GetRequest {
    fn clear(&mut self) {
        self.clear_key();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for GetRequest {
    fn eq(&self, other: &GetRequest) -> bool {
        self.key == other.key &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for GetRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct GetResponse {
    // message fields
    value: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for GetResponse {}

impl GetResponse {
    pub fn new() -> GetResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static GetResponse {
        static mut instance: ::protobuf::lazy::Lazy<GetResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const GetResponse,
        };
        unsafe {
            instance.get(|| {
                GetResponse {
                    value: ::protobuf::SingularField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional bytes value = 1;

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

impl ::protobuf::Message for GetResponse {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    try!(::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.value));
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
        for value in self.value.iter() {
            my_size += ::protobuf::rt::bytes_size(1, &value);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.value.as_ref() {
            try!(os.write_bytes(1, &v));
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
        ::std::any::TypeId::of::<GetResponse>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for GetResponse {
    fn new() -> GetResponse {
        GetResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<GetResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_bytes_accessor(
                    "value",
                    GetResponse::has_value,
                    GetResponse::get_value,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<GetResponse>(
                    "GetResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for GetResponse {
    fn clear(&mut self) {
        self.clear_value();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for GetResponse {
    fn eq(&self, other: &GetResponse) -> bool {
        self.value == other.value &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for GetResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct SeekRequest {
    // message fields
    key: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for SeekRequest {}

impl SeekRequest {
    pub fn new() -> SeekRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static SeekRequest {
        static mut instance: ::protobuf::lazy::Lazy<SeekRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const SeekRequest,
        };
        unsafe {
            instance.get(|| {
                SeekRequest {
                    key: ::protobuf::SingularField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional bytes key = 1;

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

impl ::protobuf::Message for SeekRequest {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    try!(::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.key));
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
        for value in self.key.iter() {
            my_size += ::protobuf::rt::bytes_size(1, &value);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.key.as_ref() {
            try!(os.write_bytes(1, &v));
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
        ::std::any::TypeId::of::<SeekRequest>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for SeekRequest {
    fn new() -> SeekRequest {
        SeekRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<SeekRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_bytes_accessor(
                    "key",
                    SeekRequest::has_key,
                    SeekRequest::get_key,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<SeekRequest>(
                    "SeekRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for SeekRequest {
    fn clear(&mut self) {
        self.clear_key();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for SeekRequest {
    fn eq(&self, other: &SeekRequest) -> bool {
        self.key == other.key &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for SeekRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct SeekResponse {
    // message fields
    key: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    value: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for SeekResponse {}

impl SeekResponse {
    pub fn new() -> SeekResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static SeekResponse {
        static mut instance: ::protobuf::lazy::Lazy<SeekResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const SeekResponse,
        };
        unsafe {
            instance.get(|| {
                SeekResponse {
                    key: ::protobuf::SingularField::none(),
                    value: ::protobuf::SingularField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional bytes key = 1;

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

    // optional bytes value = 2;

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

impl ::protobuf::Message for SeekResponse {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    try!(::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.key));
                },
                2 => {
                    try!(::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.value));
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
        for value in self.key.iter() {
            my_size += ::protobuf::rt::bytes_size(1, &value);
        };
        for value in self.value.iter() {
            my_size += ::protobuf::rt::bytes_size(2, &value);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.key.as_ref() {
            try!(os.write_bytes(1, &v));
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
        ::std::any::TypeId::of::<SeekResponse>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for SeekResponse {
    fn new() -> SeekResponse {
        SeekResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<SeekResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_bytes_accessor(
                    "key",
                    SeekResponse::has_key,
                    SeekResponse::get_key,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_bytes_accessor(
                    "value",
                    SeekResponse::has_value,
                    SeekResponse::get_value,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<SeekResponse>(
                    "SeekResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for SeekResponse {
    fn clear(&mut self) {
        self.clear_key();
        self.clear_value();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for SeekResponse {
    fn eq(&self, other: &SeekResponse) -> bool {
        self.key == other.key &&
        self.value == other.value &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for SeekResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct PutRequest {
    // message fields
    key: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    value: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for PutRequest {}

impl PutRequest {
    pub fn new() -> PutRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static PutRequest {
        static mut instance: ::protobuf::lazy::Lazy<PutRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const PutRequest,
        };
        unsafe {
            instance.get(|| {
                PutRequest {
                    key: ::protobuf::SingularField::none(),
                    value: ::protobuf::SingularField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional bytes key = 1;

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

    // optional bytes value = 2;

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

impl ::protobuf::Message for PutRequest {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    try!(::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.key));
                },
                2 => {
                    try!(::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.value));
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
        for value in self.key.iter() {
            my_size += ::protobuf::rt::bytes_size(1, &value);
        };
        for value in self.value.iter() {
            my_size += ::protobuf::rt::bytes_size(2, &value);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.key.as_ref() {
            try!(os.write_bytes(1, &v));
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
        ::std::any::TypeId::of::<PutRequest>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for PutRequest {
    fn new() -> PutRequest {
        PutRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<PutRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_bytes_accessor(
                    "key",
                    PutRequest::has_key,
                    PutRequest::get_key,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_bytes_accessor(
                    "value",
                    PutRequest::has_value,
                    PutRequest::get_value,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<PutRequest>(
                    "PutRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for PutRequest {
    fn clear(&mut self) {
        self.clear_key();
        self.clear_value();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for PutRequest {
    fn eq(&self, other: &PutRequest) -> bool {
        self.key == other.key &&
        self.value == other.value &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for PutRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct PutResponse {
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for PutResponse {}

impl PutResponse {
    pub fn new() -> PutResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static PutResponse {
        static mut instance: ::protobuf::lazy::Lazy<PutResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const PutResponse,
        };
        unsafe {
            instance.get(|| {
                PutResponse {
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }
}

impl ::protobuf::Message for PutResponse {
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

    fn get_unknown_fields<'s>(&'s self) -> &'s ::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields<'s>(&'s mut self) -> &'s mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<PutResponse>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for PutResponse {
    fn new() -> PutResponse {
        PutResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<PutResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let fields = ::std::vec::Vec::new();
                ::protobuf::reflect::MessageDescriptor::new::<PutResponse>(
                    "PutResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for PutResponse {
    fn clear(&mut self) {
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for PutResponse {
    fn eq(&self, other: &PutResponse) -> bool {
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for PutResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct DeleteRequest {
    // message fields
    key: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for DeleteRequest {}

impl DeleteRequest {
    pub fn new() -> DeleteRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static DeleteRequest {
        static mut instance: ::protobuf::lazy::Lazy<DeleteRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const DeleteRequest,
        };
        unsafe {
            instance.get(|| {
                DeleteRequest {
                    key: ::protobuf::SingularField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional bytes key = 1;

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

impl ::protobuf::Message for DeleteRequest {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    try!(::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.key));
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
        for value in self.key.iter() {
            my_size += ::protobuf::rt::bytes_size(1, &value);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.key.as_ref() {
            try!(os.write_bytes(1, &v));
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
        ::std::any::TypeId::of::<DeleteRequest>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for DeleteRequest {
    fn new() -> DeleteRequest {
        DeleteRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<DeleteRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_bytes_accessor(
                    "key",
                    DeleteRequest::has_key,
                    DeleteRequest::get_key,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<DeleteRequest>(
                    "DeleteRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for DeleteRequest {
    fn clear(&mut self) {
        self.clear_key();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for DeleteRequest {
    fn eq(&self, other: &DeleteRequest) -> bool {
        self.key == other.key &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for DeleteRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct DeleteResponse {
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for DeleteResponse {}

impl DeleteResponse {
    pub fn new() -> DeleteResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static DeleteResponse {
        static mut instance: ::protobuf::lazy::Lazy<DeleteResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const DeleteResponse,
        };
        unsafe {
            instance.get(|| {
                DeleteResponse {
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }
}

impl ::protobuf::Message for DeleteResponse {
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

    fn get_unknown_fields<'s>(&'s self) -> &'s ::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields<'s>(&'s mut self) -> &'s mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<DeleteResponse>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for DeleteResponse {
    fn new() -> DeleteResponse {
        DeleteResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<DeleteResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let fields = ::std::vec::Vec::new();
                ::protobuf::reflect::MessageDescriptor::new::<DeleteResponse>(
                    "DeleteResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for DeleteResponse {
    fn clear(&mut self) {
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for DeleteResponse {
    fn eq(&self, other: &DeleteResponse) -> bool {
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for DeleteResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct Request {
    // message fields
    cmd_type: ::std::option::Option<CommandType>,
    get: ::protobuf::SingularPtrField<GetRequest>,
    seek: ::protobuf::SingularPtrField<SeekRequest>,
    put: ::protobuf::SingularPtrField<PutRequest>,
    delete: ::protobuf::SingularPtrField<DeleteRequest>,
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
                    cmd_type: ::std::option::Option::None,
                    get: ::protobuf::SingularPtrField::none(),
                    seek: ::protobuf::SingularPtrField::none(),
                    put: ::protobuf::SingularPtrField::none(),
                    delete: ::protobuf::SingularPtrField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional .raft_cmdpb.CommandType cmd_type = 1;

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

    // optional .raft_cmdpb.GetRequest get = 2;

    pub fn clear_get(&mut self) {
        self.get.clear();
    }

    pub fn has_get(&self) -> bool {
        self.get.is_some()
    }

    // Param is passed by value, moved
    pub fn set_get(&mut self, v: GetRequest) {
        self.get = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_get<'a>(&'a mut self) -> &'a mut GetRequest {
        if self.get.is_none() {
            self.get.set_default();
        };
        self.get.as_mut().unwrap()
    }

    // Take field
    pub fn take_get(&mut self) -> GetRequest {
        self.get.take().unwrap_or_else(|| GetRequest::new())
    }

    pub fn get_get<'a>(&'a self) -> &'a GetRequest {
        self.get.as_ref().unwrap_or_else(|| GetRequest::default_instance())
    }

    // optional .raft_cmdpb.SeekRequest seek = 3;

    pub fn clear_seek(&mut self) {
        self.seek.clear();
    }

    pub fn has_seek(&self) -> bool {
        self.seek.is_some()
    }

    // Param is passed by value, moved
    pub fn set_seek(&mut self, v: SeekRequest) {
        self.seek = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_seek<'a>(&'a mut self) -> &'a mut SeekRequest {
        if self.seek.is_none() {
            self.seek.set_default();
        };
        self.seek.as_mut().unwrap()
    }

    // Take field
    pub fn take_seek(&mut self) -> SeekRequest {
        self.seek.take().unwrap_or_else(|| SeekRequest::new())
    }

    pub fn get_seek<'a>(&'a self) -> &'a SeekRequest {
        self.seek.as_ref().unwrap_or_else(|| SeekRequest::default_instance())
    }

    // optional .raft_cmdpb.PutRequest put = 4;

    pub fn clear_put(&mut self) {
        self.put.clear();
    }

    pub fn has_put(&self) -> bool {
        self.put.is_some()
    }

    // Param is passed by value, moved
    pub fn set_put(&mut self, v: PutRequest) {
        self.put = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_put<'a>(&'a mut self) -> &'a mut PutRequest {
        if self.put.is_none() {
            self.put.set_default();
        };
        self.put.as_mut().unwrap()
    }

    // Take field
    pub fn take_put(&mut self) -> PutRequest {
        self.put.take().unwrap_or_else(|| PutRequest::new())
    }

    pub fn get_put<'a>(&'a self) -> &'a PutRequest {
        self.put.as_ref().unwrap_or_else(|| PutRequest::default_instance())
    }

    // optional .raft_cmdpb.DeleteRequest delete = 5;

    pub fn clear_delete(&mut self) {
        self.delete.clear();
    }

    pub fn has_delete(&self) -> bool {
        self.delete.is_some()
    }

    // Param is passed by value, moved
    pub fn set_delete(&mut self, v: DeleteRequest) {
        self.delete = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_delete<'a>(&'a mut self) -> &'a mut DeleteRequest {
        if self.delete.is_none() {
            self.delete.set_default();
        };
        self.delete.as_mut().unwrap()
    }

    // Take field
    pub fn take_delete(&mut self) -> DeleteRequest {
        self.delete.take().unwrap_or_else(|| DeleteRequest::new())
    }

    pub fn get_delete<'a>(&'a self) -> &'a DeleteRequest {
        self.delete.as_ref().unwrap_or_else(|| DeleteRequest::default_instance())
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
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    let tmp = try!(is.read_enum());
                    self.cmd_type = ::std::option::Option::Some(tmp);
                },
                2 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.get));
                },
                3 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.seek));
                },
                4 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.put));
                },
                5 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.delete));
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
        for value in self.cmd_type.iter() {
            my_size += ::protobuf::rt::enum_size(1, *value);
        };
        for value in self.get.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.seek.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.put.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.delete.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.cmd_type {
            try!(os.write_enum(1, v.value()));
        };
        if let Some(v) = self.get.as_ref() {
            try!(os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.seek.as_ref() {
            try!(os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.put.as_ref() {
            try!(os.write_tag(4, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.delete.as_ref() {
            try!(os.write_tag(5, ::protobuf::wire_format::WireTypeLengthDelimited));
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

    fn descriptor_static(_: ::std::option::Option<Request>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_enum_accessor(
                    "cmd_type",
                    Request::has_cmd_type,
                    Request::get_cmd_type,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "get",
                    Request::has_get,
                    Request::get_get,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "seek",
                    Request::has_seek,
                    Request::get_seek,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "put",
                    Request::has_put,
                    Request::get_put,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "delete",
                    Request::has_delete,
                    Request::get_delete,
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
        self.clear_cmd_type();
        self.clear_get();
        self.clear_seek();
        self.clear_put();
        self.clear_delete();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for Request {
    fn eq(&self, other: &Request) -> bool {
        self.cmd_type == other.cmd_type &&
        self.get == other.get &&
        self.seek == other.seek &&
        self.put == other.put &&
        self.delete == other.delete &&
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
    cmd_type: ::std::option::Option<CommandType>,
    get: ::protobuf::SingularPtrField<GetResponse>,
    seek: ::protobuf::SingularPtrField<SeekResponse>,
    put: ::protobuf::SingularPtrField<PutResponse>,
    delete: ::protobuf::SingularPtrField<DeleteResponse>,
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
                    cmd_type: ::std::option::Option::None,
                    get: ::protobuf::SingularPtrField::none(),
                    seek: ::protobuf::SingularPtrField::none(),
                    put: ::protobuf::SingularPtrField::none(),
                    delete: ::protobuf::SingularPtrField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional .raft_cmdpb.CommandType cmd_type = 1;

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

    // optional .raft_cmdpb.GetResponse get = 2;

    pub fn clear_get(&mut self) {
        self.get.clear();
    }

    pub fn has_get(&self) -> bool {
        self.get.is_some()
    }

    // Param is passed by value, moved
    pub fn set_get(&mut self, v: GetResponse) {
        self.get = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_get<'a>(&'a mut self) -> &'a mut GetResponse {
        if self.get.is_none() {
            self.get.set_default();
        };
        self.get.as_mut().unwrap()
    }

    // Take field
    pub fn take_get(&mut self) -> GetResponse {
        self.get.take().unwrap_or_else(|| GetResponse::new())
    }

    pub fn get_get<'a>(&'a self) -> &'a GetResponse {
        self.get.as_ref().unwrap_or_else(|| GetResponse::default_instance())
    }

    // optional .raft_cmdpb.SeekResponse seek = 3;

    pub fn clear_seek(&mut self) {
        self.seek.clear();
    }

    pub fn has_seek(&self) -> bool {
        self.seek.is_some()
    }

    // Param is passed by value, moved
    pub fn set_seek(&mut self, v: SeekResponse) {
        self.seek = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_seek<'a>(&'a mut self) -> &'a mut SeekResponse {
        if self.seek.is_none() {
            self.seek.set_default();
        };
        self.seek.as_mut().unwrap()
    }

    // Take field
    pub fn take_seek(&mut self) -> SeekResponse {
        self.seek.take().unwrap_or_else(|| SeekResponse::new())
    }

    pub fn get_seek<'a>(&'a self) -> &'a SeekResponse {
        self.seek.as_ref().unwrap_or_else(|| SeekResponse::default_instance())
    }

    // optional .raft_cmdpb.PutResponse put = 4;

    pub fn clear_put(&mut self) {
        self.put.clear();
    }

    pub fn has_put(&self) -> bool {
        self.put.is_some()
    }

    // Param is passed by value, moved
    pub fn set_put(&mut self, v: PutResponse) {
        self.put = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_put<'a>(&'a mut self) -> &'a mut PutResponse {
        if self.put.is_none() {
            self.put.set_default();
        };
        self.put.as_mut().unwrap()
    }

    // Take field
    pub fn take_put(&mut self) -> PutResponse {
        self.put.take().unwrap_or_else(|| PutResponse::new())
    }

    pub fn get_put<'a>(&'a self) -> &'a PutResponse {
        self.put.as_ref().unwrap_or_else(|| PutResponse::default_instance())
    }

    // optional .raft_cmdpb.DeleteResponse delete = 5;

    pub fn clear_delete(&mut self) {
        self.delete.clear();
    }

    pub fn has_delete(&self) -> bool {
        self.delete.is_some()
    }

    // Param is passed by value, moved
    pub fn set_delete(&mut self, v: DeleteResponse) {
        self.delete = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_delete<'a>(&'a mut self) -> &'a mut DeleteResponse {
        if self.delete.is_none() {
            self.delete.set_default();
        };
        self.delete.as_mut().unwrap()
    }

    // Take field
    pub fn take_delete(&mut self) -> DeleteResponse {
        self.delete.take().unwrap_or_else(|| DeleteResponse::new())
    }

    pub fn get_delete<'a>(&'a self) -> &'a DeleteResponse {
        self.delete.as_ref().unwrap_or_else(|| DeleteResponse::default_instance())
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
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    let tmp = try!(is.read_enum());
                    self.cmd_type = ::std::option::Option::Some(tmp);
                },
                2 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.get));
                },
                3 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.seek));
                },
                4 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.put));
                },
                5 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.delete));
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
        for value in self.cmd_type.iter() {
            my_size += ::protobuf::rt::enum_size(1, *value);
        };
        for value in self.get.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.seek.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.put.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.delete.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.cmd_type {
            try!(os.write_enum(1, v.value()));
        };
        if let Some(v) = self.get.as_ref() {
            try!(os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.seek.as_ref() {
            try!(os.write_tag(3, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.put.as_ref() {
            try!(os.write_tag(4, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.delete.as_ref() {
            try!(os.write_tag(5, ::protobuf::wire_format::WireTypeLengthDelimited));
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

    fn descriptor_static(_: ::std::option::Option<Response>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_enum_accessor(
                    "cmd_type",
                    Response::has_cmd_type,
                    Response::get_cmd_type,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "get",
                    Response::has_get,
                    Response::get_get,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "seek",
                    Response::has_seek,
                    Response::get_seek,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "put",
                    Response::has_put,
                    Response::get_put,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "delete",
                    Response::has_delete,
                    Response::get_delete,
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
        self.clear_cmd_type();
        self.clear_get();
        self.clear_seek();
        self.clear_put();
        self.clear_delete();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for Response {
    fn eq(&self, other: &Response) -> bool {
        self.cmd_type == other.cmd_type &&
        self.get == other.get &&
        self.seek == other.seek &&
        self.put == other.put &&
        self.delete == other.delete &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for Response {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct ChangePeerRequest {
    // message fields
    change_type: ::std::option::Option<super::raftpb::ConfChangeType>,
    peer: ::protobuf::SingularPtrField<super::metapb::Peer>,
    update_peers: ::protobuf::RepeatedField<super::metapb::Peer>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for ChangePeerRequest {}

impl ChangePeerRequest {
    pub fn new() -> ChangePeerRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static ChangePeerRequest {
        static mut instance: ::protobuf::lazy::Lazy<ChangePeerRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ChangePeerRequest,
        };
        unsafe {
            instance.get(|| {
                ChangePeerRequest {
                    change_type: ::std::option::Option::None,
                    peer: ::protobuf::SingularPtrField::none(),
                    update_peers: ::protobuf::RepeatedField::new(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional .raftpb.ConfChangeType change_type = 1;

    pub fn clear_change_type(&mut self) {
        self.change_type = ::std::option::Option::None;
    }

    pub fn has_change_type(&self) -> bool {
        self.change_type.is_some()
    }

    // Param is passed by value, moved
    pub fn set_change_type(&mut self, v: super::raftpb::ConfChangeType) {
        self.change_type = ::std::option::Option::Some(v);
    }

    pub fn get_change_type<'a>(&self) -> super::raftpb::ConfChangeType {
        self.change_type.unwrap_or(super::raftpb::ConfChangeType::ConfChangeAddNode)
    }

    // optional .metapb.Peer peer = 2;

    pub fn clear_peer(&mut self) {
        self.peer.clear();
    }

    pub fn has_peer(&self) -> bool {
        self.peer.is_some()
    }

    // Param is passed by value, moved
    pub fn set_peer(&mut self, v: super::metapb::Peer) {
        self.peer = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_peer<'a>(&'a mut self) -> &'a mut super::metapb::Peer {
        if self.peer.is_none() {
            self.peer.set_default();
        };
        self.peer.as_mut().unwrap()
    }

    // Take field
    pub fn take_peer(&mut self) -> super::metapb::Peer {
        self.peer.take().unwrap_or_else(|| super::metapb::Peer::new())
    }

    pub fn get_peer<'a>(&'a self) -> &'a super::metapb::Peer {
        self.peer.as_ref().unwrap_or_else(|| super::metapb::Peer::default_instance())
    }

    // repeated .metapb.Peer update_peers = 3;

    pub fn clear_update_peers(&mut self) {
        self.update_peers.clear();
    }

    // Param is passed by value, moved
    pub fn set_update_peers(&mut self, v: ::protobuf::RepeatedField<super::metapb::Peer>) {
        self.update_peers = v;
    }

    // Mutable pointer to the field.
    pub fn mut_update_peers<'a>(&'a mut self) -> &'a mut ::protobuf::RepeatedField<super::metapb::Peer> {
        &mut self.update_peers
    }

    // Take field
    pub fn take_update_peers(&mut self) -> ::protobuf::RepeatedField<super::metapb::Peer> {
        ::std::mem::replace(&mut self.update_peers, ::protobuf::RepeatedField::new())
    }

    pub fn get_update_peers<'a>(&'a self) -> &'a [super::metapb::Peer] {
        &self.update_peers
    }
}

impl ::protobuf::Message for ChangePeerRequest {
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
                    self.change_type = ::std::option::Option::Some(tmp);
                },
                2 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.peer));
                },
                3 => {
                    try!(::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.update_peers));
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
        for value in self.change_type.iter() {
            my_size += ::protobuf::rt::enum_size(1, *value);
        };
        for value in self.peer.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.update_peers.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.change_type {
            try!(os.write_enum(1, v.value()));
        };
        if let Some(v) = self.peer.as_ref() {
            try!(os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        for v in self.update_peers.iter() {
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

    fn get_unknown_fields<'s>(&'s self) -> &'s ::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields<'s>(&'s mut self) -> &'s mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<ChangePeerRequest>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for ChangePeerRequest {
    fn new() -> ChangePeerRequest {
        ChangePeerRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<ChangePeerRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_enum_accessor(
                    "change_type",
                    ChangePeerRequest::has_change_type,
                    ChangePeerRequest::get_change_type,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "peer",
                    ChangePeerRequest::has_peer,
                    ChangePeerRequest::get_peer,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_message_accessor(
                    "update_peers",
                    ChangePeerRequest::get_update_peers,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<ChangePeerRequest>(
                    "ChangePeerRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for ChangePeerRequest {
    fn clear(&mut self) {
        self.clear_change_type();
        self.clear_peer();
        self.clear_update_peers();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for ChangePeerRequest {
    fn eq(&self, other: &ChangePeerRequest) -> bool {
        self.change_type == other.change_type &&
        self.peer == other.peer &&
        self.update_peers == other.update_peers &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for ChangePeerRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct ChangePeerResponse {
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for ChangePeerResponse {}

impl ChangePeerResponse {
    pub fn new() -> ChangePeerResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static ChangePeerResponse {
        static mut instance: ::protobuf::lazy::Lazy<ChangePeerResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ChangePeerResponse,
        };
        unsafe {
            instance.get(|| {
                ChangePeerResponse {
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }
}

impl ::protobuf::Message for ChangePeerResponse {
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

    fn get_unknown_fields<'s>(&'s self) -> &'s ::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields<'s>(&'s mut self) -> &'s mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<ChangePeerResponse>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for ChangePeerResponse {
    fn new() -> ChangePeerResponse {
        ChangePeerResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<ChangePeerResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let fields = ::std::vec::Vec::new();
                ::protobuf::reflect::MessageDescriptor::new::<ChangePeerResponse>(
                    "ChangePeerResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for ChangePeerResponse {
    fn clear(&mut self) {
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for ChangePeerResponse {
    fn eq(&self, other: &ChangePeerResponse) -> bool {
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for ChangePeerResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct SplitRequest {
    // message fields
    split_key: ::protobuf::SingularField<::std::vec::Vec<u8>>,
    new_region_id: ::std::option::Option<u64>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for SplitRequest {}

impl SplitRequest {
    pub fn new() -> SplitRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static SplitRequest {
        static mut instance: ::protobuf::lazy::Lazy<SplitRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const SplitRequest,
        };
        unsafe {
            instance.get(|| {
                SplitRequest {
                    split_key: ::protobuf::SingularField::none(),
                    new_region_id: ::std::option::Option::None,
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional bytes split_key = 1;

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

    // optional uint64 new_region_id = 2;

    pub fn clear_new_region_id(&mut self) {
        self.new_region_id = ::std::option::Option::None;
    }

    pub fn has_new_region_id(&self) -> bool {
        self.new_region_id.is_some()
    }

    // Param is passed by value, moved
    pub fn set_new_region_id(&mut self, v: u64) {
        self.new_region_id = ::std::option::Option::Some(v);
    }

    pub fn get_new_region_id<'a>(&self) -> u64 {
        self.new_region_id.unwrap_or(0)
    }
}

impl ::protobuf::Message for SplitRequest {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    try!(::protobuf::rt::read_singular_bytes_into(wire_type, is, &mut self.split_key));
                },
                2 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    let tmp = try!(is.read_uint64());
                    self.new_region_id = ::std::option::Option::Some(tmp);
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
        for value in self.split_key.iter() {
            my_size += ::protobuf::rt::bytes_size(1, &value);
        };
        for value in self.new_region_id.iter() {
            my_size += ::protobuf::rt::value_size(2, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.split_key.as_ref() {
            try!(os.write_bytes(1, &v));
        };
        if let Some(v) = self.new_region_id {
            try!(os.write_uint64(2, v));
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
        ::std::any::TypeId::of::<SplitRequest>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for SplitRequest {
    fn new() -> SplitRequest {
        SplitRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<SplitRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_bytes_accessor(
                    "split_key",
                    SplitRequest::has_split_key,
                    SplitRequest::get_split_key,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_u64_accessor(
                    "new_region_id",
                    SplitRequest::has_new_region_id,
                    SplitRequest::get_new_region_id,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<SplitRequest>(
                    "SplitRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for SplitRequest {
    fn clear(&mut self) {
        self.clear_split_key();
        self.clear_new_region_id();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for SplitRequest {
    fn eq(&self, other: &SplitRequest) -> bool {
        self.split_key == other.split_key &&
        self.new_region_id == other.new_region_id &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for SplitRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct SplitResponse {
    // message fields
    left: ::protobuf::SingularPtrField<super::metapb::Region>,
    right: ::protobuf::SingularPtrField<super::metapb::Region>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for SplitResponse {}

impl SplitResponse {
    pub fn new() -> SplitResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static SplitResponse {
        static mut instance: ::protobuf::lazy::Lazy<SplitResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const SplitResponse,
        };
        unsafe {
            instance.get(|| {
                SplitResponse {
                    left: ::protobuf::SingularPtrField::none(),
                    right: ::protobuf::SingularPtrField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional .metapb.Region left = 1;

    pub fn clear_left(&mut self) {
        self.left.clear();
    }

    pub fn has_left(&self) -> bool {
        self.left.is_some()
    }

    // Param is passed by value, moved
    pub fn set_left(&mut self, v: super::metapb::Region) {
        self.left = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_left<'a>(&'a mut self) -> &'a mut super::metapb::Region {
        if self.left.is_none() {
            self.left.set_default();
        };
        self.left.as_mut().unwrap()
    }

    // Take field
    pub fn take_left(&mut self) -> super::metapb::Region {
        self.left.take().unwrap_or_else(|| super::metapb::Region::new())
    }

    pub fn get_left<'a>(&'a self) -> &'a super::metapb::Region {
        self.left.as_ref().unwrap_or_else(|| super::metapb::Region::default_instance())
    }

    // optional .metapb.Region right = 2;

    pub fn clear_right(&mut self) {
        self.right.clear();
    }

    pub fn has_right(&self) -> bool {
        self.right.is_some()
    }

    // Param is passed by value, moved
    pub fn set_right(&mut self, v: super::metapb::Region) {
        self.right = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_right<'a>(&'a mut self) -> &'a mut super::metapb::Region {
        if self.right.is_none() {
            self.right.set_default();
        };
        self.right.as_mut().unwrap()
    }

    // Take field
    pub fn take_right(&mut self) -> super::metapb::Region {
        self.right.take().unwrap_or_else(|| super::metapb::Region::new())
    }

    pub fn get_right<'a>(&'a self) -> &'a super::metapb::Region {
        self.right.as_ref().unwrap_or_else(|| super::metapb::Region::default_instance())
    }
}

impl ::protobuf::Message for SplitResponse {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.left));
                },
                2 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.right));
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
        for value in self.left.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.right.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.left.as_ref() {
            try!(os.write_tag(1, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.right.as_ref() {
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
        ::std::any::TypeId::of::<SplitResponse>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for SplitResponse {
    fn new() -> SplitResponse {
        SplitResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<SplitResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "left",
                    SplitResponse::has_left,
                    SplitResponse::get_left,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "right",
                    SplitResponse::has_right,
                    SplitResponse::get_right,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<SplitResponse>(
                    "SplitResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for SplitResponse {
    fn clear(&mut self) {
        self.clear_left();
        self.clear_right();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for SplitResponse {
    fn eq(&self, other: &SplitResponse) -> bool {
        self.left == other.left &&
        self.right == other.right &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for SplitResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct AdminRequest {
    // message fields
    cmd_type: ::std::option::Option<AdminCommandType>,
    change_peer: ::protobuf::SingularPtrField<ChangePeerRequest>,
    split: ::protobuf::SingularPtrField<SplitRequest>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for AdminRequest {}

impl AdminRequest {
    pub fn new() -> AdminRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static AdminRequest {
        static mut instance: ::protobuf::lazy::Lazy<AdminRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const AdminRequest,
        };
        unsafe {
            instance.get(|| {
                AdminRequest {
                    cmd_type: ::std::option::Option::None,
                    change_peer: ::protobuf::SingularPtrField::none(),
                    split: ::protobuf::SingularPtrField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional .raft_cmdpb.AdminCommandType cmd_type = 1;

    pub fn clear_cmd_type(&mut self) {
        self.cmd_type = ::std::option::Option::None;
    }

    pub fn has_cmd_type(&self) -> bool {
        self.cmd_type.is_some()
    }

    // Param is passed by value, moved
    pub fn set_cmd_type(&mut self, v: AdminCommandType) {
        self.cmd_type = ::std::option::Option::Some(v);
    }

    pub fn get_cmd_type<'a>(&self) -> AdminCommandType {
        self.cmd_type.unwrap_or(AdminCommandType::InvalidAdmin)
    }

    // optional .raft_cmdpb.ChangePeerRequest change_peer = 2;

    pub fn clear_change_peer(&mut self) {
        self.change_peer.clear();
    }

    pub fn has_change_peer(&self) -> bool {
        self.change_peer.is_some()
    }

    // Param is passed by value, moved
    pub fn set_change_peer(&mut self, v: ChangePeerRequest) {
        self.change_peer = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_change_peer<'a>(&'a mut self) -> &'a mut ChangePeerRequest {
        if self.change_peer.is_none() {
            self.change_peer.set_default();
        };
        self.change_peer.as_mut().unwrap()
    }

    // Take field
    pub fn take_change_peer(&mut self) -> ChangePeerRequest {
        self.change_peer.take().unwrap_or_else(|| ChangePeerRequest::new())
    }

    pub fn get_change_peer<'a>(&'a self) -> &'a ChangePeerRequest {
        self.change_peer.as_ref().unwrap_or_else(|| ChangePeerRequest::default_instance())
    }

    // optional .raft_cmdpb.SplitRequest split = 3;

    pub fn clear_split(&mut self) {
        self.split.clear();
    }

    pub fn has_split(&self) -> bool {
        self.split.is_some()
    }

    // Param is passed by value, moved
    pub fn set_split(&mut self, v: SplitRequest) {
        self.split = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_split<'a>(&'a mut self) -> &'a mut SplitRequest {
        if self.split.is_none() {
            self.split.set_default();
        };
        self.split.as_mut().unwrap()
    }

    // Take field
    pub fn take_split(&mut self) -> SplitRequest {
        self.split.take().unwrap_or_else(|| SplitRequest::new())
    }

    pub fn get_split<'a>(&'a self) -> &'a SplitRequest {
        self.split.as_ref().unwrap_or_else(|| SplitRequest::default_instance())
    }
}

impl ::protobuf::Message for AdminRequest {
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
                    self.cmd_type = ::std::option::Option::Some(tmp);
                },
                2 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.change_peer));
                },
                3 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.split));
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
        for value in self.cmd_type.iter() {
            my_size += ::protobuf::rt::enum_size(1, *value);
        };
        for value in self.change_peer.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.split.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.cmd_type {
            try!(os.write_enum(1, v.value()));
        };
        if let Some(v) = self.change_peer.as_ref() {
            try!(os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.split.as_ref() {
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

    fn get_unknown_fields<'s>(&'s self) -> &'s ::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields<'s>(&'s mut self) -> &'s mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<AdminRequest>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for AdminRequest {
    fn new() -> AdminRequest {
        AdminRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<AdminRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_enum_accessor(
                    "cmd_type",
                    AdminRequest::has_cmd_type,
                    AdminRequest::get_cmd_type,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "change_peer",
                    AdminRequest::has_change_peer,
                    AdminRequest::get_change_peer,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "split",
                    AdminRequest::has_split,
                    AdminRequest::get_split,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<AdminRequest>(
                    "AdminRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for AdminRequest {
    fn clear(&mut self) {
        self.clear_cmd_type();
        self.clear_change_peer();
        self.clear_split();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for AdminRequest {
    fn eq(&self, other: &AdminRequest) -> bool {
        self.cmd_type == other.cmd_type &&
        self.change_peer == other.change_peer &&
        self.split == other.split &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for AdminRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct AdminResponse {
    // message fields
    cmd_type: ::std::option::Option<AdminCommandType>,
    change_peer: ::protobuf::SingularPtrField<ChangePeerResponse>,
    split: ::protobuf::SingularPtrField<SplitResponse>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for AdminResponse {}

impl AdminResponse {
    pub fn new() -> AdminResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static AdminResponse {
        static mut instance: ::protobuf::lazy::Lazy<AdminResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const AdminResponse,
        };
        unsafe {
            instance.get(|| {
                AdminResponse {
                    cmd_type: ::std::option::Option::None,
                    change_peer: ::protobuf::SingularPtrField::none(),
                    split: ::protobuf::SingularPtrField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional .raft_cmdpb.AdminCommandType cmd_type = 1;

    pub fn clear_cmd_type(&mut self) {
        self.cmd_type = ::std::option::Option::None;
    }

    pub fn has_cmd_type(&self) -> bool {
        self.cmd_type.is_some()
    }

    // Param is passed by value, moved
    pub fn set_cmd_type(&mut self, v: AdminCommandType) {
        self.cmd_type = ::std::option::Option::Some(v);
    }

    pub fn get_cmd_type<'a>(&self) -> AdminCommandType {
        self.cmd_type.unwrap_or(AdminCommandType::InvalidAdmin)
    }

    // optional .raft_cmdpb.ChangePeerResponse change_peer = 2;

    pub fn clear_change_peer(&mut self) {
        self.change_peer.clear();
    }

    pub fn has_change_peer(&self) -> bool {
        self.change_peer.is_some()
    }

    // Param is passed by value, moved
    pub fn set_change_peer(&mut self, v: ChangePeerResponse) {
        self.change_peer = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_change_peer<'a>(&'a mut self) -> &'a mut ChangePeerResponse {
        if self.change_peer.is_none() {
            self.change_peer.set_default();
        };
        self.change_peer.as_mut().unwrap()
    }

    // Take field
    pub fn take_change_peer(&mut self) -> ChangePeerResponse {
        self.change_peer.take().unwrap_or_else(|| ChangePeerResponse::new())
    }

    pub fn get_change_peer<'a>(&'a self) -> &'a ChangePeerResponse {
        self.change_peer.as_ref().unwrap_or_else(|| ChangePeerResponse::default_instance())
    }

    // optional .raft_cmdpb.SplitResponse split = 3;

    pub fn clear_split(&mut self) {
        self.split.clear();
    }

    pub fn has_split(&self) -> bool {
        self.split.is_some()
    }

    // Param is passed by value, moved
    pub fn set_split(&mut self, v: SplitResponse) {
        self.split = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_split<'a>(&'a mut self) -> &'a mut SplitResponse {
        if self.split.is_none() {
            self.split.set_default();
        };
        self.split.as_mut().unwrap()
    }

    // Take field
    pub fn take_split(&mut self) -> SplitResponse {
        self.split.take().unwrap_or_else(|| SplitResponse::new())
    }

    pub fn get_split<'a>(&'a self) -> &'a SplitResponse {
        self.split.as_ref().unwrap_or_else(|| SplitResponse::default_instance())
    }
}

impl ::protobuf::Message for AdminResponse {
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
                    self.cmd_type = ::std::option::Option::Some(tmp);
                },
                2 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.change_peer));
                },
                3 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.split));
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
        for value in self.cmd_type.iter() {
            my_size += ::protobuf::rt::enum_size(1, *value);
        };
        for value in self.change_peer.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.split.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.cmd_type {
            try!(os.write_enum(1, v.value()));
        };
        if let Some(v) = self.change_peer.as_ref() {
            try!(os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.split.as_ref() {
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

    fn get_unknown_fields<'s>(&'s self) -> &'s ::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields<'s>(&'s mut self) -> &'s mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<AdminResponse>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for AdminResponse {
    fn new() -> AdminResponse {
        AdminResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<AdminResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_enum_accessor(
                    "cmd_type",
                    AdminResponse::has_cmd_type,
                    AdminResponse::get_cmd_type,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "change_peer",
                    AdminResponse::has_change_peer,
                    AdminResponse::get_change_peer,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "split",
                    AdminResponse::has_split,
                    AdminResponse::get_split,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<AdminResponse>(
                    "AdminResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for AdminResponse {
    fn clear(&mut self) {
        self.clear_cmd_type();
        self.clear_change_peer();
        self.clear_split();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for AdminResponse {
    fn eq(&self, other: &AdminResponse) -> bool {
        self.cmd_type == other.cmd_type &&
        self.change_peer == other.change_peer &&
        self.split == other.split &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for AdminResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct RaftRequestHeader {
    // message fields
    region_id: ::std::option::Option<u64>,
    peer: ::protobuf::SingularPtrField<super::metapb::Peer>,
    read_quorum: ::std::option::Option<bool>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for RaftRequestHeader {}

impl RaftRequestHeader {
    pub fn new() -> RaftRequestHeader {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static RaftRequestHeader {
        static mut instance: ::protobuf::lazy::Lazy<RaftRequestHeader> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const RaftRequestHeader,
        };
        unsafe {
            instance.get(|| {
                RaftRequestHeader {
                    region_id: ::std::option::Option::None,
                    peer: ::protobuf::SingularPtrField::none(),
                    read_quorum: ::std::option::Option::None,
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional uint64 region_id = 1;

    pub fn clear_region_id(&mut self) {
        self.region_id = ::std::option::Option::None;
    }

    pub fn has_region_id(&self) -> bool {
        self.region_id.is_some()
    }

    // Param is passed by value, moved
    pub fn set_region_id(&mut self, v: u64) {
        self.region_id = ::std::option::Option::Some(v);
    }

    pub fn get_region_id<'a>(&self) -> u64 {
        self.region_id.unwrap_or(0)
    }

    // optional .metapb.Peer peer = 2;

    pub fn clear_peer(&mut self) {
        self.peer.clear();
    }

    pub fn has_peer(&self) -> bool {
        self.peer.is_some()
    }

    // Param is passed by value, moved
    pub fn set_peer(&mut self, v: super::metapb::Peer) {
        self.peer = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_peer<'a>(&'a mut self) -> &'a mut super::metapb::Peer {
        if self.peer.is_none() {
            self.peer.set_default();
        };
        self.peer.as_mut().unwrap()
    }

    // Take field
    pub fn take_peer(&mut self) -> super::metapb::Peer {
        self.peer.take().unwrap_or_else(|| super::metapb::Peer::new())
    }

    pub fn get_peer<'a>(&'a self) -> &'a super::metapb::Peer {
        self.peer.as_ref().unwrap_or_else(|| super::metapb::Peer::default_instance())
    }

    // optional bool read_quorum = 3;

    pub fn clear_read_quorum(&mut self) {
        self.read_quorum = ::std::option::Option::None;
    }

    pub fn has_read_quorum(&self) -> bool {
        self.read_quorum.is_some()
    }

    // Param is passed by value, moved
    pub fn set_read_quorum(&mut self, v: bool) {
        self.read_quorum = ::std::option::Option::Some(v);
    }

    pub fn get_read_quorum<'a>(&self) -> bool {
        self.read_quorum.unwrap_or(false)
    }
}

impl ::protobuf::Message for RaftRequestHeader {
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
                    let tmp = try!(is.read_uint64());
                    self.region_id = ::std::option::Option::Some(tmp);
                },
                2 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.peer));
                },
                3 => {
                    if wire_type != ::protobuf::wire_format::WireTypeVarint {
                        return ::std::result::Result::Err(::protobuf::rt::unexpected_wire_type(wire_type));
                    };
                    let tmp = try!(is.read_bool());
                    self.read_quorum = ::std::option::Option::Some(tmp);
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
        for value in self.region_id.iter() {
            my_size += ::protobuf::rt::value_size(1, *value, ::protobuf::wire_format::WireTypeVarint);
        };
        for value in self.peer.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        if self.read_quorum.is_some() {
            my_size += 2;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.region_id {
            try!(os.write_uint64(1, v));
        };
        if let Some(v) = self.peer.as_ref() {
            try!(os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.read_quorum {
            try!(os.write_bool(3, v));
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
        ::std::any::TypeId::of::<RaftRequestHeader>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for RaftRequestHeader {
    fn new() -> RaftRequestHeader {
        RaftRequestHeader::new()
    }

    fn descriptor_static(_: ::std::option::Option<RaftRequestHeader>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_u64_accessor(
                    "region_id",
                    RaftRequestHeader::has_region_id,
                    RaftRequestHeader::get_region_id,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "peer",
                    RaftRequestHeader::has_peer,
                    RaftRequestHeader::get_peer,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_bool_accessor(
                    "read_quorum",
                    RaftRequestHeader::has_read_quorum,
                    RaftRequestHeader::get_read_quorum,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<RaftRequestHeader>(
                    "RaftRequestHeader",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for RaftRequestHeader {
    fn clear(&mut self) {
        self.clear_region_id();
        self.clear_peer();
        self.clear_read_quorum();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for RaftRequestHeader {
    fn eq(&self, other: &RaftRequestHeader) -> bool {
        self.region_id == other.region_id &&
        self.peer == other.peer &&
        self.read_quorum == other.read_quorum &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for RaftRequestHeader {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct RaftResponseHeader {
    // message fields
    error: ::protobuf::SingularPtrField<super::errorpb::Error>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for RaftResponseHeader {}

impl RaftResponseHeader {
    pub fn new() -> RaftResponseHeader {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static RaftResponseHeader {
        static mut instance: ::protobuf::lazy::Lazy<RaftResponseHeader> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const RaftResponseHeader,
        };
        unsafe {
            instance.get(|| {
                RaftResponseHeader {
                    error: ::protobuf::SingularPtrField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional .errorpb.Error error = 1;

    pub fn clear_error(&mut self) {
        self.error.clear();
    }

    pub fn has_error(&self) -> bool {
        self.error.is_some()
    }

    // Param is passed by value, moved
    pub fn set_error(&mut self, v: super::errorpb::Error) {
        self.error = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_error<'a>(&'a mut self) -> &'a mut super::errorpb::Error {
        if self.error.is_none() {
            self.error.set_default();
        };
        self.error.as_mut().unwrap()
    }

    // Take field
    pub fn take_error(&mut self) -> super::errorpb::Error {
        self.error.take().unwrap_or_else(|| super::errorpb::Error::new())
    }

    pub fn get_error<'a>(&'a self) -> &'a super::errorpb::Error {
        self.error.as_ref().unwrap_or_else(|| super::errorpb::Error::default_instance())
    }
}

impl ::protobuf::Message for RaftResponseHeader {
    fn is_initialized(&self) -> bool {
        true
    }

    fn merge_from(&mut self, is: &mut ::protobuf::CodedInputStream) -> ::protobuf::ProtobufResult<()> {
        while !try!(is.eof()) {
            let (field_number, wire_type) = try!(is.read_tag_unpack());
            match field_number {
                1 => {
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
        for value in self.error.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        my_size += ::protobuf::rt::unknown_fields_size(self.get_unknown_fields());
        self.cached_size.set(my_size);
        my_size
    }

    fn write_to_with_cached_sizes(&self, os: &mut ::protobuf::CodedOutputStream) -> ::protobuf::ProtobufResult<()> {
        if let Some(v) = self.error.as_ref() {
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
        ::std::any::TypeId::of::<RaftResponseHeader>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for RaftResponseHeader {
    fn new() -> RaftResponseHeader {
        RaftResponseHeader::new()
    }

    fn descriptor_static(_: ::std::option::Option<RaftResponseHeader>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "error",
                    RaftResponseHeader::has_error,
                    RaftResponseHeader::get_error,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<RaftResponseHeader>(
                    "RaftResponseHeader",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for RaftResponseHeader {
    fn clear(&mut self) {
        self.clear_error();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for RaftResponseHeader {
    fn eq(&self, other: &RaftResponseHeader) -> bool {
        self.error == other.error &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for RaftResponseHeader {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct RaftCommandRequest {
    // message fields
    header: ::protobuf::SingularPtrField<RaftRequestHeader>,
    requests: ::protobuf::RepeatedField<Request>,
    admin_request: ::protobuf::SingularPtrField<AdminRequest>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for RaftCommandRequest {}

impl RaftCommandRequest {
    pub fn new() -> RaftCommandRequest {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static RaftCommandRequest {
        static mut instance: ::protobuf::lazy::Lazy<RaftCommandRequest> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const RaftCommandRequest,
        };
        unsafe {
            instance.get(|| {
                RaftCommandRequest {
                    header: ::protobuf::SingularPtrField::none(),
                    requests: ::protobuf::RepeatedField::new(),
                    admin_request: ::protobuf::SingularPtrField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional .raft_cmdpb.RaftRequestHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: RaftRequestHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header<'a>(&'a mut self) -> &'a mut RaftRequestHeader {
        if self.header.is_none() {
            self.header.set_default();
        };
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> RaftRequestHeader {
        self.header.take().unwrap_or_else(|| RaftRequestHeader::new())
    }

    pub fn get_header<'a>(&'a self) -> &'a RaftRequestHeader {
        self.header.as_ref().unwrap_or_else(|| RaftRequestHeader::default_instance())
    }

    // repeated .raft_cmdpb.Request requests = 2;

    pub fn clear_requests(&mut self) {
        self.requests.clear();
    }

    // Param is passed by value, moved
    pub fn set_requests(&mut self, v: ::protobuf::RepeatedField<Request>) {
        self.requests = v;
    }

    // Mutable pointer to the field.
    pub fn mut_requests<'a>(&'a mut self) -> &'a mut ::protobuf::RepeatedField<Request> {
        &mut self.requests
    }

    // Take field
    pub fn take_requests(&mut self) -> ::protobuf::RepeatedField<Request> {
        ::std::mem::replace(&mut self.requests, ::protobuf::RepeatedField::new())
    }

    pub fn get_requests<'a>(&'a self) -> &'a [Request] {
        &self.requests
    }

    // optional .raft_cmdpb.AdminRequest admin_request = 3;

    pub fn clear_admin_request(&mut self) {
        self.admin_request.clear();
    }

    pub fn has_admin_request(&self) -> bool {
        self.admin_request.is_some()
    }

    // Param is passed by value, moved
    pub fn set_admin_request(&mut self, v: AdminRequest) {
        self.admin_request = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_admin_request<'a>(&'a mut self) -> &'a mut AdminRequest {
        if self.admin_request.is_none() {
            self.admin_request.set_default();
        };
        self.admin_request.as_mut().unwrap()
    }

    // Take field
    pub fn take_admin_request(&mut self) -> AdminRequest {
        self.admin_request.take().unwrap_or_else(|| AdminRequest::new())
    }

    pub fn get_admin_request<'a>(&'a self) -> &'a AdminRequest {
        self.admin_request.as_ref().unwrap_or_else(|| AdminRequest::default_instance())
    }
}

impl ::protobuf::Message for RaftCommandRequest {
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
                    try!(::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.requests));
                },
                3 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.admin_request));
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
        for value in self.requests.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.admin_request.iter() {
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
        for v in self.requests.iter() {
            try!(os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.admin_request.as_ref() {
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

    fn get_unknown_fields<'s>(&'s self) -> &'s ::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields<'s>(&'s mut self) -> &'s mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<RaftCommandRequest>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for RaftCommandRequest {
    fn new() -> RaftCommandRequest {
        RaftCommandRequest::new()
    }

    fn descriptor_static(_: ::std::option::Option<RaftCommandRequest>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "header",
                    RaftCommandRequest::has_header,
                    RaftCommandRequest::get_header,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_message_accessor(
                    "requests",
                    RaftCommandRequest::get_requests,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "admin_request",
                    RaftCommandRequest::has_admin_request,
                    RaftCommandRequest::get_admin_request,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<RaftCommandRequest>(
                    "RaftCommandRequest",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for RaftCommandRequest {
    fn clear(&mut self) {
        self.clear_header();
        self.clear_requests();
        self.clear_admin_request();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for RaftCommandRequest {
    fn eq(&self, other: &RaftCommandRequest) -> bool {
        self.header == other.header &&
        self.requests == other.requests &&
        self.admin_request == other.admin_request &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for RaftCommandRequest {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,Default)]
pub struct RaftCommandResponse {
    // message fields
    header: ::protobuf::SingularPtrField<RaftResponseHeader>,
    responses: ::protobuf::RepeatedField<Response>,
    admin_response: ::protobuf::SingularPtrField<AdminResponse>,
    // special fields
    unknown_fields: ::protobuf::UnknownFields,
    cached_size: ::std::cell::Cell<u32>,
}

// see codegen.rs for the explanation why impl Sync explicitly
unsafe impl ::std::marker::Sync for RaftCommandResponse {}

impl RaftCommandResponse {
    pub fn new() -> RaftCommandResponse {
        ::std::default::Default::default()
    }

    pub fn default_instance() -> &'static RaftCommandResponse {
        static mut instance: ::protobuf::lazy::Lazy<RaftCommandResponse> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const RaftCommandResponse,
        };
        unsafe {
            instance.get(|| {
                RaftCommandResponse {
                    header: ::protobuf::SingularPtrField::none(),
                    responses: ::protobuf::RepeatedField::new(),
                    admin_response: ::protobuf::SingularPtrField::none(),
                    unknown_fields: ::protobuf::UnknownFields::new(),
                    cached_size: ::std::cell::Cell::new(0),
                }
            })
        }
    }

    // optional .raft_cmdpb.RaftResponseHeader header = 1;

    pub fn clear_header(&mut self) {
        self.header.clear();
    }

    pub fn has_header(&self) -> bool {
        self.header.is_some()
    }

    // Param is passed by value, moved
    pub fn set_header(&mut self, v: RaftResponseHeader) {
        self.header = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_header<'a>(&'a mut self) -> &'a mut RaftResponseHeader {
        if self.header.is_none() {
            self.header.set_default();
        };
        self.header.as_mut().unwrap()
    }

    // Take field
    pub fn take_header(&mut self) -> RaftResponseHeader {
        self.header.take().unwrap_or_else(|| RaftResponseHeader::new())
    }

    pub fn get_header<'a>(&'a self) -> &'a RaftResponseHeader {
        self.header.as_ref().unwrap_or_else(|| RaftResponseHeader::default_instance())
    }

    // repeated .raft_cmdpb.Response responses = 2;

    pub fn clear_responses(&mut self) {
        self.responses.clear();
    }

    // Param is passed by value, moved
    pub fn set_responses(&mut self, v: ::protobuf::RepeatedField<Response>) {
        self.responses = v;
    }

    // Mutable pointer to the field.
    pub fn mut_responses<'a>(&'a mut self) -> &'a mut ::protobuf::RepeatedField<Response> {
        &mut self.responses
    }

    // Take field
    pub fn take_responses(&mut self) -> ::protobuf::RepeatedField<Response> {
        ::std::mem::replace(&mut self.responses, ::protobuf::RepeatedField::new())
    }

    pub fn get_responses<'a>(&'a self) -> &'a [Response] {
        &self.responses
    }

    // optional .raft_cmdpb.AdminResponse admin_response = 3;

    pub fn clear_admin_response(&mut self) {
        self.admin_response.clear();
    }

    pub fn has_admin_response(&self) -> bool {
        self.admin_response.is_some()
    }

    // Param is passed by value, moved
    pub fn set_admin_response(&mut self, v: AdminResponse) {
        self.admin_response = ::protobuf::SingularPtrField::some(v);
    }

    // Mutable pointer to the field.
    // If field is not initialized, it is initialized with default value first.
    pub fn mut_admin_response<'a>(&'a mut self) -> &'a mut AdminResponse {
        if self.admin_response.is_none() {
            self.admin_response.set_default();
        };
        self.admin_response.as_mut().unwrap()
    }

    // Take field
    pub fn take_admin_response(&mut self) -> AdminResponse {
        self.admin_response.take().unwrap_or_else(|| AdminResponse::new())
    }

    pub fn get_admin_response<'a>(&'a self) -> &'a AdminResponse {
        self.admin_response.as_ref().unwrap_or_else(|| AdminResponse::default_instance())
    }
}

impl ::protobuf::Message for RaftCommandResponse {
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
                    try!(::protobuf::rt::read_repeated_message_into(wire_type, is, &mut self.responses));
                },
                3 => {
                    try!(::protobuf::rt::read_singular_message_into(wire_type, is, &mut self.admin_response));
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
        for value in self.responses.iter() {
            let len = value.compute_size();
            my_size += 1 + ::protobuf::rt::compute_raw_varint32_size(len) + len;
        };
        for value in self.admin_response.iter() {
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
        for v in self.responses.iter() {
            try!(os.write_tag(2, ::protobuf::wire_format::WireTypeLengthDelimited));
            try!(os.write_raw_varint32(v.get_cached_size()));
            try!(v.write_to_with_cached_sizes(os));
        };
        if let Some(v) = self.admin_response.as_ref() {
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

    fn get_unknown_fields<'s>(&'s self) -> &'s ::protobuf::UnknownFields {
        &self.unknown_fields
    }

    fn mut_unknown_fields<'s>(&'s mut self) -> &'s mut ::protobuf::UnknownFields {
        &mut self.unknown_fields
    }

    fn type_id(&self) -> ::std::any::TypeId {
        ::std::any::TypeId::of::<RaftCommandResponse>()
    }

    fn as_any(&self) -> &::std::any::Any {
        self as &::std::any::Any
    }

    fn descriptor(&self) -> &'static ::protobuf::reflect::MessageDescriptor {
        ::protobuf::MessageStatic::descriptor_static(None::<Self>)
    }
}

impl ::protobuf::MessageStatic for RaftCommandResponse {
    fn new() -> RaftCommandResponse {
        RaftCommandResponse::new()
    }

    fn descriptor_static(_: ::std::option::Option<RaftCommandResponse>) -> &'static ::protobuf::reflect::MessageDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::MessageDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::MessageDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                let mut fields = ::std::vec::Vec::new();
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "header",
                    RaftCommandResponse::has_header,
                    RaftCommandResponse::get_header,
                ));
                fields.push(::protobuf::reflect::accessor::make_repeated_message_accessor(
                    "responses",
                    RaftCommandResponse::get_responses,
                ));
                fields.push(::protobuf::reflect::accessor::make_singular_message_accessor(
                    "admin_response",
                    RaftCommandResponse::has_admin_response,
                    RaftCommandResponse::get_admin_response,
                ));
                ::protobuf::reflect::MessageDescriptor::new::<RaftCommandResponse>(
                    "RaftCommandResponse",
                    fields,
                    file_descriptor_proto()
                )
            })
        }
    }
}

impl ::protobuf::Clear for RaftCommandResponse {
    fn clear(&mut self) {
        self.clear_header();
        self.clear_responses();
        self.clear_admin_response();
        self.unknown_fields.clear();
    }
}

impl ::std::cmp::PartialEq for RaftCommandResponse {
    fn eq(&self, other: &RaftCommandResponse) -> bool {
        self.header == other.header &&
        self.responses == other.responses &&
        self.admin_response == other.admin_response &&
        self.unknown_fields == other.unknown_fields
    }
}

impl ::std::fmt::Debug for RaftCommandResponse {
    fn fmt(&self, f: &mut ::std::fmt::Formatter) -> ::std::fmt::Result {
        ::protobuf::text_format::fmt(self, f)
    }
}

#[derive(Clone,PartialEq,Eq,Debug,Hash)]
pub enum CommandType {
    Invalid = 0,
    Get = 1,
    Seek = 2,
    Put = 3,
    Delete = 4,
}

impl ::protobuf::ProtobufEnum for CommandType {
    fn value(&self) -> i32 {
        *self as i32
    }

    fn from_i32(value: i32) -> ::std::option::Option<CommandType> {
        match value {
            0 => ::std::option::Option::Some(CommandType::Invalid),
            1 => ::std::option::Option::Some(CommandType::Get),
            2 => ::std::option::Option::Some(CommandType::Seek),
            3 => ::std::option::Option::Some(CommandType::Put),
            4 => ::std::option::Option::Some(CommandType::Delete),
            _ => ::std::option::Option::None
        }
    }

    fn values() -> &'static [Self] {
        static values: &'static [CommandType] = &[
            CommandType::Invalid,
            CommandType::Get,
            CommandType::Seek,
            CommandType::Put,
            CommandType::Delete,
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
pub enum AdminCommandType {
    InvalidAdmin = 0,
    ChangePeer = 1,
    Split = 2,
}

impl ::protobuf::ProtobufEnum for AdminCommandType {
    fn value(&self) -> i32 {
        *self as i32
    }

    fn from_i32(value: i32) -> ::std::option::Option<AdminCommandType> {
        match value {
            0 => ::std::option::Option::Some(AdminCommandType::InvalidAdmin),
            1 => ::std::option::Option::Some(AdminCommandType::ChangePeer),
            2 => ::std::option::Option::Some(AdminCommandType::Split),
            _ => ::std::option::Option::None
        }
    }

    fn values() -> &'static [Self] {
        static values: &'static [AdminCommandType] = &[
            AdminCommandType::InvalidAdmin,
            AdminCommandType::ChangePeer,
            AdminCommandType::Split,
        ];
        values
    }

    fn enum_descriptor_static(_: Option<AdminCommandType>) -> &'static ::protobuf::reflect::EnumDescriptor {
        static mut descriptor: ::protobuf::lazy::Lazy<::protobuf::reflect::EnumDescriptor> = ::protobuf::lazy::Lazy {
            lock: ::protobuf::lazy::ONCE_INIT,
            ptr: 0 as *const ::protobuf::reflect::EnumDescriptor,
        };
        unsafe {
            descriptor.get(|| {
                ::protobuf::reflect::EnumDescriptor::new("AdminCommandType", file_descriptor_proto())
            })
        }
    }
}

impl ::std::marker::Copy for AdminCommandType {
}

static file_descriptor_proto_data: &'static [u8] = &[
    0x0a, 0x10, 0x72, 0x61, 0x66, 0x74, 0x5f, 0x63, 0x6d, 0x64, 0x70, 0x62, 0x2e, 0x70, 0x72, 0x6f,
    0x74, 0x6f, 0x12, 0x0a, 0x72, 0x61, 0x66, 0x74, 0x5f, 0x63, 0x6d, 0x64, 0x70, 0x62, 0x1a, 0x0c,
    0x6d, 0x65, 0x74, 0x61, 0x70, 0x62, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x0d, 0x65, 0x72,
    0x72, 0x6f, 0x72, 0x70, 0x62, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x1a, 0x0c, 0x72, 0x61, 0x66,
    0x74, 0x70, 0x62, 0x2e, 0x70, 0x72, 0x6f, 0x74, 0x6f, 0x22, 0x19, 0x0a, 0x0a, 0x47, 0x65, 0x74,
    0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x0b, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01,
    0x20, 0x01, 0x28, 0x0c, 0x22, 0x1c, 0x0a, 0x0b, 0x47, 0x65, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f,
    0x6e, 0x73, 0x65, 0x12, 0x0d, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x01, 0x20, 0x01,
    0x28, 0x0c, 0x22, 0x1a, 0x0a, 0x0b, 0x53, 0x65, 0x65, 0x6b, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73,
    0x74, 0x12, 0x0b, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x22, 0x2a,
    0x0a, 0x0c, 0x53, 0x65, 0x65, 0x6b, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x0b,
    0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x12, 0x0d, 0x0a, 0x05, 0x76,
    0x61, 0x6c, 0x75, 0x65, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0c, 0x22, 0x28, 0x0a, 0x0a, 0x50, 0x75,
    0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x0b, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18,
    0x01, 0x20, 0x01, 0x28, 0x0c, 0x12, 0x0d, 0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65, 0x18, 0x02,
    0x20, 0x01, 0x28, 0x0c, 0x22, 0x0d, 0x0a, 0x0b, 0x50, 0x75, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f,
    0x6e, 0x73, 0x65, 0x22, 0x1c, 0x0a, 0x0d, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x52, 0x65, 0x71,
    0x75, 0x65, 0x73, 0x74, 0x12, 0x0b, 0x0a, 0x03, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28,
    0x0c, 0x22, 0x10, 0x0a, 0x0e, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f,
    0x6e, 0x73, 0x65, 0x22, 0xd0, 0x01, 0x0a, 0x07, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12,
    0x29, 0x0a, 0x08, 0x63, 0x6d, 0x64, 0x5f, 0x74, 0x79, 0x70, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28,
    0x0e, 0x32, 0x17, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x5f, 0x63, 0x6d, 0x64, 0x70, 0x62, 0x2e, 0x43,
    0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x54, 0x79, 0x70, 0x65, 0x12, 0x23, 0x0a, 0x03, 0x67, 0x65,
    0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x16, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x5f, 0x63,
    0x6d, 0x64, 0x70, 0x62, 0x2e, 0x47, 0x65, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12,
    0x25, 0x0a, 0x04, 0x73, 0x65, 0x65, 0x6b, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x17, 0x2e,
    0x72, 0x61, 0x66, 0x74, 0x5f, 0x63, 0x6d, 0x64, 0x70, 0x62, 0x2e, 0x53, 0x65, 0x65, 0x6b, 0x52,
    0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x23, 0x0a, 0x03, 0x70, 0x75, 0x74, 0x18, 0x04, 0x20,
    0x01, 0x28, 0x0b, 0x32, 0x16, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x5f, 0x63, 0x6d, 0x64, 0x70, 0x62,
    0x2e, 0x50, 0x75, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x29, 0x0a, 0x06, 0x64,
    0x65, 0x6c, 0x65, 0x74, 0x65, 0x18, 0x05, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x72, 0x61,
    0x66, 0x74, 0x5f, 0x63, 0x6d, 0x64, 0x70, 0x62, 0x2e, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x52,
    0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x22, 0xd5, 0x01, 0x0a, 0x08, 0x52, 0x65, 0x73, 0x70, 0x6f,
    0x6e, 0x73, 0x65, 0x12, 0x29, 0x0a, 0x08, 0x63, 0x6d, 0x64, 0x5f, 0x74, 0x79, 0x70, 0x65, 0x18,
    0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x17, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x5f, 0x63, 0x6d, 0x64,
    0x70, 0x62, 0x2e, 0x43, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x54, 0x79, 0x70, 0x65, 0x12, 0x24,
    0x0a, 0x03, 0x67, 0x65, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x17, 0x2e, 0x72, 0x61,
    0x66, 0x74, 0x5f, 0x63, 0x6d, 0x64, 0x70, 0x62, 0x2e, 0x47, 0x65, 0x74, 0x52, 0x65, 0x73, 0x70,
    0x6f, 0x6e, 0x73, 0x65, 0x12, 0x26, 0x0a, 0x04, 0x73, 0x65, 0x65, 0x6b, 0x18, 0x03, 0x20, 0x01,
    0x28, 0x0b, 0x32, 0x18, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x5f, 0x63, 0x6d, 0x64, 0x70, 0x62, 0x2e,
    0x53, 0x65, 0x65, 0x6b, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x24, 0x0a, 0x03,
    0x70, 0x75, 0x74, 0x18, 0x04, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x17, 0x2e, 0x72, 0x61, 0x66, 0x74,
    0x5f, 0x63, 0x6d, 0x64, 0x70, 0x62, 0x2e, 0x50, 0x75, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e,
    0x73, 0x65, 0x12, 0x2a, 0x0a, 0x06, 0x64, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x18, 0x05, 0x20, 0x01,
    0x28, 0x0b, 0x32, 0x1a, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x5f, 0x63, 0x6d, 0x64, 0x70, 0x62, 0x2e,
    0x44, 0x65, 0x6c, 0x65, 0x74, 0x65, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x80,
    0x01, 0x0a, 0x11, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x50, 0x65, 0x65, 0x72, 0x52, 0x65, 0x71,
    0x75, 0x65, 0x73, 0x74, 0x12, 0x2b, 0x0a, 0x0b, 0x63, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x5f, 0x74,
    0x79, 0x70, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0e, 0x32, 0x16, 0x2e, 0x72, 0x61, 0x66, 0x74,
    0x70, 0x62, 0x2e, 0x43, 0x6f, 0x6e, 0x66, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x54, 0x79, 0x70,
    0x65, 0x12, 0x1a, 0x0a, 0x04, 0x70, 0x65, 0x65, 0x72, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32,
    0x0c, 0x2e, 0x6d, 0x65, 0x74, 0x61, 0x70, 0x62, 0x2e, 0x50, 0x65, 0x65, 0x72, 0x12, 0x22, 0x0a,
    0x0c, 0x75, 0x70, 0x64, 0x61, 0x74, 0x65, 0x5f, 0x70, 0x65, 0x65, 0x72, 0x73, 0x18, 0x03, 0x20,
    0x03, 0x28, 0x0b, 0x32, 0x0c, 0x2e, 0x6d, 0x65, 0x74, 0x61, 0x70, 0x62, 0x2e, 0x50, 0x65, 0x65,
    0x72, 0x22, 0x14, 0x0a, 0x12, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x50, 0x65, 0x65, 0x72, 0x52,
    0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x38, 0x0a, 0x0c, 0x53, 0x70, 0x6c, 0x69, 0x74,
    0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x11, 0x0a, 0x09, 0x73, 0x70, 0x6c, 0x69, 0x74,
    0x5f, 0x6b, 0x65, 0x79, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0c, 0x12, 0x15, 0x0a, 0x0d, 0x6e, 0x65,
    0x77, 0x5f, 0x72, 0x65, 0x67, 0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x64, 0x18, 0x02, 0x20, 0x01, 0x28,
    0x04, 0x22, 0x4c, 0x0a, 0x0d, 0x53, 0x70, 0x6c, 0x69, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e,
    0x73, 0x65, 0x12, 0x1c, 0x0a, 0x04, 0x6c, 0x65, 0x66, 0x74, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b,
    0x32, 0x0e, 0x2e, 0x6d, 0x65, 0x74, 0x61, 0x70, 0x62, 0x2e, 0x52, 0x65, 0x67, 0x69, 0x6f, 0x6e,
    0x12, 0x1d, 0x0a, 0x05, 0x72, 0x69, 0x67, 0x68, 0x74, 0x18, 0x02, 0x20, 0x01, 0x28, 0x0b, 0x32,
    0x0e, 0x2e, 0x6d, 0x65, 0x74, 0x61, 0x70, 0x62, 0x2e, 0x52, 0x65, 0x67, 0x69, 0x6f, 0x6e, 0x22,
    0x9b, 0x01, 0x0a, 0x0c, 0x41, 0x64, 0x6d, 0x69, 0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74,
    0x12, 0x2e, 0x0a, 0x08, 0x63, 0x6d, 0x64, 0x5f, 0x74, 0x79, 0x70, 0x65, 0x18, 0x01, 0x20, 0x01,
    0x28, 0x0e, 0x32, 0x1c, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x5f, 0x63, 0x6d, 0x64, 0x70, 0x62, 0x2e,
    0x41, 0x64, 0x6d, 0x69, 0x6e, 0x43, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x54, 0x79, 0x70, 0x65,
    0x12, 0x32, 0x0a, 0x0b, 0x63, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x5f, 0x70, 0x65, 0x65, 0x72, 0x18,
    0x02, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x1d, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x5f, 0x63, 0x6d, 0x64,
    0x70, 0x62, 0x2e, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x50, 0x65, 0x65, 0x72, 0x52, 0x65, 0x71,
    0x75, 0x65, 0x73, 0x74, 0x12, 0x27, 0x0a, 0x05, 0x73, 0x70, 0x6c, 0x69, 0x74, 0x18, 0x03, 0x20,
    0x01, 0x28, 0x0b, 0x32, 0x18, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x5f, 0x63, 0x6d, 0x64, 0x70, 0x62,
    0x2e, 0x53, 0x70, 0x6c, 0x69, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x22, 0x9e, 0x01,
    0x0a, 0x0d, 0x41, 0x64, 0x6d, 0x69, 0x6e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12,
    0x2e, 0x0a, 0x08, 0x63, 0x6d, 0x64, 0x5f, 0x74, 0x79, 0x70, 0x65, 0x18, 0x01, 0x20, 0x01, 0x28,
    0x0e, 0x32, 0x1c, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x5f, 0x63, 0x6d, 0x64, 0x70, 0x62, 0x2e, 0x41,
    0x64, 0x6d, 0x69, 0x6e, 0x43, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x54, 0x79, 0x70, 0x65, 0x12,
    0x33, 0x0a, 0x0b, 0x63, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x5f, 0x70, 0x65, 0x65, 0x72, 0x18, 0x02,
    0x20, 0x01, 0x28, 0x0b, 0x32, 0x1e, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x5f, 0x63, 0x6d, 0x64, 0x70,
    0x62, 0x2e, 0x43, 0x68, 0x61, 0x6e, 0x67, 0x65, 0x50, 0x65, 0x65, 0x72, 0x52, 0x65, 0x73, 0x70,
    0x6f, 0x6e, 0x73, 0x65, 0x12, 0x28, 0x0a, 0x05, 0x73, 0x70, 0x6c, 0x69, 0x74, 0x18, 0x03, 0x20,
    0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x5f, 0x63, 0x6d, 0x64, 0x70, 0x62,
    0x2e, 0x53, 0x70, 0x6c, 0x69, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x22, 0x57,
    0x0a, 0x11, 0x52, 0x61, 0x66, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x48, 0x65, 0x61,
    0x64, 0x65, 0x72, 0x12, 0x11, 0x0a, 0x09, 0x72, 0x65, 0x67, 0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x64,
    0x18, 0x01, 0x20, 0x01, 0x28, 0x04, 0x12, 0x1a, 0x0a, 0x04, 0x70, 0x65, 0x65, 0x72, 0x18, 0x02,
    0x20, 0x01, 0x28, 0x0b, 0x32, 0x0c, 0x2e, 0x6d, 0x65, 0x74, 0x61, 0x70, 0x62, 0x2e, 0x50, 0x65,
    0x65, 0x72, 0x12, 0x13, 0x0a, 0x0b, 0x72, 0x65, 0x61, 0x64, 0x5f, 0x71, 0x75, 0x6f, 0x72, 0x75,
    0x6d, 0x18, 0x03, 0x20, 0x01, 0x28, 0x08, 0x22, 0x33, 0x0a, 0x12, 0x52, 0x61, 0x66, 0x74, 0x52,
    0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x48, 0x65, 0x61, 0x64, 0x65, 0x72, 0x12, 0x1d, 0x0a,
    0x05, 0x65, 0x72, 0x72, 0x6f, 0x72, 0x18, 0x01, 0x20, 0x01, 0x28, 0x0b, 0x32, 0x0e, 0x2e, 0x65,
    0x72, 0x72, 0x6f, 0x72, 0x70, 0x62, 0x2e, 0x45, 0x72, 0x72, 0x6f, 0x72, 0x22, 0x9b, 0x01, 0x0a,
    0x12, 0x52, 0x61, 0x66, 0x74, 0x43, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x52, 0x65, 0x71, 0x75,
    0x65, 0x73, 0x74, 0x12, 0x2d, 0x0a, 0x06, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72, 0x18, 0x01, 0x20,
    0x01, 0x28, 0x0b, 0x32, 0x1d, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x5f, 0x63, 0x6d, 0x64, 0x70, 0x62,
    0x2e, 0x52, 0x61, 0x66, 0x74, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x48, 0x65, 0x61, 0x64,
    0x65, 0x72, 0x12, 0x25, 0x0a, 0x08, 0x72, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x73, 0x18, 0x02,
    0x20, 0x03, 0x28, 0x0b, 0x32, 0x13, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x5f, 0x63, 0x6d, 0x64, 0x70,
    0x62, 0x2e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x12, 0x2f, 0x0a, 0x0d, 0x61, 0x64, 0x6d,
    0x69, 0x6e, 0x5f, 0x72, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x18, 0x03, 0x20, 0x01, 0x28, 0x0b,
    0x32, 0x18, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x5f, 0x63, 0x6d, 0x64, 0x70, 0x62, 0x2e, 0x41, 0x64,
    0x6d, 0x69, 0x6e, 0x52, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x22, 0xa1, 0x01, 0x0a, 0x13, 0x52,
    0x61, 0x66, 0x74, 0x43, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e,
    0x73, 0x65, 0x12, 0x2e, 0x0a, 0x06, 0x68, 0x65, 0x61, 0x64, 0x65, 0x72, 0x18, 0x01, 0x20, 0x01,
    0x28, 0x0b, 0x32, 0x1e, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x5f, 0x63, 0x6d, 0x64, 0x70, 0x62, 0x2e,
    0x52, 0x61, 0x66, 0x74, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x48, 0x65, 0x61, 0x64,
    0x65, 0x72, 0x12, 0x27, 0x0a, 0x09, 0x72, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x73, 0x18,
    0x02, 0x20, 0x03, 0x28, 0x0b, 0x32, 0x14, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x5f, 0x63, 0x6d, 0x64,
    0x70, 0x62, 0x2e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x12, 0x31, 0x0a, 0x0e, 0x61,
    0x64, 0x6d, 0x69, 0x6e, 0x5f, 0x72, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x18, 0x03, 0x20,
    0x01, 0x28, 0x0b, 0x32, 0x19, 0x2e, 0x72, 0x61, 0x66, 0x74, 0x5f, 0x63, 0x6d, 0x64, 0x70, 0x62,
    0x2e, 0x41, 0x64, 0x6d, 0x69, 0x6e, 0x52, 0x65, 0x73, 0x70, 0x6f, 0x6e, 0x73, 0x65, 0x2a, 0x42,
    0x0a, 0x0b, 0x43, 0x6f, 0x6d, 0x6d, 0x61, 0x6e, 0x64, 0x54, 0x79, 0x70, 0x65, 0x12, 0x0b, 0x0a,
    0x07, 0x49, 0x6e, 0x76, 0x61, 0x6c, 0x69, 0x64, 0x10, 0x00, 0x12, 0x07, 0x0a, 0x03, 0x47, 0x65,
    0x74, 0x10, 0x01, 0x12, 0x08, 0x0a, 0x04, 0x53, 0x65, 0x65, 0x6b, 0x10, 0x02, 0x12, 0x07, 0x0a,
    0x03, 0x50, 0x75, 0x74, 0x10, 0x03, 0x12, 0x0a, 0x0a, 0x06, 0x44, 0x65, 0x6c, 0x65, 0x74, 0x65,
    0x10, 0x04, 0x2a, 0x3f, 0x0a, 0x10, 0x41, 0x64, 0x6d, 0x69, 0x6e, 0x43, 0x6f, 0x6d, 0x6d, 0x61,
    0x6e, 0x64, 0x54, 0x79, 0x70, 0x65, 0x12, 0x10, 0x0a, 0x0c, 0x49, 0x6e, 0x76, 0x61, 0x6c, 0x69,
    0x64, 0x41, 0x64, 0x6d, 0x69, 0x6e, 0x10, 0x00, 0x12, 0x0e, 0x0a, 0x0a, 0x43, 0x68, 0x61, 0x6e,
    0x67, 0x65, 0x50, 0x65, 0x65, 0x72, 0x10, 0x01, 0x12, 0x09, 0x0a, 0x05, 0x53, 0x70, 0x6c, 0x69,
    0x74, 0x10, 0x02, 0x4a, 0xd7, 0x20, 0x0a, 0x07, 0x12, 0x05, 0x00, 0x00, 0x84, 0x01, 0x01, 0x0a,
    0x08, 0x0a, 0x01, 0x02, 0x12, 0x03, 0x01, 0x08, 0x12, 0x0a, 0x09, 0x0a, 0x02, 0x03, 0x00, 0x12,
    0x03, 0x03, 0x07, 0x15, 0x0a, 0x09, 0x0a, 0x02, 0x03, 0x01, 0x12, 0x03, 0x04, 0x07, 0x16, 0x0a,
    0x09, 0x0a, 0x02, 0x03, 0x02, 0x12, 0x03, 0x05, 0x07, 0x15, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x00,
    0x12, 0x04, 0x07, 0x00, 0x09, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x00, 0x01, 0x12, 0x03, 0x07,
    0x08, 0x12, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x00, 0x02, 0x00, 0x12, 0x03, 0x08, 0x04, 0x1b, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02, 0x00, 0x04, 0x12, 0x03, 0x08, 0x04, 0x0c, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x00, 0x02, 0x00, 0x05, 0x12, 0x03, 0x08, 0x0d, 0x12, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x00, 0x02, 0x00, 0x01, 0x12, 0x03, 0x08, 0x13, 0x16, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x00, 0x02,
    0x00, 0x03, 0x12, 0x03, 0x08, 0x19, 0x1a, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x01, 0x12, 0x04, 0x0b,
    0x00, 0x0d, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x01, 0x01, 0x12, 0x03, 0x0b, 0x08, 0x13, 0x0a,
    0x0b, 0x0a, 0x04, 0x04, 0x01, 0x02, 0x00, 0x12, 0x03, 0x0c, 0x04, 0x1d, 0x0a, 0x0c, 0x0a, 0x05,
    0x04, 0x01, 0x02, 0x00, 0x04, 0x12, 0x03, 0x0c, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x01,
    0x02, 0x00, 0x05, 0x12, 0x03, 0x0c, 0x0d, 0x12, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x01, 0x02, 0x00,
    0x01, 0x12, 0x03, 0x0c, 0x13, 0x18, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x01, 0x02, 0x00, 0x03, 0x12,
    0x03, 0x0c, 0x1b, 0x1c, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x02, 0x12, 0x04, 0x0f, 0x00, 0x11, 0x01,
    0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x02, 0x01, 0x12, 0x03, 0x0f, 0x08, 0x13, 0x0a, 0x0b, 0x0a, 0x04,
    0x04, 0x02, 0x02, 0x00, 0x12, 0x03, 0x10, 0x04, 0x1b, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x02, 0x02,
    0x00, 0x04, 0x12, 0x03, 0x10, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x02, 0x02, 0x00, 0x05,
    0x12, 0x03, 0x10, 0x0d, 0x12, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x02, 0x02, 0x00, 0x01, 0x12, 0x03,
    0x10, 0x13, 0x16, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x02, 0x02, 0x00, 0x03, 0x12, 0x03, 0x10, 0x19,
    0x1a, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x03, 0x12, 0x04, 0x13, 0x00, 0x16, 0x01, 0x0a, 0x0a, 0x0a,
    0x03, 0x04, 0x03, 0x01, 0x12, 0x03, 0x13, 0x08, 0x14, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x03, 0x02,
    0x00, 0x12, 0x03, 0x14, 0x04, 0x1d, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x00, 0x04, 0x12,
    0x03, 0x14, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x00, 0x05, 0x12, 0x03, 0x14,
    0x0d, 0x12, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x00, 0x01, 0x12, 0x03, 0x14, 0x13, 0x16,
    0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x00, 0x03, 0x12, 0x03, 0x14, 0x1b, 0x1c, 0x0a, 0x0b,
    0x0a, 0x04, 0x04, 0x03, 0x02, 0x01, 0x12, 0x03, 0x15, 0x04, 0x1d, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x03, 0x02, 0x01, 0x04, 0x12, 0x03, 0x15, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02,
    0x01, 0x05, 0x12, 0x03, 0x15, 0x0d, 0x12, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x01, 0x01,
    0x12, 0x03, 0x15, 0x13, 0x18, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x03, 0x02, 0x01, 0x03, 0x12, 0x03,
    0x15, 0x1b, 0x1c, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x04, 0x12, 0x04, 0x18, 0x00, 0x1b, 0x01, 0x0a,
    0x0a, 0x0a, 0x03, 0x04, 0x04, 0x01, 0x12, 0x03, 0x18, 0x08, 0x12, 0x0a, 0x0b, 0x0a, 0x04, 0x04,
    0x04, 0x02, 0x00, 0x12, 0x03, 0x19, 0x04, 0x1d, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04, 0x02, 0x00,
    0x04, 0x12, 0x03, 0x19, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04, 0x02, 0x00, 0x05, 0x12,
    0x03, 0x19, 0x0d, 0x12, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04, 0x02, 0x00, 0x01, 0x12, 0x03, 0x19,
    0x13, 0x16, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04, 0x02, 0x00, 0x03, 0x12, 0x03, 0x19, 0x1b, 0x1c,
    0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x04, 0x02, 0x01, 0x12, 0x03, 0x1a, 0x04, 0x1d, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x04, 0x02, 0x01, 0x04, 0x12, 0x03, 0x1a, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x04, 0x02, 0x01, 0x05, 0x12, 0x03, 0x1a, 0x0d, 0x12, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04, 0x02,
    0x01, 0x01, 0x12, 0x03, 0x1a, 0x13, 0x18, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x04, 0x02, 0x01, 0x03,
    0x12, 0x03, 0x1a, 0x1b, 0x1c, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x05, 0x12, 0x04, 0x1d, 0x00, 0x1f,
    0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x05, 0x01, 0x12, 0x03, 0x1d, 0x08, 0x13, 0x0a, 0x0a, 0x0a,
    0x02, 0x04, 0x06, 0x12, 0x04, 0x21, 0x00, 0x23, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x06, 0x01,
    0x12, 0x03, 0x21, 0x08, 0x15, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x06, 0x02, 0x00, 0x12, 0x03, 0x22,
    0x04, 0x1b, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x06, 0x02, 0x00, 0x04, 0x12, 0x03, 0x22, 0x04, 0x0c,
    0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x06, 0x02, 0x00, 0x05, 0x12, 0x03, 0x22, 0x0d, 0x12, 0x0a, 0x0c,
    0x0a, 0x05, 0x04, 0x06, 0x02, 0x00, 0x01, 0x12, 0x03, 0x22, 0x13, 0x16, 0x0a, 0x0c, 0x0a, 0x05,
    0x04, 0x06, 0x02, 0x00, 0x03, 0x12, 0x03, 0x22, 0x19, 0x1a, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x07,
    0x12, 0x04, 0x25, 0x00, 0x27, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x07, 0x01, 0x12, 0x03, 0x25,
    0x08, 0x16, 0x0a, 0x0a, 0x0a, 0x02, 0x05, 0x00, 0x12, 0x04, 0x29, 0x00, 0x2f, 0x01, 0x0a, 0x0a,
    0x0a, 0x03, 0x05, 0x00, 0x01, 0x12, 0x03, 0x29, 0x05, 0x10, 0x0a, 0x0b, 0x0a, 0x04, 0x05, 0x00,
    0x02, 0x00, 0x12, 0x03, 0x2a, 0x04, 0x14, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x00, 0x02, 0x00, 0x01,
    0x12, 0x03, 0x2a, 0x04, 0x0b, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x00, 0x02, 0x00, 0x02, 0x12, 0x03,
    0x2a, 0x12, 0x13, 0x0a, 0x0b, 0x0a, 0x04, 0x05, 0x00, 0x02, 0x01, 0x12, 0x03, 0x2b, 0x04, 0x14,
    0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x00, 0x02, 0x01, 0x01, 0x12, 0x03, 0x2b, 0x04, 0x07, 0x0a, 0x0c,
    0x0a, 0x05, 0x05, 0x00, 0x02, 0x01, 0x02, 0x12, 0x03, 0x2b, 0x12, 0x13, 0x0a, 0x0b, 0x0a, 0x04,
    0x05, 0x00, 0x02, 0x02, 0x12, 0x03, 0x2c, 0x04, 0x14, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x00, 0x02,
    0x02, 0x01, 0x12, 0x03, 0x2c, 0x04, 0x08, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x00, 0x02, 0x02, 0x02,
    0x12, 0x03, 0x2c, 0x12, 0x13, 0x0a, 0x0b, 0x0a, 0x04, 0x05, 0x00, 0x02, 0x03, 0x12, 0x03, 0x2d,
    0x04, 0x14, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x00, 0x02, 0x03, 0x01, 0x12, 0x03, 0x2d, 0x04, 0x07,
    0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x00, 0x02, 0x03, 0x02, 0x12, 0x03, 0x2d, 0x12, 0x13, 0x0a, 0x0b,
    0x0a, 0x04, 0x05, 0x00, 0x02, 0x04, 0x12, 0x03, 0x2e, 0x04, 0x14, 0x0a, 0x0c, 0x0a, 0x05, 0x05,
    0x00, 0x02, 0x04, 0x01, 0x12, 0x03, 0x2e, 0x04, 0x0a, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x00, 0x02,
    0x04, 0x02, 0x12, 0x03, 0x2e, 0x12, 0x13, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x08, 0x12, 0x04, 0x31,
    0x00, 0x37, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x08, 0x01, 0x12, 0x03, 0x31, 0x08, 0x0f, 0x0a,
    0x0b, 0x0a, 0x04, 0x04, 0x08, 0x02, 0x00, 0x12, 0x03, 0x32, 0x04, 0x2b, 0x0a, 0x0c, 0x0a, 0x05,
    0x04, 0x08, 0x02, 0x00, 0x04, 0x12, 0x03, 0x32, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x08,
    0x02, 0x00, 0x06, 0x12, 0x03, 0x32, 0x0d, 0x18, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x08, 0x02, 0x00,
    0x01, 0x12, 0x03, 0x32, 0x19, 0x21, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x08, 0x02, 0x00, 0x03, 0x12,
    0x03, 0x32, 0x29, 0x2a, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x08, 0x02, 0x01, 0x12, 0x03, 0x33, 0x04,
    0x2b, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x08, 0x02, 0x01, 0x04, 0x12, 0x03, 0x33, 0x04, 0x0c, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x08, 0x02, 0x01, 0x06, 0x12, 0x03, 0x33, 0x0d, 0x17, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x08, 0x02, 0x01, 0x01, 0x12, 0x03, 0x33, 0x18, 0x1b, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x08, 0x02, 0x01, 0x03, 0x12, 0x03, 0x33, 0x29, 0x2a, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x08, 0x02,
    0x02, 0x12, 0x03, 0x34, 0x04, 0x2b, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x08, 0x02, 0x02, 0x04, 0x12,
    0x03, 0x34, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x08, 0x02, 0x02, 0x06, 0x12, 0x03, 0x34,
    0x0d, 0x18, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x08, 0x02, 0x02, 0x01, 0x12, 0x03, 0x34, 0x19, 0x1d,
    0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x08, 0x02, 0x02, 0x03, 0x12, 0x03, 0x34, 0x29, 0x2a, 0x0a, 0x0b,
    0x0a, 0x04, 0x04, 0x08, 0x02, 0x03, 0x12, 0x03, 0x35, 0x04, 0x2b, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x08, 0x02, 0x03, 0x04, 0x12, 0x03, 0x35, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x08, 0x02,
    0x03, 0x06, 0x12, 0x03, 0x35, 0x0d, 0x17, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x08, 0x02, 0x03, 0x01,
    0x12, 0x03, 0x35, 0x18, 0x1b, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x08, 0x02, 0x03, 0x03, 0x12, 0x03,
    0x35, 0x29, 0x2a, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x08, 0x02, 0x04, 0x12, 0x03, 0x36, 0x04, 0x2b,
    0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x08, 0x02, 0x04, 0x04, 0x12, 0x03, 0x36, 0x04, 0x0c, 0x0a, 0x0c,
    0x0a, 0x05, 0x04, 0x08, 0x02, 0x04, 0x06, 0x12, 0x03, 0x36, 0x0d, 0x1a, 0x0a, 0x0c, 0x0a, 0x05,
    0x04, 0x08, 0x02, 0x04, 0x01, 0x12, 0x03, 0x36, 0x1b, 0x21, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x08,
    0x02, 0x04, 0x03, 0x12, 0x03, 0x36, 0x29, 0x2a, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x09, 0x12, 0x04,
    0x39, 0x00, 0x3f, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x09, 0x01, 0x12, 0x03, 0x39, 0x08, 0x10,
    0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x09, 0x02, 0x00, 0x12, 0x03, 0x3a, 0x04, 0x2b, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x09, 0x02, 0x00, 0x04, 0x12, 0x03, 0x3a, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x09, 0x02, 0x00, 0x06, 0x12, 0x03, 0x3a, 0x0d, 0x18, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x09, 0x02,
    0x00, 0x01, 0x12, 0x03, 0x3a, 0x19, 0x21, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x09, 0x02, 0x00, 0x03,
    0x12, 0x03, 0x3a, 0x29, 0x2a, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x09, 0x02, 0x01, 0x12, 0x03, 0x3b,
    0x04, 0x2b, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x09, 0x02, 0x01, 0x04, 0x12, 0x03, 0x3b, 0x04, 0x0c,
    0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x09, 0x02, 0x01, 0x06, 0x12, 0x03, 0x3b, 0x0d, 0x18, 0x0a, 0x0c,
    0x0a, 0x05, 0x04, 0x09, 0x02, 0x01, 0x01, 0x12, 0x03, 0x3b, 0x19, 0x1c, 0x0a, 0x0c, 0x0a, 0x05,
    0x04, 0x09, 0x02, 0x01, 0x03, 0x12, 0x03, 0x3b, 0x29, 0x2a, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x09,
    0x02, 0x02, 0x12, 0x03, 0x3c, 0x04, 0x2b, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x09, 0x02, 0x02, 0x04,
    0x12, 0x03, 0x3c, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x09, 0x02, 0x02, 0x06, 0x12, 0x03,
    0x3c, 0x0d, 0x19, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x09, 0x02, 0x02, 0x01, 0x12, 0x03, 0x3c, 0x1a,
    0x1e, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x09, 0x02, 0x02, 0x03, 0x12, 0x03, 0x3c, 0x29, 0x2a, 0x0a,
    0x0b, 0x0a, 0x04, 0x04, 0x09, 0x02, 0x03, 0x12, 0x03, 0x3d, 0x04, 0x2b, 0x0a, 0x0c, 0x0a, 0x05,
    0x04, 0x09, 0x02, 0x03, 0x04, 0x12, 0x03, 0x3d, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x09,
    0x02, 0x03, 0x06, 0x12, 0x03, 0x3d, 0x0d, 0x18, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x09, 0x02, 0x03,
    0x01, 0x12, 0x03, 0x3d, 0x19, 0x1c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x09, 0x02, 0x03, 0x03, 0x12,
    0x03, 0x3d, 0x29, 0x2a, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x09, 0x02, 0x04, 0x12, 0x03, 0x3e, 0x04,
    0x2b, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x09, 0x02, 0x04, 0x04, 0x12, 0x03, 0x3e, 0x04, 0x0c, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x09, 0x02, 0x04, 0x06, 0x12, 0x03, 0x3e, 0x0d, 0x1b, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x09, 0x02, 0x04, 0x01, 0x12, 0x03, 0x3e, 0x1c, 0x22, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x09, 0x02, 0x04, 0x03, 0x12, 0x03, 0x3e, 0x29, 0x2a, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x0a, 0x12,
    0x04, 0x41, 0x00, 0x45, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x0a, 0x01, 0x12, 0x03, 0x41, 0x08,
    0x19, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x0a, 0x02, 0x00, 0x12, 0x03, 0x42, 0x04, 0x33, 0x0a, 0x0c,
    0x0a, 0x05, 0x04, 0x0a, 0x02, 0x00, 0x04, 0x12, 0x03, 0x42, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05,
    0x04, 0x0a, 0x02, 0x00, 0x06, 0x12, 0x03, 0x42, 0x0d, 0x22, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0a,
    0x02, 0x00, 0x01, 0x12, 0x03, 0x42, 0x23, 0x2e, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0a, 0x02, 0x00,
    0x03, 0x12, 0x03, 0x42, 0x31, 0x32, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x0a, 0x02, 0x01, 0x12, 0x03,
    0x43, 0x04, 0x33, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0a, 0x02, 0x01, 0x04, 0x12, 0x03, 0x43, 0x04,
    0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0a, 0x02, 0x01, 0x06, 0x12, 0x03, 0x43, 0x0d, 0x18, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x0a, 0x02, 0x01, 0x01, 0x12, 0x03, 0x43, 0x19, 0x1d, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x0a, 0x02, 0x01, 0x03, 0x12, 0x03, 0x43, 0x31, 0x32, 0x0a, 0x0b, 0x0a, 0x04, 0x04,
    0x0a, 0x02, 0x02, 0x12, 0x03, 0x44, 0x04, 0x33, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0a, 0x02, 0x02,
    0x04, 0x12, 0x03, 0x44, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0a, 0x02, 0x02, 0x06, 0x12,
    0x03, 0x44, 0x0d, 0x18, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0a, 0x02, 0x02, 0x01, 0x12, 0x03, 0x44,
    0x19, 0x25, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0a, 0x02, 0x02, 0x03, 0x12, 0x03, 0x44, 0x31, 0x32,
    0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x0b, 0x12, 0x04, 0x47, 0x00, 0x49, 0x01, 0x0a, 0x0a, 0x0a, 0x03,
    0x04, 0x0b, 0x01, 0x12, 0x03, 0x47, 0x08, 0x1a, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x0c, 0x12, 0x04,
    0x4b, 0x00, 0x54, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x0c, 0x01, 0x12, 0x03, 0x4b, 0x08, 0x14,
    0x0a, 0x99, 0x01, 0x0a, 0x04, 0x04, 0x0c, 0x02, 0x00, 0x12, 0x03, 0x4f, 0x04, 0x21, 0x1a, 0x8b,
    0x01, 0x20, 0x54, 0x68, 0x65, 0x20, 0x73, 0x70, 0x6c, 0x69, 0x74, 0x5f, 0x6b, 0x65, 0x79, 0x20,
    0x6d, 0x75, 0x73, 0x74, 0x20, 0x62, 0x65, 0x20, 0x69, 0x6e, 0x20, 0x74, 0x68, 0x65, 0x20, 0x62,
    0x65, 0x65, 0x6e, 0x20, 0x73, 0x70, 0x6c, 0x69, 0x74, 0x74, 0x69, 0x6e, 0x67, 0x20, 0x72, 0x65,
    0x67, 0x69, 0x6f, 0x6e, 0x2e, 0x20, 0x0a, 0x20, 0x49, 0x66, 0x20, 0x74, 0x68, 0x65, 0x20, 0x73,
    0x70, 0x6c, 0x69, 0x74, 0x5f, 0x6b, 0x65, 0x79, 0x20, 0x69, 0x73, 0x20, 0x6e, 0x6f, 0x6e, 0x65,
    0x2c, 0x20, 0x77, 0x65, 0x20, 0x77, 0x69, 0x6c, 0x6c, 0x20, 0x63, 0x68, 0x6f, 0x6f, 0x73, 0x65,
    0x20, 0x61, 0x20, 0x70, 0x72, 0x6f, 0x70, 0x65, 0x72, 0x20, 0x6b, 0x65, 0x79, 0x0a, 0x20, 0x74,
    0x6f, 0x20, 0x73, 0x70, 0x6c, 0x69, 0x74, 0x20, 0x74, 0x68, 0x65, 0x20, 0x72, 0x65, 0x67, 0x69,
    0x6f, 0x6e, 0x20, 0x69, 0x6e, 0x20, 0x68, 0x61, 0x6c, 0x66, 0x2e, 0x0a, 0x0a, 0x0c, 0x0a, 0x05,
    0x04, 0x0c, 0x02, 0x00, 0x04, 0x12, 0x03, 0x4f, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0c,
    0x02, 0x00, 0x05, 0x12, 0x03, 0x4f, 0x0d, 0x12, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0c, 0x02, 0x00,
    0x01, 0x12, 0x03, 0x4f, 0x13, 0x1c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0c, 0x02, 0x00, 0x03, 0x12,
    0x03, 0x4f, 0x1f, 0x20, 0x0a, 0xba, 0x01, 0x0a, 0x04, 0x04, 0x0c, 0x02, 0x01, 0x12, 0x03, 0x53,
    0x04, 0x26, 0x1a, 0xac, 0x01, 0x20, 0x57, 0x65, 0x20, 0x73, 0x70, 0x6c, 0x69, 0x74, 0x20, 0x74,
    0x68, 0x65, 0x20, 0x72, 0x65, 0x67, 0x69, 0x6f, 0x6e, 0x20, 0x69, 0x6e, 0x74, 0x6f, 0x20, 0x74,
    0x77, 0x6f, 0x2c, 0x20, 0x66, 0x69, 0x72, 0x73, 0x74, 0x20, 0x75, 0x73, 0x65, 0x73, 0x20, 0x74,
    0x68, 0x65, 0x20, 0x6f, 0x72, 0x69, 0x67, 0x69, 0x6e, 0x20, 0x0a, 0x20, 0x70, 0x61, 0x72, 0x65,
    0x6e, 0x74, 0x20, 0x72, 0x65, 0x67, 0x69, 0x6f, 0x6e, 0x20, 0x69, 0x64, 0x2c, 0x20, 0x61, 0x6e,
    0x64, 0x20, 0x74, 0x68, 0x65, 0x20, 0x73, 0x65, 0x63, 0x6f, 0x6e, 0x64, 0x20, 0x75, 0x73, 0x65,
    0x73, 0x20, 0x74, 0x68, 0x65, 0x20, 0x6e, 0x65, 0x77, 0x5f, 0x72, 0x65, 0x67, 0x69, 0x6f, 0x6e,
    0x5f, 0x69, 0x64, 0x2e, 0x0a, 0x20, 0x57, 0x65, 0x20, 0x6d, 0x75, 0x73, 0x74, 0x20, 0x67, 0x75,
    0x61, 0x72, 0x61, 0x6e, 0x74, 0x65, 0x65, 0x20, 0x74, 0x68, 0x61, 0x74, 0x20, 0x74, 0x68, 0x65,
    0x20, 0x6e, 0x65, 0x77, 0x5f, 0x72, 0x65, 0x67, 0x69, 0x6f, 0x6e, 0x5f, 0x69, 0x64, 0x20, 0x69,
    0x73, 0x20, 0x67, 0x6c, 0x6f, 0x62, 0x61, 0x6c, 0x20, 0x75, 0x6e, 0x69, 0x71, 0x75, 0x65, 0x2e,
    0x0a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0c, 0x02, 0x01, 0x04, 0x12, 0x03, 0x53, 0x04, 0x0c, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x0c, 0x02, 0x01, 0x05, 0x12, 0x03, 0x53, 0x0d, 0x13, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x0c, 0x02, 0x01, 0x01, 0x12, 0x03, 0x53, 0x14, 0x21, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x0c, 0x02, 0x01, 0x03, 0x12, 0x03, 0x53, 0x24, 0x25, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x0d, 0x12,
    0x04, 0x56, 0x00, 0x59, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x0d, 0x01, 0x12, 0x03, 0x56, 0x08,
    0x15, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x0d, 0x02, 0x00, 0x12, 0x03, 0x57, 0x04, 0x25, 0x0a, 0x0c,
    0x0a, 0x05, 0x04, 0x0d, 0x02, 0x00, 0x04, 0x12, 0x03, 0x57, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05,
    0x04, 0x0d, 0x02, 0x00, 0x06, 0x12, 0x03, 0x57, 0x0d, 0x1a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0d,
    0x02, 0x00, 0x01, 0x12, 0x03, 0x57, 0x1b, 0x1f, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0d, 0x02, 0x00,
    0x03, 0x12, 0x03, 0x57, 0x23, 0x24, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x0d, 0x02, 0x01, 0x12, 0x03,
    0x58, 0x04, 0x25, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0d, 0x02, 0x01, 0x04, 0x12, 0x03, 0x58, 0x04,
    0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0d, 0x02, 0x01, 0x06, 0x12, 0x03, 0x58, 0x0d, 0x1a, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x0d, 0x02, 0x01, 0x01, 0x12, 0x03, 0x58, 0x1b, 0x20, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x0d, 0x02, 0x01, 0x03, 0x12, 0x03, 0x58, 0x23, 0x24, 0x0a, 0x0a, 0x0a, 0x02, 0x05,
    0x01, 0x12, 0x04, 0x5b, 0x00, 0x5f, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x05, 0x01, 0x01, 0x12, 0x03,
    0x5b, 0x05, 0x15, 0x0a, 0x0b, 0x0a, 0x04, 0x05, 0x01, 0x02, 0x00, 0x12, 0x03, 0x5c, 0x04, 0x18,
    0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x01, 0x02, 0x00, 0x01, 0x12, 0x03, 0x5c, 0x04, 0x10, 0x0a, 0x0c,
    0x0a, 0x05, 0x05, 0x01, 0x02, 0x00, 0x02, 0x12, 0x03, 0x5c, 0x16, 0x17, 0x0a, 0x0b, 0x0a, 0x04,
    0x05, 0x01, 0x02, 0x01, 0x12, 0x03, 0x5d, 0x04, 0x18, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x01, 0x02,
    0x01, 0x01, 0x12, 0x03, 0x5d, 0x04, 0x0e, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x01, 0x02, 0x01, 0x02,
    0x12, 0x03, 0x5d, 0x16, 0x17, 0x0a, 0x0b, 0x0a, 0x04, 0x05, 0x01, 0x02, 0x02, 0x12, 0x03, 0x5e,
    0x04, 0x18, 0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x01, 0x02, 0x02, 0x01, 0x12, 0x03, 0x5e, 0x04, 0x09,
    0x0a, 0x0c, 0x0a, 0x05, 0x05, 0x01, 0x02, 0x02, 0x02, 0x12, 0x03, 0x5e, 0x16, 0x17, 0x0a, 0x0a,
    0x0a, 0x02, 0x04, 0x0e, 0x12, 0x04, 0x61, 0x00, 0x65, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x0e,
    0x01, 0x12, 0x03, 0x61, 0x08, 0x14, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x0e, 0x02, 0x00, 0x12, 0x03,
    0x62, 0x04, 0x30, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0e, 0x02, 0x00, 0x04, 0x12, 0x03, 0x62, 0x04,
    0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0e, 0x02, 0x00, 0x06, 0x12, 0x03, 0x62, 0x0d, 0x1d, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x0e, 0x02, 0x00, 0x01, 0x12, 0x03, 0x62, 0x1e, 0x26, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x0e, 0x02, 0x00, 0x03, 0x12, 0x03, 0x62, 0x2e, 0x2f, 0x0a, 0x0b, 0x0a, 0x04, 0x04,
    0x0e, 0x02, 0x01, 0x12, 0x03, 0x63, 0x04, 0x30, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0e, 0x02, 0x01,
    0x04, 0x12, 0x03, 0x63, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0e, 0x02, 0x01, 0x06, 0x12,
    0x03, 0x63, 0x0d, 0x1e, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0e, 0x02, 0x01, 0x01, 0x12, 0x03, 0x63,
    0x1f, 0x2a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0e, 0x02, 0x01, 0x03, 0x12, 0x03, 0x63, 0x2e, 0x2f,
    0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x0e, 0x02, 0x02, 0x12, 0x03, 0x64, 0x04, 0x30, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x0e, 0x02, 0x02, 0x04, 0x12, 0x03, 0x64, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x0e, 0x02, 0x02, 0x06, 0x12, 0x03, 0x64, 0x0d, 0x19, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0e, 0x02,
    0x02, 0x01, 0x12, 0x03, 0x64, 0x1a, 0x1f, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0e, 0x02, 0x02, 0x03,
    0x12, 0x03, 0x64, 0x2e, 0x2f, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x0f, 0x12, 0x04, 0x67, 0x00, 0x6b,
    0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x0f, 0x01, 0x12, 0x03, 0x67, 0x08, 0x15, 0x0a, 0x0b, 0x0a,
    0x04, 0x04, 0x0f, 0x02, 0x00, 0x12, 0x03, 0x68, 0x04, 0x30, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0f,
    0x02, 0x00, 0x04, 0x12, 0x03, 0x68, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0f, 0x02, 0x00,
    0x06, 0x12, 0x03, 0x68, 0x0d, 0x1d, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0f, 0x02, 0x00, 0x01, 0x12,
    0x03, 0x68, 0x1e, 0x26, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0f, 0x02, 0x00, 0x03, 0x12, 0x03, 0x68,
    0x2e, 0x2f, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x0f, 0x02, 0x01, 0x12, 0x03, 0x69, 0x04, 0x30, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x0f, 0x02, 0x01, 0x04, 0x12, 0x03, 0x69, 0x04, 0x0c, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x0f, 0x02, 0x01, 0x06, 0x12, 0x03, 0x69, 0x0d, 0x1f, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x0f, 0x02, 0x01, 0x01, 0x12, 0x03, 0x69, 0x20, 0x2b, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0f, 0x02,
    0x01, 0x03, 0x12, 0x03, 0x69, 0x2e, 0x2f, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x0f, 0x02, 0x02, 0x12,
    0x03, 0x6a, 0x04, 0x30, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0f, 0x02, 0x02, 0x04, 0x12, 0x03, 0x6a,
    0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0f, 0x02, 0x02, 0x06, 0x12, 0x03, 0x6a, 0x0d, 0x1a,
    0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x0f, 0x02, 0x02, 0x01, 0x12, 0x03, 0x6a, 0x1b, 0x20, 0x0a, 0x0c,
    0x0a, 0x05, 0x04, 0x0f, 0x02, 0x02, 0x03, 0x12, 0x03, 0x6a, 0x2e, 0x2f, 0x0a, 0x0a, 0x0a, 0x02,
    0x04, 0x10, 0x12, 0x04, 0x6d, 0x00, 0x72, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04, 0x10, 0x01, 0x12,
    0x03, 0x6d, 0x08, 0x19, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x10, 0x02, 0x00, 0x12, 0x03, 0x6e, 0x04,
    0x2b, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x10, 0x02, 0x00, 0x04, 0x12, 0x03, 0x6e, 0x04, 0x0c, 0x0a,
    0x0c, 0x0a, 0x05, 0x04, 0x10, 0x02, 0x00, 0x05, 0x12, 0x03, 0x6e, 0x0d, 0x13, 0x0a, 0x0c, 0x0a,
    0x05, 0x04, 0x10, 0x02, 0x00, 0x01, 0x12, 0x03, 0x6e, 0x14, 0x1d, 0x0a, 0x0c, 0x0a, 0x05, 0x04,
    0x10, 0x02, 0x00, 0x03, 0x12, 0x03, 0x6e, 0x29, 0x2a, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x10, 0x02,
    0x01, 0x12, 0x03, 0x6f, 0x04, 0x2b, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x10, 0x02, 0x01, 0x04, 0x12,
    0x03, 0x6f, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x10, 0x02, 0x01, 0x06, 0x12, 0x03, 0x6f,
    0x0d, 0x18, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x10, 0x02, 0x01, 0x01, 0x12, 0x03, 0x6f, 0x19, 0x1d,
    0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x10, 0x02, 0x01, 0x03, 0x12, 0x03, 0x6f, 0x29, 0x2a, 0x0a, 0x2a,
    0x0a, 0x04, 0x04, 0x10, 0x02, 0x02, 0x12, 0x03, 0x71, 0x04, 0x2b, 0x1a, 0x1d, 0x20, 0x74, 0x72,
    0x75, 0x65, 0x20, 0x66, 0x6f, 0x72, 0x20, 0x72, 0x65, 0x61, 0x64, 0x20, 0x6c, 0x69, 0x6e, 0x65,
    0x61, 0x72, 0x69, 0x7a, 0x61, 0x74, 0x69, 0x6f, 0x6e, 0x0a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x10,
    0x02, 0x02, 0x04, 0x12, 0x03, 0x71, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x10, 0x02, 0x02,
    0x05, 0x12, 0x03, 0x71, 0x0d, 0x11, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x10, 0x02, 0x02, 0x01, 0x12,
    0x03, 0x71, 0x12, 0x1d, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x10, 0x02, 0x02, 0x03, 0x12, 0x03, 0x71,
    0x29, 0x2a, 0x0a, 0x0a, 0x0a, 0x02, 0x04, 0x11, 0x12, 0x04, 0x74, 0x00, 0x76, 0x01, 0x0a, 0x0a,
    0x0a, 0x03, 0x04, 0x11, 0x01, 0x12, 0x03, 0x74, 0x08, 0x1a, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x11,
    0x02, 0x00, 0x12, 0x03, 0x75, 0x04, 0x26, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x11, 0x02, 0x00, 0x04,
    0x12, 0x03, 0x75, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x11, 0x02, 0x00, 0x06, 0x12, 0x03,
    0x75, 0x0d, 0x1a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x11, 0x02, 0x00, 0x01, 0x12, 0x03, 0x75, 0x1b,
    0x20, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x11, 0x02, 0x00, 0x03, 0x12, 0x03, 0x75, 0x24, 0x25, 0x0a,
    0x0a, 0x0a, 0x02, 0x04, 0x12, 0x12, 0x04, 0x78, 0x00, 0x7e, 0x01, 0x0a, 0x0a, 0x0a, 0x03, 0x04,
    0x12, 0x01, 0x12, 0x03, 0x78, 0x08, 0x1a, 0x0a, 0x0b, 0x0a, 0x04, 0x04, 0x12, 0x02, 0x00, 0x12,
    0x03, 0x79, 0x04, 0x2e, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x12, 0x02, 0x00, 0x04, 0x12, 0x03, 0x79,
    0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x12, 0x02, 0x00, 0x06, 0x12, 0x03, 0x79, 0x0d, 0x1e,
    0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x12, 0x02, 0x00, 0x01, 0x12, 0x03, 0x79, 0x1f, 0x25, 0x0a, 0x0c,
    0x0a, 0x05, 0x04, 0x12, 0x02, 0x00, 0x03, 0x12, 0x03, 0x79, 0x2c, 0x2d, 0x0a, 0x59, 0x0a, 0x04,
    0x04, 0x12, 0x02, 0x01, 0x12, 0x03, 0x7c, 0x04, 0x2e, 0x1a, 0x4c, 0x20, 0x57, 0x65, 0x20, 0x63,
    0x61, 0x6e, 0x27, 0x74, 0x20, 0x65, 0x6e, 0x63, 0x6c, 0x6f, 0x73, 0x65, 0x20, 0x6e, 0x6f, 0x72,
    0x6d, 0x61, 0x6c, 0x20, 0x72, 0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x73, 0x20, 0x61, 0x6e, 0x64,
    0x20, 0x61, 0x64, 0x6d, 0x69, 0x6e, 0x69, 0x73, 0x74, 0x72, 0x61, 0x74, 0x6f, 0x72, 0x20, 0x72,
    0x65, 0x71, 0x75, 0x65, 0x73, 0x74, 0x0a, 0x20, 0x61, 0x74, 0x20, 0x73, 0x61, 0x6d, 0x65, 0x20,
    0x74, 0x69, 0x6d, 0x65, 0x2e, 0x20, 0x0a, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x12, 0x02, 0x01, 0x04,
    0x12, 0x03, 0x7c, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x12, 0x02, 0x01, 0x06, 0x12, 0x03,
    0x7c, 0x0d, 0x14, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x12, 0x02, 0x01, 0x01, 0x12, 0x03, 0x7c, 0x15,
    0x1d, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x12, 0x02, 0x01, 0x03, 0x12, 0x03, 0x7c, 0x2c, 0x2d, 0x0a,
    0x0b, 0x0a, 0x04, 0x04, 0x12, 0x02, 0x02, 0x12, 0x03, 0x7d, 0x04, 0x2e, 0x0a, 0x0c, 0x0a, 0x05,
    0x04, 0x12, 0x02, 0x02, 0x04, 0x12, 0x03, 0x7d, 0x04, 0x0c, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x12,
    0x02, 0x02, 0x06, 0x12, 0x03, 0x7d, 0x0d, 0x19, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x12, 0x02, 0x02,
    0x01, 0x12, 0x03, 0x7d, 0x1a, 0x27, 0x0a, 0x0c, 0x0a, 0x05, 0x04, 0x12, 0x02, 0x02, 0x03, 0x12,
    0x03, 0x7d, 0x2c, 0x2d, 0x0a, 0x0c, 0x0a, 0x02, 0x04, 0x13, 0x12, 0x06, 0x80, 0x01, 0x00, 0x84,
    0x01, 0x01, 0x0a, 0x0b, 0x0a, 0x03, 0x04, 0x13, 0x01, 0x12, 0x04, 0x80, 0x01, 0x08, 0x1b, 0x0a,
    0x0c, 0x0a, 0x04, 0x04, 0x13, 0x02, 0x00, 0x12, 0x04, 0x81, 0x01, 0x04, 0x2e, 0x0a, 0x0d, 0x0a,
    0x05, 0x04, 0x13, 0x02, 0x00, 0x04, 0x12, 0x04, 0x81, 0x01, 0x04, 0x0c, 0x0a, 0x0d, 0x0a, 0x05,
    0x04, 0x13, 0x02, 0x00, 0x06, 0x12, 0x04, 0x81, 0x01, 0x0d, 0x1f, 0x0a, 0x0d, 0x0a, 0x05, 0x04,
    0x13, 0x02, 0x00, 0x01, 0x12, 0x04, 0x81, 0x01, 0x20, 0x26, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x13,
    0x02, 0x00, 0x03, 0x12, 0x04, 0x81, 0x01, 0x2c, 0x2d, 0x0a, 0x0c, 0x0a, 0x04, 0x04, 0x13, 0x02,
    0x01, 0x12, 0x04, 0x82, 0x01, 0x04, 0x2e, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x13, 0x02, 0x01, 0x04,
    0x12, 0x04, 0x82, 0x01, 0x04, 0x0c, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x13, 0x02, 0x01, 0x06, 0x12,
    0x04, 0x82, 0x01, 0x0d, 0x15, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x13, 0x02, 0x01, 0x01, 0x12, 0x04,
    0x82, 0x01, 0x16, 0x1f, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x13, 0x02, 0x01, 0x03, 0x12, 0x04, 0x82,
    0x01, 0x2c, 0x2d, 0x0a, 0x0c, 0x0a, 0x04, 0x04, 0x13, 0x02, 0x02, 0x12, 0x04, 0x83, 0x01, 0x04,
    0x2e, 0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x13, 0x02, 0x02, 0x04, 0x12, 0x04, 0x83, 0x01, 0x04, 0x0c,
    0x0a, 0x0d, 0x0a, 0x05, 0x04, 0x13, 0x02, 0x02, 0x06, 0x12, 0x04, 0x83, 0x01, 0x0d, 0x1a, 0x0a,
    0x0d, 0x0a, 0x05, 0x04, 0x13, 0x02, 0x02, 0x01, 0x12, 0x04, 0x83, 0x01, 0x1b, 0x29, 0x0a, 0x0d,
    0x0a, 0x05, 0x04, 0x13, 0x02, 0x02, 0x03, 0x12, 0x04, 0x83, 0x01, 0x2c, 0x2d,
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
