// This file is generated. Do not edit
// @generated

// https://github.com/Manishearth/rust-clippy/issues/702
#![allow(unknown_lints)]
#![allow(clippy)]

#![cfg_attr(rustfmt, rustfmt_skip)]

#![allow(box_pointers)]
#![allow(dead_code)]
#![allow(missing_docs)]
#![allow(non_camel_case_types)]
#![allow(non_snake_case)]
#![allow(non_upper_case_globals)]
#![allow(trivial_casts)]
#![allow(unsafe_code)]
#![allow(unused_imports)]
#![allow(unused_results)]

const METHOD_KV_OPERATION_PUT: ::grpcio::Method<super::record::PutKvRequest, super::record::PutKvResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/KvOperation/Put",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_KV_OPERATION_GET: ::grpcio::Method<super::record::GetKvRequest, super::record::GetKvResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/KvOperation/Get",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_KV_OPERATION_DELETE: ::grpcio::Method<super::record::DeleteKvRequest, super::record::DeleteKvResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/KvOperation/Delete",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

const METHOD_KV_OPERATION_SCAN: ::grpcio::Method<super::record::ScanKvRequest, super::record::ScanKvResponse> = ::grpcio::Method {
    ty: ::grpcio::MethodType::Unary,
    name: "/KvOperation/Scan",
    req_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
    resp_mar: ::grpcio::Marshaller { ser: ::grpcio::pb_ser, de: ::grpcio::pb_de },
};

#[derive(Clone)]
pub struct KvOperationClient {
    client: ::grpcio::Client,
}

impl KvOperationClient {
    pub fn new(channel: ::grpcio::Channel) -> Self {
        KvOperationClient {
            client: ::grpcio::Client::new(channel),
        }
    }

    pub fn put_opt(&self, req: &super::record::PutKvRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::record::PutKvResponse> {
        self.client.unary_call(&METHOD_KV_OPERATION_PUT, req, opt)
    }

    pub fn put(&self, req: &super::record::PutKvRequest) -> ::grpcio::Result<super::record::PutKvResponse> {
        self.put_opt(req, ::grpcio::CallOption::default())
    }

    pub fn put_async_opt(&self, req: &super::record::PutKvRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::record::PutKvResponse>> {
        self.client.unary_call_async(&METHOD_KV_OPERATION_PUT, req, opt)
    }

    pub fn put_async(&self, req: &super::record::PutKvRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::record::PutKvResponse>> {
        self.put_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_opt(&self, req: &super::record::GetKvRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::record::GetKvResponse> {
        self.client.unary_call(&METHOD_KV_OPERATION_GET, req, opt)
    }

    pub fn get(&self, req: &super::record::GetKvRequest) -> ::grpcio::Result<super::record::GetKvResponse> {
        self.get_opt(req, ::grpcio::CallOption::default())
    }

    pub fn get_async_opt(&self, req: &super::record::GetKvRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::record::GetKvResponse>> {
        self.client.unary_call_async(&METHOD_KV_OPERATION_GET, req, opt)
    }

    pub fn get_async(&self, req: &super::record::GetKvRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::record::GetKvResponse>> {
        self.get_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn delete_opt(&self, req: &super::record::DeleteKvRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::record::DeleteKvResponse> {
        self.client.unary_call(&METHOD_KV_OPERATION_DELETE, req, opt)
    }

    pub fn delete(&self, req: &super::record::DeleteKvRequest) -> ::grpcio::Result<super::record::DeleteKvResponse> {
        self.delete_opt(req, ::grpcio::CallOption::default())
    }

    pub fn delete_async_opt(&self, req: &super::record::DeleteKvRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::record::DeleteKvResponse>> {
        self.client.unary_call_async(&METHOD_KV_OPERATION_DELETE, req, opt)
    }

    pub fn delete_async(&self, req: &super::record::DeleteKvRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::record::DeleteKvResponse>> {
        self.delete_async_opt(req, ::grpcio::CallOption::default())
    }

    pub fn scan_opt(&self, req: &super::record::ScanKvRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<super::record::ScanKvResponse> {
        self.client.unary_call(&METHOD_KV_OPERATION_SCAN, req, opt)
    }

    pub fn scan(&self, req: &super::record::ScanKvRequest) -> ::grpcio::Result<super::record::ScanKvResponse> {
        self.scan_opt(req, ::grpcio::CallOption::default())
    }

    pub fn scan_async_opt(&self, req: &super::record::ScanKvRequest, opt: ::grpcio::CallOption) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::record::ScanKvResponse>> {
        self.client.unary_call_async(&METHOD_KV_OPERATION_SCAN, req, opt)
    }

    pub fn scan_async(&self, req: &super::record::ScanKvRequest) -> ::grpcio::Result<::grpcio::ClientUnaryReceiver<super::record::ScanKvResponse>> {
        self.scan_async_opt(req, ::grpcio::CallOption::default())
    }
    pub fn spawn<F>(&self, f: F) where F: ::futures::Future<Item = (), Error = ()> + Send + 'static {
        self.client.spawn(f)
    }
}

pub trait KvOperation {
    fn put(&mut self, ctx: ::grpcio::RpcContext, req: super::record::PutKvRequest, sink: ::grpcio::UnarySink<super::record::PutKvResponse>);
    fn get(&mut self, ctx: ::grpcio::RpcContext, req: super::record::GetKvRequest, sink: ::grpcio::UnarySink<super::record::GetKvResponse>);
    fn delete(&mut self, ctx: ::grpcio::RpcContext, req: super::record::DeleteKvRequest, sink: ::grpcio::UnarySink<super::record::DeleteKvResponse>);
    fn scan(&mut self, ctx: ::grpcio::RpcContext, req: super::record::ScanKvRequest, sink: ::grpcio::UnarySink<super::record::ScanKvResponse>);
}

pub fn create_kv_operation<S: KvOperation + Send + Clone + 'static>(s: S) -> ::grpcio::Service {
    let mut builder = ::grpcio::ServiceBuilder::new();
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_KV_OPERATION_PUT, move |ctx, req, resp| {
        instance.put(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_KV_OPERATION_GET, move |ctx, req, resp| {
        instance.get(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_KV_OPERATION_DELETE, move |ctx, req, resp| {
        instance.delete(ctx, req, resp)
    });
    let mut instance = s.clone();
    builder = builder.add_unary_handler(&METHOD_KV_OPERATION_SCAN, move |ctx, req, resp| {
        instance.scan(ctx, req, resp)
    });
    builder.build()
}
