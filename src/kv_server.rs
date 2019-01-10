extern crate futures;
extern crate grpcio;
extern crate protos;
extern crate crypto;

use crypto::digest::Digest;
use crypto::sha2::Sha256;

use std::io::Read;
use std::sync::Arc;
use std::{io, thread};
use rocksdb::{DB, Writable};
use rocksdb::DBCompactionStyle::Universal;

use futures::sync::oneshot;
use futures::Future;
use grpcio::{Environment, RpcContext, ServerBuilder, UnarySink};

use protos::record::*;
use protos::record_grpc::{self, KvOperation};

use protobuf::Message;

#[derive(Clone)]
struct KvOperationService;

static DB_PATH: &str = "/tmp/rocksdb.1";
// 1k
static SLICE_SIZE: usize = 1024;

fn create_slice_info(key_hash: String, index: u32, offset: u32, length: u32, slice_hash: String) -> SliceInfo{
    let mut slice_info = SliceInfo::new();
    slice_info.set_keyHash(key_hash);
    slice_info.set_index(index);
    slice_info.set_offset(offset);
    slice_info.set_length(length);
    slice_info.set_sliceHash(slice_hash);

    return slice_info;
}

//fn create_value_info (value_sliced: bool, slices: u32, value: bytes) -> ValueInfo{
//    let mut value_info = ValueInfo::new();
//    value_info.set_valueSliced(value_sliced);
//    for i in 0..slices {
//
//    }
//    value_info.set_value(value);
//    return value_info;
//}

fn create_hash(k: &[u8]) -> String {
    let mut sha = Sha256::new();
    sha.input(k);
    let key_hash = sha.result_str();

    return key_hash;
}

impl KvOperation for KvOperationService{
    fn put(&mut self, ctx: RpcContext, put_kv_request: PutKvRequest, sink: UnarySink<PutKvResponse>){

        let operation_type = put_kv_request.get_field_type();
        let kv_entry = put_kv_request.get_entry();
        let key = kv_entry.get_key().get_userKey();
        let value = kv_entry.get_value();
        let key_hash = create_hash(key);

        let mut value_info = ValueInfo::new();


        let db = DB::open_default(DB_PATH).unwrap();
        let mut put_kv_response = PutKvResponse::new();

        if value.len() > SLICE_SIZE{
            value_info.set_valueSliced(true);

            let mut slice_info_vec = protobuf::RepeatedField::new();
            let num_slice = value.len() / SLICE_SIZE;
            let last_value_size = value.len() % SLICE_SIZE;
            for i in 0..num_slice {
                let temp = &value[(i * SLICE_SIZE) .. (i + 1) * SLICE_SIZE];
                let temp_hash = create_hash(temp);
                let slice_info = create_slice_info(key_hash.clone(),
                                                   i as u32,
                                                   (i*SLICE_SIZE) as u32,
                                                   SLICE_SIZE as u32,
                                                   temp_hash);
                slice_info_vec.push(slice_info.clone());
                // put <slice_info, slice_value>
                match db.put(slice_info.write_to_bytes().unwrap().as_slice(), temp){
                    Ok(()) => println!("successfully put <slice_info, slice_value>!"),
//                    Ok(()) => println!("successfully put <slice_info, slice_value>! sliced value = {:?}", temp),
                    Err(e) => {
                        println!("operational problem encountered: {}", e);
                        put_kv_response.set_status(OperationStatus::ERROR);
                    },
                }
            }
            if last_value_size != 0 {
                let last_value = &value[(value.len() - last_value_size)..value.len()];
//                println!("successfully put <slice_info, slice_value>! last sliced value = {:?}", last_value.clone());
                value_info.set_lastValue(last_value.to_vec());
            }
            value_info.set_sliceInfo(slice_info_vec);

        } else {
            value_info.set_valueSliced(false);
            value_info.set_lastValue(value.to_vec());
        }


        match db.put(key, value_info.write_to_bytes().unwrap().as_slice()) {
            Ok(()) => {
                println!("successfully put <key, value_info>");
                put_kv_response.set_status(OperationStatus::SUCCESS);
            },
            Err(e) => {
                println!("operational problem encountered: {}", e);
                put_kv_response.set_status(OperationStatus::ERROR);
            },
        }

        let f = sink
            .success(put_kv_response.clone())
            .map(move |_| println!("Responded with put_kv_response {:?}", put_kv_response.get_status()))
            .map_err(move |err| eprintln!("Failed to reply: {:?}", err));
        ctx.spawn(f)
    }

    fn get(&mut self, ctx: RpcContext, get_kv_request: GetKvRequest, sink: UnarySink<GetKvResponse>){
        let key = get_kv_request.get_key().get_userKey();
        let operation_type = get_kv_request.get_field_type();
//        println!("Received key = {:?}, operation type = {:?}", String::from_utf8_lossy(key), operation_type);

        let mut get_kv_response = GetKvResponse::new();
        let db = DB::open_default(DB_PATH).unwrap();
        match db.get(key) {
            Ok(Some(value)) => {
                println!("retrieved <key, value_info>");
                let value_info:ValueInfo = protobuf::parse_from_bytes(value.to_vec().as_slice()).unwrap();
                if !value_info.get_valueSliced() {
                    get_kv_response.set_value(value_info.get_lastValue().to_vec());
                    get_kv_response.set_status(OperationStatus::SUCCESS);
                } else {
                    let slice_infos = value_info.get_sliceInfo();
                    let mut r_value: Vec<u8> = vec![];
                    for i in 0..slice_infos.len() {
                        let sli_in: &SliceInfo = slice_infos.get(i).unwrap();
                        if sli_in.get_keyHash().ne(&create_hash(key.clone())){
                            get_kv_response.set_status(OperationStatus::ERROR);
                            break;
                        }
                        match db.get(sli_in.write_to_bytes().unwrap().as_slice()) {
                            Ok(Some(z_value)) => {
//                                println!("retrieved <slice_info, slice_value>");
                                let mut slice_value = z_value.to_vec();
                                println!("retrieved <slice_info, slice_value>.");
//                                println!("retrieved <slice_info, slice_value>. Sliced value = {:?}", slice_value);
                                r_value.append(&mut slice_value);
                            },
                            Ok(None) => {
                                println!("value not found");
                                get_kv_response.set_status(OperationStatus::ERROR);
                                break;
                            },
                            Err(e) => {
                                println!("operational problem encountered: {}", e);
                                get_kv_response.set_status(OperationStatus::ERROR);
                                break;
                            },
                        }

                    }
                    let mut last_value = value_info.get_lastValue().to_vec();
                    r_value.append(&mut last_value);
//                    println!("total value = {:?}", r_value.clone());
                    get_kv_response.set_value(r_value);
                    get_kv_response.set_status(OperationStatus::SUCCESS);
                }

            },
            Ok(None) => {
                println!("value not found");
                get_kv_response.set_status(OperationStatus::ERROR);
            },
            Err(e) => {
                println!("operational problem encountered: {}", e);
                get_kv_response.set_status(OperationStatus::ERROR);
            },
        }

        let f = sink
            .success(get_kv_response.clone())
            .map(move |_| println!("Responded with get_kv_response {:?}", get_kv_response.get_status()))
            .map_err(move |err| eprintln!("Failed to reply: {:?}", err));
        ctx.spawn(f)
    }

    fn delete(&mut self, ctx: RpcContext, delete_kv_request: DeleteKvRequest, sink: UnarySink<DeleteKvResponse>){
        let key = delete_kv_request.get_key().get_userKey();
        let operation_type = delete_kv_request.get_field_type();
//        println!("Received key = {:?}, operation type = {:?}", String::from_utf8_lossy(key), operation_type);

        let mut delete_kv_response = DeleteKvResponse::new();
        let db = DB::open_default(DB_PATH).unwrap();
        match db.delete(key) {
            Ok(()) => {
//                println!("successfully delete!");
                delete_kv_response.set_status(OperationStatus::SUCCESS);
            },
            Err(e) => {
                println!("operational problem encountered: {}", e);
                delete_kv_response.set_status(OperationStatus::ERROR);
            },
        }

        let f = sink
            .success(delete_kv_response.clone())
            .map(move |_| println!("Responded with delete_kv_response {:?}", delete_kv_response.get_status()))
            .map_err(move |err| eprintln!("Failed to reply: {:?}", err));
        ctx.spawn(f)
    }

    fn scan(&mut self, ctx: RpcContext, get_kv_request: ScanKvRequest, sink: UnarySink<ScanKvResponse>){
        // WIP
    }
}



fn main() {
    let env = Arc::new(Environment::new(1));
    let service = record_grpc::create_kv_operation(KvOperationService);
    let mut server = ServerBuilder::new(env)
        .register_service(service)
        .bind("127.0.0.1", 3334)
        .build()
        .unwrap();
    server.start();

    for &(ref host, port) in server.bind_addrs() {
        println!("{} listening on {}:{}", "kv_operation", host, port);
    }

    let (tx, rx) = oneshot::channel();
    thread::spawn(move || {
        println!("Press ENTER to exit...");
        let _ = io::stdin().read(&mut [0]).unwrap();
        tx.send(())
    });
    let _ = rx.wait();
    let _ = server.shutdown().wait();
}
