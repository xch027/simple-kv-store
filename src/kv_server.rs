extern crate futures;
extern crate grpcio;
extern crate protos;
extern crate crypto;
extern crate rocksdb;

use crypto::digest::Digest;
use crypto::sha2::Sha256;

use std::io::Read;
use std::sync::Arc;
use std::{io, thread};
use rocksdb::{DB, WriteBatch, Direction, IteratorMode};
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
// 256 B
static SLICE_SIZE: usize = 256;
// max key number in scan
static SCAN_MAX_KEYS: usize = 10;

fn create_slice_info(key_hash: String, index: u32, offset: u32, length: u32, slice_hash: String) -> SliceInfo{
    let mut slice_info = SliceInfo::new();
    slice_info.set_keyHash(key_hash);
    slice_info.set_index(index);
    slice_info.set_offset(offset);
    slice_info.set_length(length);
    slice_info.set_sliceHash(slice_hash);

    return slice_info;
}

fn create_hash(k: &[u8]) -> String {
    let mut sha = Sha256::new();
    sha.input(k);
    let key_hash = sha.result_str();

    return key_hash;
}

impl KvOperation for KvOperationService{
    fn put(&mut self, ctx: RpcContext, put_kv_request: PutKvRequest, sink: UnarySink<PutKvResponse>){
        let mut put_kv_response = PutKvResponse::new();

        let operation_type = put_kv_request.get_field_type();
        if operation_type != OperationType::PUT {
            put_kv_response.set_status(OperationStatus::ERROR_TYPE_INCORRECT);
        } else {
            let kv_entry = put_kv_request.get_entry();
            let key = kv_entry.get_key();
            let value = kv_entry.get_value();
            let key_hash = create_hash(key.write_to_bytes().unwrap().as_slice());

            let mut value_info = ValueInfo::new();

            let db = DB::open_default(DB_PATH).unwrap();
            let mut batch = WriteBatch::default();
            if value.len() > SLICE_SIZE {
                value_info.set_valueSliced(true);

                let mut slice_info_key_vec = protobuf::RepeatedField::new();
                let num_slice = value.len() / SLICE_SIZE;
                let last_value_size = value.len() % SLICE_SIZE;

                for i in 0..num_slice {
                    let temp = &value[(i * SLICE_SIZE)..(i + 1) * SLICE_SIZE];
                    let temp_hash = create_hash(temp);
                    let slice_info = create_slice_info(key_hash.clone(),
                                                       i as u32,
                                                       (i * SLICE_SIZE) as u32,
                                                       SLICE_SIZE as u32,
                                                       temp_hash);
                    let mut slice_info_key = Key::new();
                    slice_info_key.set_userKey(slice_info.write_to_bytes().unwrap());
                    slice_info_key.set_keyType(KeyType::SLICE_KEY);

                    // put <slice_info_key, slice_value>
                    match batch.put(slice_info_key.write_to_bytes().unwrap().as_slice(), temp){
                        Ok(()) => println!("successfully batch put <slice_info, slice_value>!"),
                        Err(e) => {
                            println!("operational problem encountered: {}", e);
                            put_kv_response.set_status(OperationStatus::ERROR_PUT_SLICE);
                        },
                    }

                    slice_info_key_vec.push(slice_info_key.clone());
                }
                if last_value_size != 0 {
                    let last_value = &value[(value.len() - last_value_size)..value.len()];
//                println!("successfully put <slice_info, slice_value>! last sliced value = {:?}", last_value.clone());
                    value_info.set_lastValue(last_value.to_vec());
                }
                value_info.set_sliceInfoKey(slice_info_key_vec);
            } else {
                value_info.set_valueSliced(false);
                value_info.set_lastValue(value.to_vec());
            }

            // put <key, value_info>
            match batch.put(key.write_to_bytes().unwrap().as_slice(), value_info.write_to_bytes().unwrap().as_slice()){
                Ok(()) => {
                    println!("successfully batch put <key, value_info>");
                },
                Err(e) => {
                    println!("operational problem encountered: {}", e);
                    put_kv_response.set_status(OperationStatus::ERROR);
                },
            }

            // instead, write batch to improve performance
            match db.write(batch){
                Ok(()) => {
                    println!("successfully put <key, value_info>");
                    put_kv_response.set_status(OperationStatus::SUCCESS);
                },
                Err(e) => {
                    println!("operational problem encountered: {}", e);
                    put_kv_response.set_status(OperationStatus::ERROR);
                },
            }
        }

        let f = sink
            .success(put_kv_response.clone())
            .map(move |_| println!("Responded with put_kv_response {:?}", put_kv_response.get_status()))
            .map_err(move |err| eprintln!("Failed to reply: {:?}", err));
        ctx.spawn(f)
    }

    fn get(&mut self, ctx: RpcContext, get_kv_request: GetKvRequest, sink: UnarySink<GetKvResponse>){
        let mut get_kv_response = GetKvResponse::new();

        let operation_type = get_kv_request.get_field_type();
        if operation_type != OperationType::GET{
            get_kv_response.set_status(OperationStatus::ERROR_TYPE_INCORRECT);
        } else {
            let key = get_kv_request.get_key();
            let db = DB::open_default(DB_PATH).unwrap();
            // get <key, vaue_info>
            match db.get(key.write_to_bytes().unwrap().as_slice()) {
                Ok(Some(value)) => {
                    println!("retrieved <key, value_info>");
                    let value_info: ValueInfo = protobuf::parse_from_bytes(value.to_vec().as_slice()).unwrap();
                    if !value_info.get_valueSliced() {
                        get_kv_response.set_value(value_info.get_lastValue().to_vec());
                        get_kv_response.set_status(OperationStatus::SUCCESS);
                    } else {
                        let slice_info_keys= value_info.get_sliceInfoKey();
                        let mut r_value: Vec<u8> = vec![];
                        for i in 0..slice_info_keys.len() {
                            let slice_info_key:&Key = slice_info_keys.get(i).unwrap();
                            let slice_info :SliceInfo = protobuf::parse_from_bytes(slice_info_key.get_userKey()).unwrap();
                            if slice_info.get_keyHash().ne(&create_hash(key.write_to_bytes().unwrap().as_slice())) {
                                get_kv_response.set_status(OperationStatus::ERROR_KEY_HASH_INCORRECT);
                                break;
                            }
                            // get <slice_info_key, slice_value>
                            match db.get(slice_info_key.write_to_bytes().unwrap().as_slice()) {
                                Ok(Some(z_value)) => {
//                                println!("retrieved <slice_info, slice_value>");
                                    let mut slice_value = z_value.to_vec();
                                    println!("retrieved <slice_info, slice_value>.");
//                                println!("retrieved <slice_info, slice_value>. Sliced value = {:?}", slice_value);
                                    r_value.append(&mut slice_value);
                                },
                                Ok(None) => {
                                    println!("value not found");
                                    get_kv_response.set_status(OperationStatus::ERROR_SLICE_NOT_FOUND);
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
                    get_kv_response.set_status(OperationStatus::ERROR_KEY_NOT_FOUND);
                },
                Err(e) => {
                    println!("operational problem encountered: {}", e);
                    get_kv_response.set_status(OperationStatus::ERROR);
                },
            }
        }

        let f = sink
            .success(get_kv_response.clone())
            .map(move |_| println!("Responded with get_kv_response {:?}", get_kv_response.get_status()))
            .map_err(move |err| eprintln!("Failed to reply: {:?}", err));
        ctx.spawn(f)
    }

    fn delete(&mut self, ctx: RpcContext, delete_kv_request: DeleteKvRequest, sink: UnarySink<DeleteKvResponse>){
        let mut delete_kv_response = DeleteKvResponse::new();

        let operation_type = delete_kv_request.get_field_type();
        if operation_type != OperationType::DELETE{
            delete_kv_response.set_status(OperationStatus::ERROR_TYPE_INCORRECT);
        } else {
            let key = delete_kv_request.get_key();
            let db = DB::open_default(DB_PATH).unwrap();
            match db.get(key.write_to_bytes().unwrap().as_slice()) {
                Ok(Some(value)) => {
                    println!("delete: retrieved <key, value_info>");
                    let value_info: ValueInfo = protobuf::parse_from_bytes(value.to_vec().as_slice()).unwrap();
                    if !value_info.get_valueSliced() {
                        match db.delete(key.write_to_bytes().unwrap().as_slice()) {
                            Ok(()) => {
//                          println!("successfully delete!");
                                delete_kv_response.set_status(OperationStatus::SUCCESS);
                            },
                            Err(e) => {
                                println!("operational problem encountered: {}", e);
                                delete_kv_response.set_status(OperationStatus::ERROR);
                            },
                        }
                    } else {
                        let slice_info_keys= value_info.get_sliceInfoKey();
                        let mut r_value: Vec<u8> = vec![];
                        for i in 0..slice_info_keys.len() {
                            let slice_info_key: &Key = slice_info_keys.get(i).unwrap();
                            let slice_info :SliceInfo = protobuf::parse_from_bytes(slice_info_key.get_userKey()).unwrap();
                            if slice_info.get_keyHash().ne(&create_hash(key.write_to_bytes().unwrap().as_slice())) {
                                delete_kv_response.set_status(OperationStatus::ERROR_KEY_HASH_INCORRECT);
                                break;
                            }
                            // get <slice_info_key, slice_value>
                            match db.get(slice_info_key.write_to_bytes().unwrap().as_slice()) {
                                Ok(Some(z_value)) => {
                                    // delete <slice_info_key, slice_value>
                                    match db.delete(slice_info_key.write_to_bytes().unwrap().as_slice()) {
                                        Ok(()) => {
                                            println!("successfully deleted slice key!");
                                        },
                                        Err(e) => {
                                            println!("operational problem encountered: {}", e);
                                            delete_kv_response.set_status(OperationStatus::ERROR);
                                            break;
                                        },
                                    }
                                },
                                Ok(None) => {
                                    println!("value not found, no such slice key!");
                                    delete_kv_response.set_status(OperationStatus::ERROR_SLICE_NOT_FOUND);
                                    break;
                                },
                                Err(e) => {
                                    println!("operational problem encountered: {}", e);
                                    delete_kv_response.set_status(OperationStatus::ERROR);
                                    break;
                                },
                            }
                        }
                        // delete <key, vaue_info>
                        match db.delete(key.write_to_bytes().unwrap().as_slice()) {
                            Ok(()) => {
                                println!("successfully delete!");
                                delete_kv_response.set_status(OperationStatus::SUCCESS);
                            },
                            Err(e) => {
                                println!("operational problem encountered: {}", e);
                                delete_kv_response.set_status(OperationStatus::ERROR);
                            },
                        }
                    }
                },
                Ok(None) => {
                    println!("value not found, no such key!");
                    delete_kv_response.set_status(OperationStatus::ERROR_KEY_NOT_FOUND);
                },
                Err(e) => {
                    println!("operational problem encountered: {}", e);
                    delete_kv_response.set_status(OperationStatus::ERROR);
                },
            }
        }

        let f = sink
            .success(delete_kv_response.clone())
            .map(move |_| println!("Responded with delete_kv_response {:?}", delete_kv_response.get_status()))
            .map_err(move |err| eprintln!("Failed to reply: {:?}", err));
        ctx.spawn(f)
    }

    fn scan(&mut self, ctx: RpcContext, scan_kv_request: ScanKvRequest, sink: UnarySink<ScanKvResponse>){
        let mut scan_kv_response = ScanKvResponse::new();

        let operation_type = scan_kv_request.get_field_type();
        if operation_type != OperationType::SCAN{
            scan_kv_response.set_status(OperationStatus::ERROR_TYPE_INCORRECT);
        } else {
            let key = scan_kv_request.get_key();
            let mut num_iter = 0;

            let db = DB::open_default(DB_PATH).unwrap();
            let mut iter = db.iterator(
                IteratorMode::From(
                    key.write_to_bytes().unwrap().as_slice(),
                    Direction::Forward
                )
            );

            let mut kv_entry_vec = protobuf::RepeatedField::new();
            for (key, value) in iter {
                num_iter += 1;
                if num_iter > SCAN_MAX_KEYS {
                    break;
                }

                let temp_key: Key = protobuf::parse_from_bytes(key.to_vec().as_slice()).unwrap();
                // get <key, slice_info> pair
                if temp_key.get_keyType() == KeyType::DEFAULT_KEY {
                    match db.get(temp_key.write_to_bytes().unwrap().as_slice()) {
                        Ok(Some(value)) => {
                            println!("scan: retrieved <key, value_info>");
                            let value_info: ValueInfo = protobuf::parse_from_bytes(value.to_vec().as_slice()).unwrap();

                            if !value_info.get_valueSliced() {
                                let mut kv = KvEntry::new();
                                kv.set_key(temp_key.clone());
                                kv.set_value(value_info.get_lastValue().to_vec());

                                kv_entry_vec.push(kv);
                            } else {
                                let slice_info_keys= value_info.get_sliceInfoKey();
                                let mut r_value: Vec<u8> = vec![];
                                for i in 0..slice_info_keys.len() {
                                    let slice_info_key:&Key = slice_info_keys.get(i).unwrap();
                                    let slice_info :SliceInfo = protobuf::parse_from_bytes(slice_info_key.get_userKey()).unwrap();
                                    if slice_info.get_keyHash().ne(&create_hash(temp_key.write_to_bytes().unwrap().as_slice())) {
                                        scan_kv_response.set_status(OperationStatus::ERROR_KEY_HASH_INCORRECT);
                                        break;
                                    }
                                    // get <slice_info_key, slice_value>
                                    match db.get(slice_info_key.write_to_bytes().unwrap().as_slice()) {
                                        Ok(Some(z_value)) => {
//                                println!("retrieved <slice_info, slice_value>");
                                            let mut slice_value = z_value.to_vec();
                                            println!("retrieved <slice_info, slice_value>.");
//                                println!("retrieved <slice_info, slice_value>. Sliced value = {:?}", slice_value);
                                            r_value.append(&mut slice_value);
                                        },
                                        Ok(None) => {
                                            println!("value not found");
                                            scan_kv_response.set_status(OperationStatus::ERROR_SLICE_NOT_FOUND);
                                            break;
                                        },
                                        Err(e) => {
                                            println!("operational problem encountered: {}", e);
                                            scan_kv_response.set_status(OperationStatus::ERROR);
                                            break;
                                        },
                                    }
                                }
                                let mut last_value = value_info.get_lastValue().to_vec();
                                r_value.append(&mut last_value);

                                let mut kv = KvEntry::new();
                                kv.set_key(temp_key.clone());
                                kv.set_value(r_value.to_vec());

                                kv_entry_vec.push(kv);
                            }

                            scan_kv_response.set_entries(kv_entry_vec.clone());
                            scan_kv_response.set_token(temp_key.clone());
                            scan_kv_response.set_status(OperationStatus::SUCCESS);
                        },
                        Ok(None) => {
                            println!("value not found");
                            scan_kv_response.set_status(OperationStatus::ERROR_KEY_NOT_FOUND);
                            break;
                        },
                        Err(e) => {
                            println!("operational problem encountered: {}", e);
                            scan_kv_response.set_status(OperationStatus::ERROR);
                            break;
                        },
                    }
                }
            }

        }

        let f = sink
            .success(scan_kv_response.clone())
            .map(move |_| println!("Responded with scan_kv_response {:?}", scan_kv_response.get_status()))
            .map_err(move |err| eprintln!("Failed to reply: {:?}", err));
        ctx.spawn(f)
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
