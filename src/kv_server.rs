extern crate futures;
extern crate grpcio;
extern crate protos;
extern crate crypto;
extern crate rocksdb;

use crypto::digest::Digest;
use crypto::sha2::Sha256;
#[macro_use]
extern crate log;
#[path = "log_util.rs"]
mod log_util;

use std::collections::HashMap;
use std::io::Read;
use std::sync::Arc;
use std::{io, thread};
use rocksdb::{DB, Options, ColumnFamilyDescriptor, WriteBatch, Direction, IteratorMode};

use futures::sync::oneshot;
use futures::Future;
use grpcio::{Environment, RpcContext, ServerBuilder, UnarySink};

use protos::record::*;
use protos::record_grpc::{self, KvOperation};

use protobuf::Message;

#[derive(Clone)]
struct KvOperationService;

const DB_PATH: &str = "/tmp/rocksdb.1";
const SECONDARY_DB_PATH: &str = "/tmp/rocksdb.2";
// 256 B
const SLICE_SIZE: usize = 256;

fn get_db_with_configure() -> DB {
    // Todo: add more db configures
    let mut cf_opts = Options::default();
    cf_opts.set_max_write_buffer_number(16);
    let cf = ColumnFamilyDescriptor::new("cf1", cf_opts);

    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    DB::open_cf_descriptors(&opts, DB_PATH, vec![cf]).unwrap()
}

fn get_secondary_db_with_configure() -> DB {
    // Todo: add more db configures
    let mut cf_opts = Options::default();
    cf_opts.set_max_write_buffer_number(16);
    let cf = ColumnFamilyDescriptor::new("cf1", cf_opts);

    let mut opts = Options::default();
    opts.create_if_missing(true);
    opts.create_missing_column_families(true);

    DB::open_cf_descriptors(&opts, SECONDARY_DB_PATH, vec![cf]).unwrap()
}

fn create_slice_info(key_hash: String, index: u32, slice_hash: String) -> SliceInfo {
    let mut slice_info = SliceInfo::new();
    slice_info.set_key_hash(key_hash);
    slice_info.set_index(index);
    slice_info.set_slice_hash(slice_hash);

    slice_info
}

fn create_hash(v: &[u8]) -> String {
    let mut sha = Sha256::new();
    sha.input(v);
    let v_hash = sha.result_str();

    v_hash
}

impl KvOperation for KvOperationService{
    fn put(
        &mut self,
        ctx: RpcContext,
        put_kv_request: PutKvRequest,
        sink: UnarySink<PutKvResponse>
    ){
        let mut op_success_flag = true;
        let mut put_kv_response = PutKvResponse::new();
        let kv_entry = put_kv_request.get_entry();
        let key = kv_entry.get_key();
        let value = kv_entry.get_value();
        let key_hash = create_hash(key.clone());

        let mut value_info = ValueInfo::new();

        if value.len() > SLICE_SIZE {
            value_info.set_value_sliced(true);

            let db_2 = get_secondary_db_with_configure();
            let mut batch = WriteBatch::default();
            let mut slice_info_vec = protobuf::RepeatedField::new();
            let num_slice = value.len() / SLICE_SIZE;
            let last_value_size = value.len() % SLICE_SIZE;

            for i in 0..num_slice {
                let temp = &value[(i * SLICE_SIZE)..(i + 1) * SLICE_SIZE];
                let temp_hash = create_hash(temp);
                let slice_info = create_slice_info(key_hash.clone(),
                                                   i as u32,
                                                   temp_hash);

                // put <slice_info, slice_value>
                match batch.put(slice_info.clone().write_to_bytes().unwrap().as_slice(), temp) {
                    Ok(()) => info!("successfully batched <slice_info, slice_value>"),
                    Err(e) => {
                        error!("operational problem encountered: {}", e);
                        put_kv_response.set_status(String::from("put <slice_info, slice_value> failed!"));
                        op_success_flag = false;
                        break;
                    },
                }

                slice_info_vec.push(slice_info);
            }
            if op_success_flag {
                if last_value_size != 0 {
                    let last_value = &value[(value.len() - last_value_size)..value.len()];
//                println!("successfully put <slice_info, slice_value>! last sliced value = {:?}", last_value.clone());
                    value_info.set_last_value(last_value.to_vec());
                }
                value_info.set_slice_info(slice_info_vec);

                // batch put metadata to db_2
                match db_2.write(batch) {
                    Ok(()) => {
                        info!("successfully batch put <slice_info, slice_value>");
                    },
                    Err(e) => {
                        error!("operational problem encountered: {}", e);
                        put_kv_response.set_status(String::from("put <slice_info, slice_value> failed!"));
                    },
                }
            }
        } else {
            value_info.set_value_sliced(false);
            value_info.set_last_value(value.to_vec());
        }

        if op_success_flag {
            let db = get_db_with_configure();
            // put <key, value_info>
            match db.put(key, value_info.write_to_bytes().unwrap().as_slice()) {
                Ok(()) => {
                    info!("successfully put <key, value_info>");
                    put_kv_response.set_status(String::from("successfully put <key, value>!"));
                },
                Err(e) => {
                    error!("operational problem encountered: {}", e);
                    put_kv_response.set_status(String::from("put <key, value> failed!"));
                },
            }
        }

        let f = sink
            .success(put_kv_response.clone())
            .map(move |_| info!("Responded with put_kv_response {:?}", put_kv_response.get_status()))
            .map_err(move |err| error!("Failed to reply: {:?}", err));
        ctx.spawn(f)
    }

    fn get(
        &mut self,
        ctx: RpcContext,
        get_kv_request: GetKvRequest,
        sink: UnarySink<GetKvResponse>
    ){
        let mut op_success_flag = true;
        let mut get_kv_response = GetKvResponse::new();

        let key = get_kv_request.get_key();
        let db = get_db_with_configure();
        // get <key, vaue_info>
        match db.get(key) {
            Ok(Some(value)) => {
                info!("get: retrieved <key, value_info>");
                let value_info: ValueInfo = protobuf::parse_from_bytes(value.to_vec().as_slice()).unwrap();
                if !value_info.get_value_sliced() {
                    get_kv_response.set_value(value_info.get_last_value().to_vec());
                } else {
                    let db_2 = get_secondary_db_with_configure();

                    let slice_info_keys = value_info.get_slice_info();
                    let mut r_value: Vec<u8> = vec![];
                    for i in 0..slice_info_keys.len() {
                        let slice_info = slice_info_keys.get(i).unwrap();

                        if slice_info.get_key_hash().ne(&create_hash(key))
                        || slice_info.get_index() != i as u32 {
                            op_success_flag = false;
                            break;
                        }
                        // get <slice_info, slice_value>
                        match db_2.get(slice_info.write_to_bytes().unwrap().as_slice()) {
                            Ok(Some(z_value)) => {
                                let mut slice_value = z_value.to_vec();
                                if slice_info.get_slice_hash().ne(&create_hash(slice_value.as_slice())) {
                                    op_success_flag = false;
                                    break;
                                }
                                info!("get: retrieved <slice_info, slice_value>");
                                r_value.append(&mut slice_value);
                            },
                            Ok(None) => {
                                warn!("get: value not found");
                                op_success_flag = false;
                                break;
                            },
                            Err(e) => {
                                error!("get: operational problem encountered: {}", e);
                                op_success_flag = false;
                                break;
                            },
                        }
                    }
                    if op_success_flag {
                        let mut last_value = value_info.get_last_value().to_vec();
                        r_value.append(&mut last_value);
                        get_kv_response.set_value(r_value);
                    }
                }
            },
            Ok(None) => {
                warn!("get: value not found");
            },
            Err(e) => {
                error!("get: operational problem encountered: {}", e);
            },
        }

        let f = sink
            .success(get_kv_response.clone())
            .map(move |_| info!("Responded with value {:?}", get_kv_response.get_value()))
            .map_err(move |err| error!("Failed to reply: {:?}", err));
        ctx.spawn(f)
    }

    fn delete(
        &mut self,
        ctx: RpcContext,
        delete_kv_request: DeleteKvRequest,
        sink: UnarySink<DeleteKvResponse>
    ){
        let mut delete_kv_response = DeleteKvResponse::new();

        let mut op_success_flag = true;
        let key = delete_kv_request.get_key();
        let db = get_db_with_configure();
        match db.get(key) {
            Ok(Some(value)) => {
                let value_info: ValueInfo = protobuf::parse_from_bytes(value.to_vec().as_slice()).unwrap();
                if !value_info.get_value_sliced() {
                    match db.delete(key) {
                        Ok(()) => {
                            delete_kv_response.set_status(String::from("successfully delete <key, value>!"));
                        },
                        Err(e) => {
                            error!("delete: operational problem encountered: {}", e);
                            op_success_flag = false;
                            delete_kv_response.set_status(String::from("delete <key, value> failed!"));
                        },
                    }
                } else {
                    let db_2 = get_secondary_db_with_configure();

                    let slice_info_keys= value_info.get_slice_info();
                    for i in 0..slice_info_keys.len() {
                        let slice_info = slice_info_keys.get(i).unwrap();

                        if slice_info.get_key_hash().ne(&create_hash(key))
                            || slice_info.get_index() != i as u32 {
                            delete_kv_response.set_status(String::from("delete <key, value> failed!"));
                            op_success_flag = false;
                            break;
                        }

                        // Todo: error handling if sth wrong in db.delete
                        // delete <slice_info, slice_value>
                        match db_2.delete(slice_info.write_to_bytes().unwrap().as_slice()) {
                            Ok(()) => {
                                info!("successfully deleted <slice_key, slice_value>!");
                            },
                            Err(e) => {
                                error!("delete: operational problem encountered: {}", e);
                                delete_kv_response.set_status(String::from("delete <key, value> failed!"));
                                op_success_flag = false;
                                break;
                            },
                        }
                    }
                }
            },
            Ok(None) => {
                warn!("delete: value not found, no such key!");
                op_success_flag = false;
                delete_kv_response.set_status(String::from("delete <key, value> failed!"));
            },
            Err(e) => {
                error!("delete: operational problem encountered: {}", e);
                op_success_flag = false;
                delete_kv_response.set_status(String::from("delete <key, value> failed!"));
            },
        }

        if op_success_flag {
            // delete <key, value_info>
            match db.delete(key) {
                Ok(()) => {
                    info!("successfully delete <key, value_info>");
                    delete_kv_response.set_status(String::from("successfully delete <key, value>"));
                },
                Err(e) => {
                    error!("delete: operational problem encountered: {}", e);
                    delete_kv_response.set_status(String::from("delete <key, value> failed!"));
                },
            }
        }

        let f = sink
            .success(delete_kv_response.clone())
            .map(move |_| info!("Responded with delete_kv_response {:?}", delete_kv_response.get_status()))
            .map_err(move |err| error!("Failed to reply: {:?}", err));
        ctx.spawn(f)
    }

    fn scan(
        &mut self,
        ctx: RpcContext,
        scan_kv_request: ScanKvRequest,
        sink: UnarySink<ScanKvResponse>
    ){
        let mut scan_kv_response = ScanKvResponse::new();
        let mut num_iter = 0;
        let mut kv_entry_vec = protobuf::RepeatedField::new();

        let key = scan_kv_request.get_start_key();
        let limit = scan_kv_request.get_limit();
        let direction = if scan_kv_request.get_reverse() {
            Direction::Reverse
        } else {
            Direction::Forward
        };
        let key_only = scan_kv_request.get_key_only();

        let db = get_db_with_configure();
        let iter = db.iterator(
            IteratorMode::From(
                key,
                direction
            )
        );

        if key_only {
            for (key, _) in iter {
                num_iter += 1;
                if num_iter > limit {
                    break;
                }

                let mut kv = KvEntry::new();
                let t_key = key.into_vec();
                kv.set_key(t_key);
                kv_entry_vec.push(kv);
            }

        } else {
            for (key, value) in iter {
                num_iter += 1;
                if num_iter > limit {
                    break;
                }

                let temp_key: Vec<u8> = key.to_vec();
                let value_info: ValueInfo = protobuf::parse_from_bytes(value.to_vec().as_slice()).unwrap();

                if !value_info.get_value_sliced() {
                    let mut kv = KvEntry::new();
                    kv.set_key(temp_key.clone());
                    kv.set_value(value_info.get_last_value().to_vec());

                    kv_entry_vec.push(kv);

                } else {
                    let db_2 = get_secondary_db_with_configure();

                    let slice_info_keys = value_info.get_slice_info();


                    let iter_db_2 = db_2.iterator(
                        IteratorMode::From(
                            slice_info_keys.get(0).unwrap().write_to_bytes().unwrap().as_slice(),
                            Direction::Forward
                        )
                    );

                    let mut value_map = HashMap::new();;
                    for (slice_info_key, slice_value) in iter_db_2 {
                        let slice_info: SliceInfo = protobuf::parse_from_bytes(slice_info_key.to_vec().as_slice()).unwrap();
                        if slice_info.get_key_hash().eq(&create_hash(temp_key.as_slice()))
                        && slice_info.get_slice_hash().eq(&create_hash(slice_value.to_vec().as_slice())){
                            value_map.insert(slice_info.index, slice_value.to_vec());
                        }
                    }

                    let mut r_value: Vec<u8> = vec![];
                    for i in 0..value_map.len(){
                        r_value.extend(value_map.get(&(i as u32)).unwrap());
                    }

                    let last_value = value_info.get_last_value().to_vec();
                    r_value.extend(last_value);

                    let mut kv = KvEntry::new();
                    kv.set_key(temp_key.clone());
                    kv.set_value(r_value.to_vec());

                    kv_entry_vec.push(kv);

                }
            }
        }

        scan_kv_response.set_entries(kv_entry_vec);

        let f = sink
            .success(scan_kv_response.clone())
            .map(move |_| info!("Scan responded with {:?} kv pairs", scan_kv_response.get_entries().len()))
            .map_err(move |err| error!("Failed to reply: {:?}", err));
        ctx.spawn(f)
    }
}



fn main() {
    let _guard = log_util::init_log(None);
    let env = Arc::new(Environment::new(1));
    let service = record_grpc::create_kv_operation(KvOperationService);
    let mut server = ServerBuilder::new(env)
        .register_service(service)
        .bind("127.0.0.1", 3_334)
        .build()
        .unwrap();
    server.start();

    for &(ref host, port) in server.bind_addrs() {
        info!("{} listening on {}:{}", "kv_operation", host, port);
    }

    let (tx, rx) = oneshot::channel();
    thread::spawn(move || {
        info!("Press ENTER to exit...");
        let _ = io::stdin().read(&mut [0]).unwrap();
        tx.send(())
    });
    let _ = rx.wait();
    let _ = server.shutdown().wait();
}
