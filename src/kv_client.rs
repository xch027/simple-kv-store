extern crate grpcio;
extern crate protos;
extern crate rand;

use std::sync::Arc;
use std::thread;
use std::time::Duration;
use std::mem::transmute;

use rand::Rng;

use grpcio::{ChannelBuilder, EnvBuilder};

use protos::record::{KeyType, OperationType, OperationStatus, Key, KvEntry, PutKvRequest, GetKvRequest, DeleteKvRequest, ScanKvRequest};
use protos::record_grpc::{KvOperationClient};

// Todo: move configs to config.toml
const THREAD_NUM: i32 = 400;
const SLEEP_TIME_MILLIS: u64 = 0;
// 256 B
const MAX_KEY_SIZE: i32 = 256;
// 3k + 2b
const MAX_VALUE_SIZE: i32 = 3 * 1024 + 2;


fn create_key (k : Vec<u8>) -> Key {
    let mut key = Key::new();
    key.set_userKey(k);
    key.set_keyType(KeyType::DEFAULT_KEY);
    key
}

fn create_kv_entry( k : Vec<u8>, v : Vec<u8>) -> KvEntry {
    let mut kv_entry = KvEntry::new();
    let key = create_key(k);
    kv_entry.set_key(key);
    kv_entry.set_value(v);
    kv_entry
}

fn create_channels (port : i32) -> KvOperationClient{
    let env = Arc::new(EnvBuilder::new().build());
    let ch = ChannelBuilder::new(env).connect(format!("localhost:{}", port).as_str());
    let client = KvOperationClient::new(ch);
    client
}

fn generate_random_bytes (capacity : i32) -> Vec<u8> {
    let random_bytes: Vec<u8> = (0..capacity).map(|_| { rand::random::<u8>() }).collect();
    random_bytes
}

fn put_kv_single_test_with_key_value_size (client: KvOperationClient, key_size: i32, value_size: i32) {
    // put_kv test
    let mut put_kv_request = PutKvRequest::new();
    put_kv_request.set_field_type(OperationType::PUT);
    put_kv_request.set_entry(create_kv_entry(generate_random_bytes(key_size),
                                             generate_random_bytes(value_size)));

    let put_kv_response = client.put(&put_kv_request).expect("RPC Failed");
    println!("Received put_kv_response = {:?}", put_kv_response.get_status());
    assert_eq!(put_kv_response.get_status(), OperationStatus::SUCCESS);
}

fn put_kv_single_test_with_key_value (client: KvOperationClient, key: Vec<u8>, value: Vec<u8>) {
    // put_kv test
    let mut put_kv_request = PutKvRequest::new();
    put_kv_request.set_field_type(OperationType::PUT);
    put_kv_request.set_entry(create_kv_entry(key, value));

    let put_kv_response = client.put(&put_kv_request).expect("RPC Failed");
    println!("Received put_kv_response = {:?}", put_kv_response.get_status());
//    assert_eq!(put_kv_response.get_status(), OperationStatus::SUCCESS);
}

fn get_kv_single_test_with_key (client: KvOperationClient, key: Vec<u8>) -> Vec<u8>{
    // get_kv test
    let mut get_kv_request = GetKvRequest::new();
    get_kv_request.set_field_type(OperationType::GET);
    get_kv_request.set_key(create_key(key));

    let get_kv_response = client.get(&get_kv_request).expect("RPC Failed");
    println!("Received get_kv_response = {:?}", get_kv_response.get_status());
//    assert_eq!(get_kv_response.get_status(), OperationStatus::SUCCESS);

    get_kv_response.value.as_slice().to_vec()
}

fn delete_kv_single_test_with_key (client: KvOperationClient, key: Vec<u8>){
    // delete_kv test
    let mut delete_kv_request = DeleteKvRequest::new();
    delete_kv_request.set_field_type(OperationType::DELETE);
    delete_kv_request.set_key(create_key(key));
    let delete_kv_response = client.delete(&delete_kv_request).expect("RPC Failed");
    println!("Received delete_kv_response = {:?}", delete_kv_response.get_status());
//    assert_eq!(delete_kv_response.get_status(), OperationStatus::SUCCESS);
}

fn scan_kv_single_test_with_key (client: KvOperationClient, key: Vec<u8>) -> Vec<u8>{
    // scan_kv test
    let mut scan_kv_request = ScanKvRequest::new();
    scan_kv_request.set_field_type(OperationType::SCAN);
    scan_kv_request.set_key(create_key(key));
    let scan_kv_response = client.scan(&scan_kv_request).expect("RPC Failed");
    println!("Received scan_kv_response = {:?}", scan_kv_response.get_status());
//    println!("Received scan_kv = {:?}", scan_kv_response.get_entries());
    println!("Received kv entries number = {:?}", scan_kv_response.get_entries().len());
    println!("Received scan_kv_token = {:?}", scan_kv_response.get_token());
//    assert_eq!(delete_kv_response.get_status(), OperationStatus::SUCCESS);

    scan_kv_response.get_token().get_userKey().to_vec()
}

fn create_put_kv_request (key_size: i32, value_size: i32) -> PutKvRequest{
    let mut put_kv_request = PutKvRequest::new();
    put_kv_request.set_field_type(OperationType::PUT);
    let key_random_bytes = generate_random_bytes(key_size);
    let value_random_bytes = generate_random_bytes(value_size);
    put_kv_request.set_entry(create_kv_entry(key_random_bytes, value_random_bytes));
    put_kv_request
}

fn multithreading_put_kv_test (threads_num : i32, port : i32, sleep : u64){
    // multithreading
    let mut threads = Vec::new();
    for i in 0..threads_num {
        threads.push(thread::spawn(move || {
            println!("hi number {} from the spawned thread!", i);
            let client = create_channels(port);
            let put_kv_request = create_put_kv_request(MAX_KEY_SIZE, MAX_VALUE_SIZE);
            let put_kv_response = client.put(&put_kv_request).expect("RPC Failed");
            println!("Received put_kv_response = {:?}", put_kv_response.get_status());

            thread::sleep(Duration::from_millis(sleep));
        }));
    }

    for thread in threads {
        thread.join().unwrap();
    }
}

fn main() {
    let port = 3334;

    // <key, value> put/get/delete/scan test to verify correctness
    let large_key = generate_random_bytes(MAX_KEY_SIZE);
    let large_value = generate_random_bytes(MAX_VALUE_SIZE);
    put_kv_single_test_with_key_value(create_channels(port), large_key.clone(), large_value.clone());
    let ret_value = get_kv_single_test_with_key(create_channels(port), large_key.clone());
    if ret_value.as_slice().eq(large_value.as_slice()) {
        println!("The value is the same.");
    } else {
        println!("Fatal: the value is not the same.");
        println!("{:?}", ret_value);
        println!("{:?}", large_value);
    }
    scan_kv_single_test_with_key(create_channels(port), large_key.clone());
    delete_kv_single_test_with_key(create_channels(port), large_key);
    delete_kv_single_test_with_key(create_channels(port), b"xiao".to_vec());

//    // scan test to verify correctness
//    for i in 0..20 {
//        put_kv_single_test_with_key_value(create_channels(port),
//                                          ("xiao".to_owned() + &i.to_string()).as_bytes().to_vec(),
//                                          ("chen".to_owned() + &i.to_string()).as_bytes().to_vec());
//    }
//    let token = scan_kv_single_test_with_key(create_channels(port), b"xiao0".to_vec());


//    multithreading_put_kv_test(THREAD_NUM, port, SLEEP_TIME_MILLIS);
}