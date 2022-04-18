use std::path::Path;
use std::str;
use lmdb::{EnvironmentBuilder,Environment, DatabaseFlags,RwTransaction,WriteFlags, RwCursor,Cursor};
use tempfile::tempdir;

use serde_derive::Deserialize;
use serde_derive::Serialize;
extern crate serde;
use std::fs::File;
use std::io::Read;
use walkdir::{DirEntry, WalkDir};
use bincode;
#[derive(Deserialize)]
struct Response {
    dataset: Vec<Element>,
}

#[derive(Deserialize)]
struct Element {
    timestamp: u64,
    entry: String,
}

#[derive(Serialize)]
struct Timerange {
    starttime: u64,
    endtime: u64,
}
fn main() {
    //let path = Path::new("");
    //let str = "test";
    let tempdir = tempdir().unwrap();
    
    let env_builder = Environment::new();
    //let env = EnvironmentBuilder::open(&env_builder, path);
    let env = EnvironmentBuilder::open(&env_builder, tempdir.path());
    let env = match env{
        Ok(file) => file,
        Err(error) => panic!("Problem opening the file: {:?}", error),
    };
    
    //let db = Environment::create_db(&env, None, DatabaseFlags::DUP_SORT);
    let db = Environment::create_db(&env, None, DatabaseFlags::INTEGER_KEY);

    let db = match db{
        Ok(file) => file,
        Err(error) => panic!("Problem create db: {:?}", error),
    };


    let trans = Environment::begin_rw_txn(&env);
    
    let mut trans = match trans{
        Ok(file) => file,
        Err(error) => panic!("Problem begin rwTransaction: {:?}", error),
    };

    let cursor = RwTransaction::open_rw_cursor(& mut trans,db);

    let mut cursor = match cursor{
        Ok(file) => file,
        Err(error) => panic!("Problem begin rwCursor: {:?}", error),
    };

    //basic_util_test(cursor);
    //test_timeStamp(cursor);
    read_multi_json_test_timeRange(cursor);

}
fn basic_util_test(mut cursor:RwCursor ){
    let key = "1";
    let value = "cat";
    let res = RwCursor::put( &mut cursor, &key, &value, WriteFlags::APPEND);

    match res{
        Ok(file) => file,
        Err(error) => panic!("Problem with put: {:?}", error),
    };

    let pair = Cursor::get(&cursor, Some(key.as_bytes()), None, 0);

    let pair = match pair{
        Ok(file) => file,
        Err(error) => panic!("Problem with get: {:?}", error),
    };
    let (_,v) = pair;
    
    let v = str::from_utf8(v);
    let v = match v{
        Ok(file) => file,
        Err(error) => panic!("Problem with v: {:?}", error),
    };

    println!("{}", v);

}
fn test_timeStamp(mut cursor:RwCursor ){
    
    let mut file = File::open("./dataset/thunder_bird2.json").unwrap();
    //let mut file = File::open("./dataset/simple.json").unwrap();
    let mut buff = String::new();
    file.read_to_string(&mut buff).unwrap();
    let resp: Response = serde_json::from_str(&buff).unwrap();

    for element in resp.dataset {
        let key = element.timestamp;
        let value = element.entry;
        //let res = RwCursor::put( &mut cursor, &key.to_be_bytes(), &value, WriteFlags::NO_DUP_DATA);
        let res = RwCursor::put( &mut cursor, &key.to_be_bytes(), &value, WriteFlags::APPEND_DUP);
        
        match res{
            Ok(file) => file,
            Err(error) => {
                println!("{}", value);
                //panic!("Problem with put: {:?}", error)
            },
        };
    }

    let key:u64 =1131566461;
    let mut itr = Cursor:: iter_dup_of(&mut cursor, &key.to_be_bytes());
    let mut itr = match itr{
        Ok(file) => file,
        Err(error) => panic!("Problem with itr: {:?}", error),
    };
    
    while true {
        let tmp = itr.next();
        match tmp {
            // The division was valid
            Some(x) => {
                let (_,v) = x;
                let v = str::from_utf8(v);
                let v = match v{
                    Ok(file) => file,
                    Err(error) => panic!("Problem with v: {:?}", error),
                };

                println!("{}", v);
            },
            // The division was invalid
            None    => break,
        }
    }


}
fn is_hidden(entry: &DirEntry) -> bool {
    entry.file_name()
         .to_str()
         .map(|s| s.starts_with("."))
         .unwrap_or(false)
}
fn demo<T>(v: Vec<T>) -> [T; 16] where T: Copy {
    let slice = v.as_slice();
    let array: [T; 16] = match slice.try_into() {
        Ok(ba) => ba,
        Err(_) => panic!("Expected a Vec of length {} but it was {}", 16, v.len()),
    };
    array
}
fn read_multi_json_test_timeRange(mut cursor:RwCursor ){
    let mut key_start = 0;
    let mut key_end = 0;
    let time_range = 60;
    let mut s = String::from("");

    let walker = WalkDir::new("./log2json/json_directory").into_iter();
    for entry in walker.filter_entry(|e| !is_hidden(e)) {
        let entry = entry.unwrap();
        if entry.metadata().unwrap().is_file() {
            // println!("{}", entry.path().display());
            let d_name = String::from(entry.path().to_string_lossy());
            // println!("{}", d_name);
            let mut file = File::open(d_name).unwrap();
            let mut buff = String::new();
            file.read_to_string(&mut buff).unwrap();
            let resp: Response = serde_json::from_str(&buff).unwrap();
            for element in resp.dataset {
                if key_start == 0 {
                    key_start = element.timestamp;
                    key_end = key_start + time_range;                  
                    
                }
                else {
                    if element.timestamp >= key_start && element.timestamp <= key_end {
                        s.push_str(&element.entry);
                        s.push_str("\n");
                    }
                    else {
                        println!("key_start={}", key_start);
                        println!("key_end={}", key_end);
                        println!("-------------- ");
                        // let mut tmp_start = key_start.to_be_bytes();
                        // println!(" tmp_start1={:?}", tmp_start);

                        // key_start = key_start<<32;
                        // tmp_start = key_start.to_be_bytes();
                        // println!(" tmp_start2={:?}", tmp_start);

                        // let tmp_end = key_end.to_be_bytes();
                        // println!(" tmp_end1={:?}", tmp_end);

                        // tmp_start[4..8].clone_from_slice(&tmp_end[4..8]);
                        // println!(" tmp_start3={:?}", tmp_start);
                        // println!("-------------- ");
                        
                        // //println!("s={}", s);
                        // let res = RwCursor::put( &mut cursor, &tmp_start, &s, WriteFlags::APPEND);
                        let tmp = Timerange{
                            starttime: key_start,
                            endtime: key_end
                        };
                        //let encoded: Vec<u8> = bincode::serialize(&entity).unwrap();
                        let bytes: Vec<u8> = bincode::serialize(&tmp).unwrap();
                        let bytes = demo(bytes);
                        println!("s={:?}", bytes);
                        let res = RwCursor::put( &mut cursor, &bytes, &s, WriteFlags::NO_OVERWRITE);

        
                        match res{
                            Ok(file) => file,
                            Err(error) => {
                                let pair = Cursor::get(&cursor, Some(&bytes), None, 0);

                                let pair = match pair{
                                    Ok(file) => file,
                                    Err(error) => panic!("Problem with get: {:?}", error),
                                };
                                let (_,v) = pair;
                                
                                let v = str::from_utf8(v);
                                let v = match v{
                                    Ok(file) => file,
                                    Err(error) => panic!("Problem with v: {:?}", error),
                                };

                                println!("{}", v);
                                panic!("Problem with put: {:?}", error)
                            },
                        };
                        // println!("s={}", s);
                        s = String::from("");
                        key_start = element.timestamp;
                        key_end = key_start + time_range;
                    }
                }
            }
        }
    }
    let mut key_start = 1131567255;
    let mut key_end = key_start + time_range;
    println!("key_start={}", key_start);
    println!("key_end={}", key_end);
    // let tmp_start = key_start.to_be_bytes();
    // let tmp_end = key_end.to_be_bytes();

    // let mut tmp_start = key_start.to_be_bytes();
    // println!(" tmp_start1={:?}", tmp_start);

    // key_start = key_start<<32;
    // tmp_start = key_start.to_be_bytes();
    // println!(" tmp_start2={:?}", tmp_start);

    // let tmp_end = key_end.to_be_bytes();
    // println!(" tmp_end1={:?}", tmp_end);

    // tmp_start[4..8].clone_from_slice(&tmp_end[4..8]);
    // println!(" tmp_start3={:?}", tmp_start);
    let tmp = Timerange{
        starttime: key_start,
        endtime: key_end
    };
    let bytes: Vec<u8> = bincode::serialize(&tmp).unwrap();
    let bytes = demo(bytes);
    //let pair = Cursor::get(&cursor, Some(&tmp_start), None, 0);
    let pair = Cursor::get(&cursor, Some(&bytes), None, 0);

    let pair = match pair{
        Ok(file) => file,
        Err(error) => panic!("Problem with get: {:?}", error),
    };
    let (_,v) = pair;
    
    let v = str::from_utf8(v);
    let v = match v{
        Ok(file) => file,
        Err(error) => panic!("Problem with v: {:?}", error),
    };

    println!("{}", v);

}
fn read_one_json(){
    let mut file = File::open("./thunder_bird.json").unwrap();
    let mut buff = String::new();
    file.read_to_string(&mut buff).unwrap();
    let resp: Response = serde_json::from_str(&buff).unwrap();

    let mut key_start = 0;
    let mut key_end = 0;
    let time_range = 60;
    let mut s = String::from("");

    for element in resp.dataset {
        // println!("timestamp={}", element.timestamp); //u64
        // println!("entry={}", element.entry); //String
        if key_start == 0 {
            key_start = element.timestamp;
            key_end = key_start + time_range;
            println!("key_start={}", key_start);
            println!("key_end={}", key_end);
        }
        else {
            if element.timestamp >= key_start && element.timestamp <= key_end {
                s.push_str(&element.entry);
                s.push_str("\n");
            }
            else {
                println!("s={}", s);
                s = String::from("");
                key_start = element.timestamp;
                key_end = key_start + time_range;
                println!("key_start={}", key_start);
                println!("key_end={}", key_end);
            }
        }
    }

    println!("s={}", s);

}