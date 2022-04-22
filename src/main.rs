use std::collections::btree_map;
use std::path::Path;
use std::str;
//use std::str::pattern::StrSearcher;
use lmdb::{EnvironmentBuilder,Environment, DatabaseFlags,RwTransaction,WriteFlags, RwCursor,Cursor,Transaction};
use tempfile::tempdir;

use serde_derive::Deserialize;
use serde_derive::Serialize;
extern crate serde;
use std::fs::File;
use std::io::Read;
use walkdir::{DirEntry, WalkDir};
use bincode;
use std::collections::BTreeMap;
#[derive(Deserialize)]
struct Response {
    dataset: Vec<Element>,
}

#[derive(Deserialize)]
struct Element {
    timestamp: u64,
    entry: String,
}

// #[derive(Serialize)]
// struct Timerange {
//     starttime: u64,
//     endtime: u64,
// }
fn main() {
    //let path = Path::new("");
    //let str = "test";
    let tempdir = tempdir().unwrap();
    
    let mut env_builder = Environment::new();
    let env_builder = EnvironmentBuilder::set_map_size(& mut env_builder, 0xFFFFFFFF);
    //let env = EnvironmentBuilder::open(&env_builder, path);
    let env = EnvironmentBuilder::open(&env_builder, tempdir.path());
    let env = match env{
        Ok(file) => file,
        Err(error) => panic!("Problem opening the file: {:?}", error),
    };

    //for timestamp test
    //let db = Environment::create_db(&env, None, DatabaseFlags::DUP_SORT);
    let db = Environment::create_db(&env, None, DatabaseFlags:: INTEGER_KEY);

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

    search_with_btreeMap(cursor);
    //basic_util_test(cursor);
    //test_timeStamp(cursor);
    //read_multi_json_test_timeRange(cursor);
    //test_bug(cursor);
    //test_btreeMap(cursor);

}

fn search_with_btreeMap(mut cursor:RwCursor ){
    let mut key_start = 0;
    let mut key_end = 0;
    let mut key_tmp:u64 = 0;
    let time_range = 60;
    let mut btree:BTreeMap<u64, Vec<String>> = BTreeMap::new();

    let mut v:Vec<String> = Vec::new();

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
                    key_tmp = element.timestamp;  
                    
                    
                    v.push(element.entry);
                    
                }
                else {
                    if element.timestamp >= key_start && element.timestamp <= key_end {
                        if element.timestamp==key_tmp{
                            v.push(element.entry);
                        }
                        else{
                            btree.insert(key_tmp, v);
                            v= Vec::new();
                            v.push(element.entry);
                            key_tmp = element.timestamp;
                        }
                    }
                    else {
                        println!("key_start={}", key_start);
                        println!("key_end={}", key_end);

                        println!("-------------- ");
                        
                        let s_res = bincode::serialize(&btree);
                        let s_res = match s_res{
                            Ok(file) => file,
                            Err(error) => panic!("Problem with get: {:?}", error),
                        };
                        let res = RwCursor::put( &mut cursor, &key_start.to_be_bytes(), &s_res, WriteFlags::NO_OVERWRITE);

        
                        match res{
                            Ok(file) => file,
                            Err(error) => {
                                
                                panic!("Problem with put: {:?}", error)
                            },
                        };
                        btree = BTreeMap::new();

                        v = Vec::new();
                        key_start = key_end+1;
                        key_end = key_start + time_range;
                        key_tmp = element.timestamp;
                        v.push(element.entry);
                    }
                }
            }
        }
    }
    let mut key:u64 = 1131566461;
    let mut key_end = key + time_range;

    println!("get!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
    println!("key_start={}", key);
    println!("key_end={}", key_end);
    

    let pair = Cursor::get(&cursor, Some(&key.to_be_bytes()), None, 16);

    let pair = match pair{
        Ok(file) => file,
        Err(error) => panic!("Problem with get: {:?}", error),
    };
    let (_,v) = pair;
    let v:Result<BTreeMap<u64, Vec<String>>, Box<bincode::ErrorKind>> = bincode::deserialize(v);
    let v = match v{
        Ok(file) => file,
        Err(error) => panic!("Problem with v: {:?}", error),
    };
    match v.get(&key) {
        Some(review) => {
            for (pos, e) in review.iter().enumerate() {
                println!("{}: {:?}", pos, e);
            }
        },
        None => println!("{} is unmatched.", key)
    }
 

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
fn test_btreeMap(mut cursor:RwCursor ){


    let mut movie_reviews = BTreeMap::new();

    let key1:u64 = 123;
    let mut v1:Vec<String> = Vec::new();
    v1.push("abc".to_string());
    v1.push("def".to_string());

    let key2:u64 = 456;
    let mut v2:Vec<String> = Vec::new();
    v2.push("qaz".to_string());
    v2.push("wsx".to_string());
    // review some movies.
    movie_reviews.insert(key1,v1);
    movie_reviews.insert(key2,v2);


    let to_find = [key1, key2];
    for movie in &to_find {
        match movie_reviews.get(movie) {
        Some(review) => {
            for (pos, e) in review.iter().enumerate() {
                println!("{}: {:?}", pos, e);
            }
        },
        None => println!("{} is unreviewed.", movie)
        }
    }
    let s_res = bincode::serialize(&movie_reviews);
    let s_res = match s_res{
        Ok(file) => file,
        Err(error) => panic!("Problem with get: {:?}", error),
    };

    let key:u64 = 2;
    let res = RwCursor::put( &mut cursor, &key.to_be_bytes(), &s_res, WriteFlags::APPEND);

    match res{
        Ok(file) => file,
        Err(error) => panic!("Problem with put: {:?}", error),
    };

    let pair = Cursor::get(&cursor, Some(&key.to_be_bytes()), None, 0);

    let pair = match pair{
        Ok(file) => file,
        Err(error) => panic!("Problem with get: {:?}", error),
    };
    let (_,v) = pair;
    
    let v:Result<BTreeMap<u64, Vec<String>>, Box<bincode::ErrorKind>> = bincode::deserialize(v);
    let v = match v{
        Ok(file) => file,
        Err(error) => panic!("Problem with v: {:?}", error),
    };

    println!("-------------");
    for movie in &to_find {
        match v.get(movie) {
        Some(review) => {
            for (pos, e) in review.iter().enumerate() {
                println!("{}: {:?}", pos, e);
            }
        },
        None => println!("{} is unreviewed.", movie)
        }
    }
}
fn test_bug(mut cursor:RwCursor){
    for x in 1..100 {
        let x :u64 = x;
        let res = RwCursor::put( &mut cursor, &x.to_be_bytes(), &x.to_be_bytes(), WriteFlags::NO_OVERWRITE);
        match res{
            Ok(file) => file,
            Err(error) => panic!("Problem with put: {:?}", error),
        };
    }

    let key:u64 = 50;
    let pair = Cursor::get(&cursor, Some(&key.to_be_bytes()), None, 16);
    //let pair = Cursor::get(&cursor, None, Some(&key.to_be_bytes()), 15);

    let pair = match pair{
        Ok(file) => file,
        Err(error) => panic!("Problem with get: {:?}", error),
    };
    let (_,v) = pair;
    println!("{}", u64::from_be_bytes(v.try_into().unwrap()));
    // let v = str::from_utf8(v);
    // let v = match v{
    //     Ok(file) => file,
    //     Err(error) => panic!("Problem with v: {:?}", error),
    // };

    // println!("{}", v);
    // let mut itr = Cursor:: iter_start(&mut cursor);
    // while true {
    //     let tmp = itr.next();
    //     match tmp {
    //         // The division was valid
    //         Some(x) => {
    //             let (_,a) = x;
    //             println!("{}", u64::from_be_bytes(a.try_into().unwrap()));
                
                
    //         },
    //         // The division was invalid
    //         None    => break,
    //     }
    // }
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
                //println!("{}", value);
                //panic!("Problem with put: {:?}", error)
            },
        };
    }

    let key:u64 =1131566520;
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

                        let tmp_start = key_start.to_string();
                        let tmp_end = key_end.to_string();
                        println!("tmp_start={}", tmp_start);
                        println!("tmp_end={}", tmp_end);
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
                        
                        //println!("s={}", s);
                        // let res = RwCursor::put( &mut cursor, &tmp_start, &s, WriteFlags::APPEND);
                        
                        let res = RwCursor::put( &mut cursor, &key_start.to_be_bytes(), &s, WriteFlags::NO_OVERWRITE);

        
                        match res{
                            Ok(file) => file,
                            Err(error) => {
                                // let pair = Cursor::get(&cursor, Some(tmp_start.as_bytes()), None, 0);

                                // let pair = match pair{
                                //     Ok(file) => file,
                                //     Err(error) => panic!("Problem with get: {:?}", error),
                                // };
                                // let (_,v) = pair;
                                
                                // let v = str::from_utf8(v);
                                // let v = match v{
                                //     Ok(file) => file,
                                //     Err(error) => panic!("Problem with v: {:?}", error),
                                // };

                                // println!("{}", v);
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
    let mut key:u64 = 1131566949;
    let mut key_end = key + time_range;

    println!("get!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
    println!("key_start={}", key);
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
    
    //let pair = Cursor::get(&cursor, Some(&tmp_start), None, 0);
    let tmp_start = key.to_string();
    let tmp_end = key_end.to_string();
    println!("tmp_start={}", tmp_start);
    println!("tmp_end={}", tmp_end);
    let pair = Cursor::get(&cursor, Some(&key.to_be_bytes()), None, 16);

    let pair = match pair{
        Ok(file) => file,
        Err(error) => panic!("Problem with get: {:?}", error),
    };
    let (_,v) = pair;
    //println!("{}", u64::from_be_bytes(v.try_into().unwrap()));
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