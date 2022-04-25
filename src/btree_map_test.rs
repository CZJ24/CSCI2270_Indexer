use std::collections::btree_map;
use std::path::Path;
use std::str;
//use std::str::pattern::StrSearcher;
use lmdb::{EnvironmentBuilder,Environment, DatabaseFlags,RwTransaction,WriteFlags, RwCursor,RoCursor,Cursor,Transaction,Database};
use tempfile::tempdir;

use serde_derive::Deserialize;
use serde_derive::Serialize;
extern crate serde;
use std::fs::File;
use std::io::Read;
use walkdir::{DirEntry, WalkDir};
use bincode;
use std::collections::BTreeMap;
use libc;
use std::time::{Duration, Instant};
#[derive(Deserialize)]
struct Response {
    dataset: Vec<Element>,
}

#[derive(Deserialize)]
struct Element {
    timestamp: u64,
    entry: String,
}

fn is_hidden(entry: &DirEntry) -> bool {
    entry.file_name()
         .to_str()
         .map(|s| s.starts_with("."))
         .unwrap_or(false)
}

//pub fn store_with_btreeMap(mut cursor:RwCursor ){
pub fn store_with_btreeMap(mut trans:RwTransaction, mut db:Database ){   

    let mut key_start = 0;
    let mut key_end = 0;
    let mut key_tmp:u64 = 0;
    let time_range = 60;
    let mut btree:BTreeMap<u64, Vec<String>> = BTreeMap::new();

    let mut v:Vec<String> = Vec::new();
    //let mut file_path = "./log2json/json_directory";
    //let mut file_path = "C:/Users/14767/master-term2/csci2270/project/log2json/json_directory";
    // let mut file_path = "C:/Users/14767/master-term2/csci2270/project/log2json/small_dir";
    for i in 0..551{
        let d_name = format!("C:/Users/14767/master-term2/csci2270/project/log2json/small_dir/file_{}.json", i );
        //let d_name = format!("./log2json/json_directory/file_{}.json", i );
        let mut file = File::open(d_name).unwrap();
        let mut buff = String::new();
        file.read_to_string(&mut buff).unwrap();
        let resp: Response = serde_json::from_str(&buff).unwrap();
        for element in resp.dataset {
            if key_start == 0 {
                key_start = element.timestamp;
                key_end = key_start + time_range-1;                  
                v.push(element.entry);
            }
            else {
                while element.timestamp > key_end {
                    if v.len()!=0{
                        btree.insert(key_tmp, v);
                    }                  
                    let s_res = bincode::serialize(&btree);
                    let s_res = match s_res{
                        Ok(file) => file,
                        Err(error) => panic!("Problem with serialize: {:?}", error),
                    };
                    let res = RwTransaction::put( &mut trans,db, &key_start.to_be_bytes(), &s_res, WriteFlags::NO_OVERWRITE);
        
                    match res{
                        Ok(file) => file,
                        Err(error) => {
                            println!("Problem with put: {:?}, key_start={}", error, key_start);
                                //panic!("Problem with put: {:?}", error)
                        },
                    };
                    btree = BTreeMap::new();
                    v= Vec::new();
                    key_start = key_end+1;
                    key_end = key_start + time_range-1;
                    key_tmp = element.timestamp;
                }     
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
        }
    }

    let res = trans.commit();
    match res{
        Ok(file) => file,
        Err(error) => panic!("Problem trans commit: {:?}", error),
    };
    
}

pub fn search_with_btreeMap(mut cursor:RoCursor ){
    let start = Instant::now();

    //let mut key:u64 = 1132524601;
    let mut key:u64 = 1134528001;
    //let mut key:u64 = 1135532601;
    println!("get!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
    println!("key_start={}", key);   

    let pair = Cursor::get(&cursor, Some(&key.to_be_bytes()), None, 16);

    let pair = match pair{
        Ok(file) => file,
        Err(error) => panic!("Problem with get: {:?}", error),
    };
    let duration = start.elapsed();
    println!("Time elapsed after get is: {:?}", duration);
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
    let duration = start.elapsed();
    println!("Time elapsed after print the result is: {:?}", duration);
}

pub fn test_btreeMap(mut cursor:RwCursor ){


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