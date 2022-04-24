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
// pub fn store_with_string(mut cursor:RwCursor ){
pub fn store_with_string(mut trans:RwTransaction, mut db:Database ){    
    let mut key_start = 0;
    let mut key_end = 0;
    let time_range = 60;
    let mut s = String::new();
    let mut count = 0;
    //let mut file_path = "./log2json/json_directory";
    // let mut file_path = "C:/Users/14767/master-term2/csci2270/project/log2json/json_directory";
    let mut file_path = "C:/Users/14767/master-term2/csci2270/project/log2json/small_dir";
    

    let walker = WalkDir::new(file_path).into_iter();
    
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
                    s.push_str(&element.entry);
                    s.push_str("\n");
                }
                else {
                    if element.timestamp >= key_start && element.timestamp <= key_end {
                        // if key_start == 1134651576&&count<100{
                            
                        //     println!("{}", element.timestamp);
                        //     println!("{}", element.entry);
                        //     count+=1;
                        // }
                        s.push_str(&element.entry);
                        s.push_str("\n");
                    }
                    else {
                        //println!("key_start={}", key_start);
                        // println!("key_end={}", key_end);

                        // println!("-------------- ");
                        //println!("{}", s);
                        
                        // if key_start == 1134651576{
                        //     println!("key_start={}", key_start);

                        //     println!("-------------- ");
                        //     println!("{}", s);
                        // }

                        // if key_start == 1134651576&&count<100 {
                        //     println!("second loop!!!!!!!!!!");
                        //     println!("{}", element.timestamp);
                        //     println!("{}", element.entry);
                        //     count+=1;
                        // }
                        //let res = RwCursor::put( &mut cursor, &key_start.to_be_bytes(), &s, WriteFlags::NO_OVERWRITE);
                        let res = RwTransaction::put( &mut trans,db, &key_start.to_be_bytes(), &s, WriteFlags::NO_OVERWRITE);
        
                        match res{
                            Ok(file) => file,
                            Err(error) => {

                                //println!("key_start={}", key_start);
                                // println!("{}", v);
                                //println!("s={}", s);
                                println!("Problem with put: {:?}, key_start={}", error, key_start);
                                //panic!("Problem with put: {:?}", error)
                            },
                        };
                        // drop(s);
                        // println!("s={}", s);
                        s = String::new();
                        key_start = element.timestamp;
                        key_end = key_start + time_range;
                        s.push_str(&element.entry);
                        s.push_str("\n");
                    }
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

pub fn search_with_string(mut cursor:RoCursor ){    

    let start = Instant::now();
    let time_range = 60;
    //let mut key:u64 = 1131566461;
    //let mut key:u64 = 1131584501;
    let mut key:u64 = 1132133562;
    //let mut key:u64 = 1131523501;
    
    let mut key_end = key + time_range;

    println!("get!!!!!!!!!!!!!!!!!!!!!!!!!!!!!");
    println!("key_start={}", key);
    println!("key_end={}", key_end);
    
    let pair = Cursor::get(&cursor, Some(&key.to_be_bytes()), None, 16);

    let pair = match pair{
        Ok(file) => file,
        Err(error) => panic!("Problem with get: {:?}", error),
    };
    let duration = start.elapsed();
    println!("Time elapsed after get is: {:?}", duration);

    let (_,v) = pair;
    //println!("{}", u64::from_be_bytes(v.try_into().unwrap()));
    let v = str::from_utf8(v);
    let v = match v{
        Ok(file) => file,
        Err(error) => panic!("Problem with v: {:?}", error),
    };

    println!("{}", v);

    let duration = start.elapsed();
    println!("Time elapsed after print the result is: {:?}", duration);
}