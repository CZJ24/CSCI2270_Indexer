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

use std::error::Error;
use grep_regex::RegexMatcher;
use grep_searcher::Searcher;
use grep_searcher::sinks::UTF8;
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
    let time_range = 100;
    let mut s = String::new();
    //let mut file_path = "./log2json/json_directory";
    // let mut file_path = "C:/Users/14767/master-term2/csci2270/project/log2json/json_directory";
    
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
                s.push_str(&element.entry);
                s.push_str("\n");
            }
            else {
                while element.timestamp > key_end {
                    let res = RwTransaction::put( &mut trans,db, &key_start.to_be_bytes(), &s, WriteFlags::NO_OVERWRITE);
        
                    match res{
                        Ok(file) => file,
                        Err(error) => {
                            println!("Problem with put: {:?}, key_start={}", error, key_start);
                                //panic!("Problem with put: {:?}", error)
                        },
                    };
                    //println!("start = {}", key_start);
                    //println!("end = {}", key_end);
                    //println!("{}", s);
                    //println!("-----------------------------------------");
                    s = String::new();
                    key_start = key_end+1;
                    key_end = key_start + time_range-1;
                }     
                s.push_str(&element.entry);
                s.push_str("\n");          
            }
        }
    }
    
    let res = trans.commit();
    match res{
        Ok(file) => file,
        Err(error) => panic!("Problem trans commit: {:?}", error),
    };

}
fn grep_function(match_text: &[u8],input_text: &str) -> Result<(), Box<dyn Error>> {
    let mut matches: Vec<String> = vec![];
    let raw_string = format!(r"{}", input_text);
    let matcher = RegexMatcher::new_line_matcher(&raw_string)?;

    Searcher::new().search_slice(&matcher, match_text, UTF8(|_lnum, line| {
        // We are guaranteed to find a match, so the unwrap is OK.
        matches.push(line.to_string());
        Ok(true)
    }))?;

    // for match_element in matches.iter() {
    //     println!("{}", match_element);
    // }
    Ok(())
}
pub fn search_with_string(mut cursor:RoCursor ){    

    let start = Instant::now();
    let time_range = 100;
    
    //let mut key:u64 = 1132524601;
    let mut key:u64 = 1134528001;
    //let mut key:u64 = 1135532601;
    
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
    //println!("{}", u64::from_be_bytes(v.try_into().unwrap()));
    let v = str::from_utf8(v);
    let v = match v{
        Ok(file) => file,
        Err(error) => panic!("Problem with v: {:?}", error),
    };
    let duration = start.elapsed();
    println!("Time elapsed after get is: {:?}", duration);

    if let Err(_e) = grep_function(v.as_bytes(), &key.to_string()) { /* */ }
    let duration = start.elapsed();
    println!("Time elapsed after get is: {:?}", duration);
    // println!("{}", v);

}