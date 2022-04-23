use std::str;
use lmdb::{EnvironmentBuilder,Environment, DatabaseFlags,RwTransaction,WriteFlags, RwCursor,Cursor,Transaction};

use serde_derive::Deserialize;
use serde_derive::Serialize;
extern crate serde;
use std::fs::File;
use std::io::Read;
use walkdir::{DirEntry, WalkDir};


#[derive(Deserialize)]
struct Response {
    dataset: Vec<Element>,
}

#[derive(Deserialize)]
struct Element {
    timestamp: u64,
    entry: String,
}
pub fn basic_util_test(mut cursor:RwCursor ){
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

pub fn test_bug(mut cursor:RwCursor){
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
}  
pub fn test_timeStamp(mut cursor:RwCursor ){
    
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



pub fn read_one_json(){
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