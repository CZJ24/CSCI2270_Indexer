use std::path::Path;
use std::str;
use lmdb::{EnvironmentBuilder,Environment, DatabaseFlags,RwTransaction,WriteFlags, RwCursor,Cursor};
use tempfile::tempdir;

use serde_derive::Deserialize;
extern crate serde;
use std::fs::File;
use std::io::Read;

#[derive(Deserialize)]
struct Response {
    dataset: Vec<Element>,
}

#[derive(Deserialize)]
struct Element {
    timestamp: u64,
    entry: String,
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

    let db = Environment::create_db(&env, None, DatabaseFlags::DUP_SORT);

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

    // let key = "1";
    // let value = "cat";
    // let res = RwCursor::put( &mut cursor, &key, &value, WriteFlags::APPEND);

    // match res{
    //     Ok(file) => file,
    //     Err(error) => panic!("Problem with put: {:?}", error),
    // };

    // let pair = Cursor::get(&cursor, Some(key.as_bytes()), None, 0);

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

    test2(cursor);

}

fn test2(mut cursor:RwCursor ){
    
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

fn test1(){
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