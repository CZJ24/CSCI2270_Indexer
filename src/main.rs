
use std::path::Path;

//use std::str::pattern::StrSearcher;
use lmdb::{EnvironmentBuilder,Environment, DatabaseFlags,RwTransaction,RoTransaction,WriteFlags, RwCursor,Cursor,Transaction};
use tempfile::tempdir;


extern crate serde;


use libc;
mod btree_map_test;
mod grep_test;
mod basic_test;



fn main() {

    //create_db();
    open_db();
    
}
fn open_db(){
    let tempdir = tempdir().unwrap();
    let map_size:libc::size_t =  42949672960;
    let mut env_builder = Environment::new();
    let env_builder = EnvironmentBuilder::set_map_size(& mut env_builder, map_size);

    let path = Path::new("./env");
    println!("{}", tempdir.path().display());
    let env = EnvironmentBuilder::open(&env_builder, path);

    let env = match env{
        Ok(file) => file,
        Err(error) => panic!("Problem opening the file: {:?}", error),
    };

    let db = Environment::open_db(&env, None);

    let db = match db{
        Ok(file) => file,
        Err(error) => panic!("Problem open db: {:?}", error),
    };

    let trans = Environment::begin_ro_txn(&env);
    
    let mut trans = match trans{
        Ok(file) => file,
        Err(error) => panic!("Problem begin roTransaction: {:?}", error),
    };

    let cursor = RoTransaction::open_ro_cursor(& mut trans,db);

    let mut cursor = match cursor{
        Ok(file) => file,
        Err(error) => panic!("Problem begin roCursor: {:?}", error),
    };
    btree_map_test::search_with_btreeMap(cursor);

}
fn create_db(){
    let tempdir = tempdir().unwrap();
    let map_size:libc::size_t =  42949672960;
    let mut env_builder = Environment::new();
    let env_builder = EnvironmentBuilder::set_map_size(& mut env_builder, map_size);

    let path = Path::new("./env");
    println!("{}", tempdir.path().display());
    let env = EnvironmentBuilder::open(&env_builder, path);
    //let env = EnvironmentBuilder::open(&env_builder, tempdir.path());
    let env = match env{
        Ok(file) => file,
        Err(error) => panic!("Problem opening the file: {:?}", error),
    };

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

    // let cursor = RwTransaction::open_rw_cursor(& mut trans,db);

    // let mut cursor = match cursor{
    //     Ok(file) => file,
    //     Err(error) => panic!("Problem begin rwCursor: {:?}", error),
    // };

    // btree_map_test::store_with_btreeMap(cursor);
    btree_map_test::store_with_btreeMap(trans, db);

    let res = env.sync(true);
    match res{
        Ok(file) => file,
        Err(error) => panic!("Problem begin sync: {:?}", error),
    };

    

    let res = env.stat();
    let res = match res{
        Ok(file) => file,
        Err(error) => panic!("Problem begin sync: {:?}", error),
    };
    println!("{}", res.entries());

}

