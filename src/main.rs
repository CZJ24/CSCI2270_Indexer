
use std::path::Path;
use std::time::{Duration, Instant};
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
    let map_size:libc::size_t =  42949672960;
    let mut env_builder = Environment::new();
    let env_builder = EnvironmentBuilder::set_map_size(& mut env_builder, map_size);

    //let path = Path::new("../5G/env/env_btree_big");
    //let path = Path::new("../35G/env/env_btree_big");
    let path = Path::new("../5G/env2/env_string_big");
    //let path = Path::new("../35G/env2/env_string_big");
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
    let res = env.stat();
    let res = match res{
        Ok(file) => file,
        Err(error) => panic!("Problem begin sync: {:?}", error),
    };
    println!("{}", res.entries());
    //let start = Instant::now();

    //btree_map_test::search_with_btreeMap(cursor);
    grep_test::search_with_string(cursor);
    
    // let duration = start.elapsed();
    // println!("Time elapsed in search function() is: {:?}", duration);
    
}
fn create_db(){
    let map_size:libc::size_t =  42949672960;
    let mut env_builder = Environment::new();
    let env_builder = EnvironmentBuilder::set_map_size(& mut env_builder, map_size);

    //let path = Path::new("../5G/env/env_btree_big");
    //let path = Path::new("../35G/env/env_btree_big");
    let path = Path::new("../5G/env2/env_string_big");
    //let path = Path::new("../35G/env2/env_string_big");
    let env = EnvironmentBuilder::open(&env_builder, path);
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

    //btree_map_test::store_with_btreeMap(cursor);
    let start = Instant::now();
    
    //btree_map_test::store_with_btreeMap(trans, db);
    grep_test::store_with_string(trans, db);
    let duration = start.elapsed();
    println!("Time elapsed in store_function() is: {:?}", duration);

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


    // let res = trans.commit();
    // match res{
    //     Ok(file) => file,
    //     Err(error) => panic!("Problem begin rwCursor: {:?}", error),
    // };
}

