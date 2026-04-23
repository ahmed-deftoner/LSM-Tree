mod db;
use db::Db;

fn main() {
    let store = Db::new("data.log");

    store.set("name", "ahmed").unwrap();
    store.set("lang", "rust").unwrap();
    store.set("name", "nadeem").unwrap(); 

    println!("{:?}", store.get("name").unwrap());
    println!("{:?}", store.get("lang").unwrap()); 
    println!("{:?}", store.get("missing").unwrap()); 
}
