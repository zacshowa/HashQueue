use std::hash::{Hash};
use std::collections::HashSet;
use std::fmt::Debug;
use std::ops::Deref;
use std::path::Path;

use bincode;
use serde::{Deserialize, Serialize};
use sled::{self, Error, IVec, Tree};

use crate::errors::HashQueueError;

pub struct HashQueue<T>{
    tree: Tree,
    set: HashSet<T>,
}

impl<T> HashQueue<T>
    where
        T: Hash + Eq + Clone + Serialize + Debug,
        for<'de> T: Deserialize<'de>,
{

    ///Name: open
    ///
    /// Desc: This function opens a new HashQueue from the disk at the given path via sled, and populates the hashset from the database.
    ///
    /// Additional notes: If any of the fallible operations in this function fail, this function will return a `HashQueueError`. Therefore, we know
    ///                    that if it doesn't fail, the data structure has been properly initialized, and consistent with the desired properties of the data structure.
    ///
    /// Usage:
    ///```
    /// use std::path::Path;
    /// use set_deque::hash_queue::HashQueue;
    ///
    /// let mut hash_queue = HashQueue::open(Path::new("./examples/open"), "test").unwrap();
    ///
    /// hash_queue.push_back(1).unwrap();
    ///
    /// let result = hash_queue.front().unwrap();
    ///
    /// assert_eq!(Some(1), result);
    ///

    pub fn open<P: AsRef<Path>, V: AsRef<[u8]>>(path: P, name: V) -> Result<Self, HashQueueError>{
        let db = sled::open(path)?;

        //This looks weird, and may be a bit of a hack, but this way we can filter out any errors that happen in iterating over the db and fail if any occur.
        let collected_iter = db.iter().collect::<Result<Vec<(IVec, IVec)>, Error>>()?;

        let mut set: HashSet<T> = HashSet::new();
        //After all, we need to be sure the data structures are *always* synced, so we should fail fast.
        if collected_iter.is_empty(){
            Ok(Self{
                tree: db.open_tree(name)?,
                set
        })
        }
        else{
            for (_, value) in collected_iter {

                let item = bincode::deserialize(value.as_ref())?; //deserialize the item to store it in the hash set.
                set.insert(item); //inset the value into the set
            }
            Ok(Self{
                tree: db.open_tree(name)?,
                set
            })
        }
    }

    ///Name: is_empty
    ///
    /// Desc: This function uses the cardinality of the hash set to determine if the queue is empty.
    ///
    /// Usage:
    ///```
    /// use std::path::Path;
    /// use set_deque::hash_queue::HashQueue;
    ///
    /// let mut hash_queue = HashQueue::open(Path::new("./examples/is_empty"), "test").unwrap();
    ///
    /// hash_queue.push_back(1).unwrap();
    ///
    /// let result = hash_queue.is_empty();
    ///
    /// assert_eq!(false, result);
    ///
    /// let result = hash_queue.pop_front().unwrap();
    ///
    ///
    /// let result = hash_queue.is_empty();
    ///
    /// assert_eq!(true, result);
    /// ```

    pub fn is_empty(&self) -> bool{
        self.set.is_empty()
    }

    ///This function calculates the index at back of the deque.
    fn back_index(&self) -> i64 {
        if let Ok(Some((key, _val))) = self.tree.last() {
            let k = i64::from_be_bytes(
                key.as_ref()[..8]
                    .try_into()
                    .expect("back_index: couldn't convert key to bytes"),
            );
            println!("back_index: {}", k);
            println!("back_index+1: {:?}", k + 1i64);
            k + 1i64
        } else {
            0i64
        }
    }

    ///Name: front
    ///
    /// Desc: This function returns the front of the queue, if it exists. This is similar to a peek function
    /// as it will not modify the queue in any way.
    ///
    /// Usage:
    ///```
    /// use std::path::Path;
    /// use set_deque::hash_queue::HashQueue;
    ///
    /// let mut hash_queue = HashQueue::open(Path::new("./examples/front"), "test").unwrap();
    ///
    /// hash_queue.push_back(1).unwrap();
    ///
    /// let result = hash_queue.front().unwrap();
    ///
    /// assert_eq!(Some(1), result);
    ///
    /// let result = hash_queue.pop_front().unwrap();
    ///
    /// assert_eq!(Some(1), result);
    ///
    /// let result = hash_queue.is_empty();
    ///
    /// assert_eq!(true, result);
    /// ```
    pub fn front(&self) -> Result<Option<T>, HashQueueError> {
        if let Ok(Some((_key, val))) = self.tree.first() {
            Ok(Some(bincode::deserialize(val.deref())?))
        } else {
            Ok(None)
        }
    }

    ///Name: back
    ///
    /// Desc: This function returns the back of the queue, if it exists. This is similar to a peek function
    /// as it will not modify the queue in any way.
    ///
    /// Additional Notes: Originally, this was intended to be a Deque, I still plan to make this into one one day, but based on the original use case
    ///                  there was no need for it to be a deque, so I left it as a queue. making it into a deque might require a refactor as the current
    ///                  method of indexing the queue is incompatible with a deque.
    ///
    /// Usage:
    ///```
    /// use std::path::Path;
    /// use std::ptr::hash;
    /// use set_deque::hash_queue::HashQueue;
    ///
    /// let mut hash_queue = HashQueue::open(Path::new("./examples/back"), "test").unwrap();
    ///
    /// hash_queue.push_back(1).unwrap();
    /// hash_queue.push_back(2).unwrap();
    /// let result = hash_queue.back().unwrap();
    ///
    /// assert_eq!(Some(2), result);
    ///
    /// ```
    pub fn back(&self) -> Result<Option<T>, HashQueueError> {
        if let Ok(Some((_key, val))) = self.tree.last() {
            Ok(Some(bincode::deserialize(val.deref())?))
        } else {
            Ok(None)
        }
    }

    ///Name: pop_front
    ///
    /// Desc: This function returns the front element of the queue, if it exists. This will modify the queue and remove the element.
    /// If the element doesn't exist, this method will return Ok(None). It will only return a HashQueueError if an error occurs that indicates the data structure is corrupted, or an error that can't be recovered from occurs.
    ///
    /// Usage:
    ///```
    /// use std::path::Path;
    /// use set_deque::hash_queue::HashQueue;
    ///
    /// let mut hash_queue = HashQueue::open(Path::new("./examples/pop_front"), "test").unwrap();
    ///
    /// hash_queue.push_back(1).unwrap();
    ///
    ///
    /// let result = hash_queue.pop_front().unwrap();
    ///
    /// assert_eq!(Some(1), result);
    ///
    /// let result = hash_queue.is_empty();
    ///
    /// assert_eq!(true, result);
    /// ```
    pub fn pop_front(&mut self) -> Result<Option<T>, HashQueueError> {
        if let Ok(Some((key, val))) = self.tree.pop_min() {
            let data = bincode::deserialize(val.deref())?;
            println!("pop_front: {:?}", key);
            println!("pop_front: {:?}", data);
            match self.set.remove(&data){
                true => {
                    self.tree.flush().unwrap();
                    Ok(Some(data))
                },
                false => {
                    Ok(None)
                }
            }
        } else {
            Ok(None)
        }
    }

    ///Name: pop_back
    ///
    /// Desc: This function returns the back element of the queue, if it exists. This will modify the queue and remove the element.
    /// If the element doesn't exist, this method will return Ok(None). It will only return a HashQueueError if an error occurs that indicates the data structure is corrupted, or an error that can't be recovered from occurs.
    ///
    /// Usage:
    ///```
    /// use std::path::Path;
    /// use set_deque::hash_queue::HashQueue;
    ///
    /// let mut hash_queue = HashQueue::open(Path::new("./examples/pop_back"), "test").unwrap();
    ///
    /// hash_queue.push_back(1).unwrap();
    /// hash_queue.push_back(2).unwrap();
    ///
    /// let result = hash_queue.pop_back().unwrap();
    ///
    /// assert_eq!(Some(2), result);
    /// ```
    pub fn pop_back (&mut self) -> Result<Option<T>, HashQueueError> {
        if let Ok(Some((key, val))) = self.tree.pop_max() {
            let data = bincode::deserialize(val.deref())?;
            println!("pop_back: {:?}", key);
            println!("pop_back: {:?}", data);
            match self.set.remove(&data){
                true => {
                    self.tree.flush().unwrap();
                    Ok(Some(data))
                },
                false => {
                    Err(HashQueueError::SyncError {
                        message: "pop_back".to_string(),
                    })
                }
            }
        } else {
            Ok(None)
        }
    }

    //This is an internal function that is used to insert an item to the sled db at a given index.
    fn insert_at(&mut self, value: T, n: i64) -> Result<bool, HashQueueError>{
        println!("insert_at: {}", n);
        if self.set.insert(value.clone()){
            self.tree
                .insert(i64::to_be_bytes(n), bincode::serialize(&value)?)
                .expect("insert_at: failure to insert");
            Ok(true)
        }
        else{
            Ok(false)
        }
    }

    ///Name: push_back
    ///
    /// Desc: This function pushes an element to the back of the queue. This will modify the queue.
    /// If the element isn't already present in the queue, this method will return ```Ok(true)```, and modify the queue to include the element. If the element is already present, it will return ```Ok(false)```
    /// It will only return a HashQueueError if an error occurs that indicates the data structure is corrupted, or an error that can't be recovered from occurs.
    ///
    /// Usage:
    ///```
    /// use std::path::Path;
    /// use set_deque::hash_queue::HashQueue;
    ///
    /// let mut hash_queue = HashQueue::open(Path::new("./examples/push_back"), "test").unwrap();
    ///
    /// hash_queue.push_back(1).unwrap();
    ///
    ///
    /// let result = hash_queue.pop_front().unwrap();
    ///
    /// assert_eq!(Some(1), result);
    ///
    /// let result = hash_queue.is_empty();
    ///
    /// assert_eq!(true, result);
    /// ```
    pub fn push_back(&mut self, value: T) -> Result<bool, HashQueueError>{
        let last = self.back_index();
        let return_value = self.insert_at(value, last );
        self.tree.flush().expect("push_back: failure to flush tree");
        return_value
    }

    ///Name: clear
    ///
    /// Desc: This function removes all of the data from the data structure. This includes the file backed db.
    /// Only use it if you intend to remove the data.
    ///
    /// Usage:
    ///```
    /// use std::path::Path;
    /// use set_deque::hash_queue::HashQueue;
    ///
    /// let mut hash_queue = HashQueue::open(Path::new("./examples/clear"), "test").unwrap();
    ///
    /// hash_queue.push_back(1).unwrap();
    ///
    /// hash_queue.clear();
    ///
    /// let result = hash_queue.is_empty();
    ///
    /// assert_eq!(true, result);
    /// ```
    pub fn clear(&mut self) {
        self.tree.clear().expect("clear: failure to clear tree");
        self.set.clear();
    }

}

#[cfg(test)]
mod tests{
    use std::fmt::Debug;
    use std::hash::Hash;
    use std::path::Path;
    use serde::{Deserialize, Serialize};
    use crate::hash_queue::HashQueue;



    /// This function is a basic start up that is used to initialize the set-deque and
    fn test_setup<T:  Hash + Eq + Clone + Serialize + Debug + for<'de> Deserialize<'de>>(_: T, db_name: &str ) -> HashQueue<T>{
        let mut set_deque: HashQueue<T> = HashQueue::open(Path::new(db_name), "test").unwrap();
        set_deque.clear();
        set_deque
    }

    #[test]
    fn should_add_to_hash_queue(){
        let mut hash_queue = test_setup("1".to_string(), "./tests/should_add_to_hash_queue");
        let result = hash_queue.push_back("1".to_string());
        assert_eq!(true, result.unwrap());
    }


    #[test]
    fn should_report_hash_queue_is_empty(){
        let hash_queue= test_setup("1".to_string(), "./tests/should_report_hash_queue_is_empty");
        let result = hash_queue.is_empty();
        assert_eq!(true, result);
    }


    #[test]
    fn should_report_hash_queue_is_not_empty(){
        let mut hash_queue= test_setup("1".to_string(), "./tests/should_report_hash_queue_is_not_empty");
        hash_queue.push_back("1".to_string()).unwrap();
        let result = hash_queue.is_empty();
        assert_eq!(false, result);
    }

    #[test]
    fn should_see_front_of_hash_queue_and_dequeue(){
        let mut hash_queue= test_setup(1u64, "./tests/should_see_front_of_hash_queue_and_dequeue");
        hash_queue.push_back(1).unwrap();
        let result = hash_queue.front().unwrap();
        assert_eq!(Some(1), result);
        let result = hash_queue.pop_front().unwrap();
        assert_eq!(Some(1), result);
    }


    #[test]
    fn should_fail_to_add_duplicate_item(){
        let mut hash_queue = test_setup(1u64, "./tests/should_fail_to_add_duplicate_item");
        hash_queue.push_back(1).unwrap();
        let result = hash_queue.push_back(1).unwrap();
        assert_eq!(false, result);
    }

    #[test]
    fn should_empty_hash_queue(){
        let mut hash_queue = test_setup(1u64, "./tests/should_empty_hash_queue");
        hash_queue.push_back(1).unwrap();
        let result = hash_queue.front().unwrap();
        assert_eq!(Some(1), result);
        let result = hash_queue.pop_front().unwrap();
        assert_eq!(Some(1), result);
        assert_eq!(true, hash_queue.is_empty());
    }

    #[test]
    fn should_produce_items_in_correct_order(){
        let mut hash_queue= test_setup(1u64, "./tests/should_produce_items_in_correct_order");

        hash_queue.push_back(1).unwrap();
        hash_queue.push_back(2).unwrap();
        hash_queue.push_back(3).unwrap();

        let one = hash_queue.pop_front().unwrap();
        let two = hash_queue.pop_front().unwrap();
        let three = hash_queue.pop_front().unwrap();

        assert_eq!(one, Some(1));
        assert_eq!(two, Some(2));
        assert_eq!(three, Some(3));
    }

    #[test]
    fn should_produce_items_in_correct_order_reversed(){
        let mut hash_queue = test_setup(1u64, "./tests/should_produce_items_in_correct_order_reversed");

        hash_queue.push_back(1).unwrap();
        hash_queue.push_back(2).unwrap();
        hash_queue.push_back(3).unwrap();

        let one = hash_queue.pop_back().unwrap();
        let two = hash_queue.pop_back().unwrap();
        let three = hash_queue.pop_back().unwrap();

        assert_eq!(one, Some(3));
        assert_eq!(two, Some(2));
        assert_eq!(three, Some(1));
    }

}