# hash-queue
A data structure inspired by problems faced while building a network monitoring utility. 

## Motivation

This project is an exercise in optimization, creating intelligent data structures, and applying the knowledge of past pain points
to create a more robust and performant solution to problems I have previously solved.

The initial version of this project is based upon what my initial solution would have been at the time this ADT was originally being worked on.
Future versions will be me attempting to implement better solutions to the problem. My current ideas include custom file writing and reading, as well as using a ring buffer, rather than an in memory db, for the queue.

### Contributions:

if you would like to contribute to this repo please create a fork and submit a pull request from your fork.


# Usage
```rust
use hash_queue::HashQueue;

fn main(){
    let mut hash_queue = HashQueue::open(Path::new("./path/to/db"), "db name").unwrap();
    
    hash_queue.push_back(1u64);
    hash_queue.push_back(2u64);
    hash_queue.push_back(3u64);
    
    assert_eq!(hash_queue.pop_front(), Some(1u64));
    assert_eq!(hash_queue.pop_front(), Some(2u64));
}