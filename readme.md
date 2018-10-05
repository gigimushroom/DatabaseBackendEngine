# Overview
A new *disk-oriented storage manager* for the SQLite DBMS.
Such a storage manager assumes that the primary storage location of the database is on disk.

Using SQLite's Virtual Table interface, allows you use your storage manager in SQLite without changing application-level code.

## Table of Contents
- [Features](#features)
  - [Buffer Pool Manager](#buffer-pool-manager)
  - [B+ Tree Index](#b-plus-tree-index)
  - [Concurrency Control for Transactions](#concurrency-control-for-transactions)
  - [Logging and Recovery System](#logging-and-recovery-system)
- [How to Build](#how-to-build)
- [Technical Brief](#technical-brief)
  - [The Virtual table](#the-virtual-table)
  - [Buffer Pool Manager](#buffer-pool-manager)
    - [Extendible Hash](#extendible-hash)
    - [LRU Replacer](#lru-replacer)
  - [B+ Tree](#b-plus-tree)
  - [Transaction Manager](#transaction-manager)
  - [Lock Manager](#lock-manager)
  - [Disk Manager](#disk-manager)
  - [Log Manager](#log-manager)
  - [Log recovery](#log-recovery)
- [Resources](#other-tech-used-in-system)


# Features
### Buffer Pool Manager
The buffer pool is responsible for moving physical pages back and forth from main memory to disk. It allows a DBMS to support databases that are larger than the amount of memory that is available to the system. Its operations are transparent to other parts in the system. For example, the system asks the buffer pool for a page using its unique identifier (page_id) and it does not know whether that page is already in memory or whether the system has to go retrieve it from disk.

### B Plus Tree Index
Implement an B+ Tree index in the database system. The index is responsible for fast data retrieval without having to search through every row in a database table, providing the basis for both rapid random lookups and efficient access of ordered records.

### Concurrency Control for Transactions
Implement a concurrent index and lock manager in the database system. The first task is to implement a lock manager which is responsible for keeping track of the tuple-level locks issued to transactions and supporting shared & exclusive lock grant and release. The second task is an extension of task #2 where you will enable your B+tree index to support multi-threaded updates.

### Logging and Recovery System
Implement Logging and Recovery mechanism in your database system. The first task is to implement write ahead logging (WAL) under No-Force/Steal buffering policy and log every single page-level write operation and transaction command.
Implement the ability for the DBMS to recover its state from the log file.

# How to Build
To enable logging and debug symbol in your project, you will need to reconfigure it like this:
```
$ mkdir build
$ cd build
$ cmake -DCMAKE_BUILD_TYPE=DEBUG ..
$ make
```

# Technical Brief
We will introduce the main virtual table, and its table pages, index.

Key components includes buffer manager, lock manager, transaction manager, log manager, log recovery.

## The Virtual table
A [virtual table](https://www.sqlite.org/vtab.html) is an object that is registered with an open SQLite database connection. From the perspective of an SQL statement, the virtual table object looks like any other table or view. But behind the scenes, queries and updates on a virtual table invoke callback methods of the virtual table object instead of reading and writing on the database file.

Our impl of VTable contains A table heap and An index. A virtual table is bascially a DB table, it contains the contents stored in disk, and an index used for search.

### table heap: 
doubly-linked list of table pages in heap
### Index: 
B+ tree index
#### Page
Wrapper around actual data page in main memory and also contains bookkeeping.
Information used by buffer pool manager like pin_count/dirty_flag/page_id.
Use page as a basic unit within the database system
#### Table Page:
```
* table_page.h
*
* Slotted page format:
*  ---------------------------------------
* | HEADER | ... FREE SPACES ... | TUPLES |
*  ---------------------------------------
*                                 ^
*                         free space pointer
*
*  Header format (size in byte):
*  -------------------------------------------------------------------------
* | PageId (4)| LSN (4)| PrevPageId (4)| NextPageId (4)| FreeSpacePointer(4) |
*  --------------------------------------------------------------------------
*  --------------------------------------------------------------
* | TupleCount (4) | Tuple_1 offset (4) | Tuple_1 size (4) | ... |
*
```
Table page stores metadata and tuples in disk.

#### Tuple
```
 * tuple.h
 *
 * Tuple format:
 *  ------------------------------------------------------------------
 * | FIXED-SIZE or VARIED-SIZED OFFSET | PAYLOAD OF VARIED-SIZED FIELD|
 *  ------------------------------------------------------------------
 ```

#### RID
Contains page id, slot num. Used by index Scan. Saved as B+ tree index value.

## Buffer Pool Manager
Functionality: The simplified Buffer Manager interface allows a client to
new/delete pages on disk, to read a disk page into the buffer pool and pin
it, also to unpin a page in the buffer pool.
```
Page *FetchPage(page_id_t page_id);
bool UnpinPage(page_id_t page_id, bool is_dirty);
bool FlushPage(page_id_t page_id);
Page *NewPage(page_id_t &page_id);
bool DeletePage(page_id_t page_id);
```  

### Extendible Hash
extendible_hash.h : implementation of in-memory hash table using extendible
hashing

Functionality: The buffer pool manager must maintain a page table to be able
to quickly map a PageId to its corresponding memory location; or alternately
report that the PageId does not match any currently-buffered page.


### LRU Replacer
Functionality: The buffer pool manager must maintain a LRU list to collect
all the pages that are unpinned and ready to be swapped. The simplest way to
implement LRU is a FIFO queue, but remember to dequeue or enqueue pages when
a page changes from unpinned to pinned, or vice-versa.


## B Plus Tree
It is a balanced tree in which the internal pages direct the search and leaf pages contains actual data entries. The tree structure grows and shrink dynamically, supports split and merge.

Has internal page, leaf page, and index iterator.

## Transaction Manager
1. Begin:starts a new txn.
2. Commit:Commits a txn. Release all locks.
3. Abort: Abort a txn. Undo all operations, release all locks.
Uses Lock Manager, and Log Manager.

### Transaction
```
 * Transaction states:
 *
 *     _________________________
 *    |                         v
 * GROWING -> SHRINKING -> COMMITTED   ABORTED
 *    |__________|________________________^
 *
```
Has shared lock set, and exclusive lock set for table pages.

## Lock Manager
Tuple level lock manager, use wait-die to prevent deadlocks

## Disk Manager
Disk manager takes care of the allocation and deallocation of pages within a
database. It also performs read and write of pages to and from disk, and
provides a logical file layer within the context of a database management
system.

## Log Manager
Maintain a separate thread that is awaken when the log buffer is
full or time out(every X second) to write log buffer's content into disk log
file. 

To achieve the goal of atomicity and durability, the database system must output to stable storage information describing the modifications made by any transaction, this information can help us ensure that all modifications performed by committed transactions are reflected in the database (perhaps during the course of recovery actions after a crash). It can also help us ensure that no modifications made by an aborted or crashed transaction persist in the database. The most widely used structure for recording database modifications is the log. The log is a sequence of log records, recording all the update activities in the database. 

### Log Record
```
 * log_record.h
 * For every write opeartion on table page, you should write ahead a
 * corresponding log record.
 * For EACH log record, HEADER is like (5 fields in common, 20 bytes in totoal)
 *-------------------------------------------------------------
 * | size | LSN | transID | prevLSN | LogType |
 *-------------------------------------------------------------
 * For insert type log record
 *-------------------------------------------------------------
 * | HEADER | tuple_rid | tuple_size | tuple_data(char[] array) |
 *-------------------------------------------------------------
 * For delete type(including markdelete, rollbackdelete, applydelete)
 *-------------------------------------------------------------
 * | HEADER | tuple_rid | tuple_size | tuple_data(char[] array) |
 *-------------------------------------------------------------
 * For update type log record
 *------------------------------------------------------------------------------
 * | HEADER | tuple_rid | tuple_size | old_tuple_data | tuple_size |
 * | new_tuple_data |
 *------------------------------------------------------------------------------
 * For new page type log record
 *-------------------------------------------------------------
 * | HEADER | prev_page_id |
 *-------------------------------------------------------------
```
LogRecordType
```
  INVALID = 0,
  INSERT, //1
  MARKDELETE,
  APPLYDELETE, //3
  ROLLBACKDELETE,
  UPDATE,
  BEGIN,   //6
  COMMIT, //7
  ABORT,  // 8
  // when create a new page in heap table
  NEWPAGE,  // 9
``` 
Log record is the basic unit got written in log file. Used for system recovery.

## Log recovery
Ability for the DBMS to recover its state from the log file
Supports Redo, undo.
APIs:
```
Redo()
Undo()
DeserializeLogRecord()

// maintain active transactions and its corresponds latest lsn
std::unordered_map<txn_id_t, lsn_t> active_txn_;
// mapping log sequence number to log file offset, for undo purpose
std::unordered_map<lsn_t, int> lsn_mapping_;
```
Internally keeps a log buffer, read from log file until EOF.
#### In Redo phase:
1. Deserialize every log record, save txn's latest log No. in active txn map, and lsn_mapping
2. If txn committed/aborted, remove it from map.
3. After redo, active_txn_ map contains all non committed/aborted txns. Used for undo stages.
#### In Undo Phase:
1. For each txn in active txn map, undo the operation
2. Find the operation's previous operation, undo it. (Found prev use previous LSN from log record.)
3. Loop all txns.



## Other Tech used in System
#### What is Two Phase Lock
Strict 2PL
2 rules:
1. If a transaction T wants to read (or modify) an object, it first requests a shared(or exclusive) lock on the object
2. All locks held by a tx are released when the transaction is completed.

2PL:
1. The same
2. A transaction cannot request additional locks once it releases any lock (tx allow to release lock before the end)

#### How to use Concurrency in C++11
[Example usage of condition variable](https://en.cppreference.com/w/cpp/thread/condition_variable/wait)

Our lock manager uses condition var to wait/awake among threads


Example usage of future/promise:
```
std::promise<void> go;
std::shared_future<void> ready(go.get_future());

thread 1:
ready.wait(); // wait until value got set

thread 2:
go.set_value();
```

[The Wait-Die algorithm](http://www.cs.colostate.edu/~cs551/CourseNotes/Deadlock/WaitWoundDie.html)
```
Allow wait only if waiting process is older. 
The Wait-Die algorithm kills the younger process. 
    When the younger process restarts 
        and requests the resource again, 
    it may be killed once more.
```
The Wound-Wait algorithm
```
Allow wait only if waiting process is younger. 
The Wound-Wait algorithm preempts the younger process. 
    When the younger process re-requests resource, 
        it has to wait for older process to finish. 
This is the better of the two algorithms.   
``` 

[How does a relational database work ?](http://coding-geek.com/how-databases-work/)