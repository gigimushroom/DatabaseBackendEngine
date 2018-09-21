a new disk-oriented storage manager for the SQLite DBMS. Such a storage manager assumes that the primary storage location of the database is on disk.

using SQLite's Virtual Table interface

Task 1:
The buffer pool is responsible for moving physical pages back and forth from main memory to disk. It allows a DBMS to support databases that are larger than the amount of memory that is available to the system. Its operations are transparent to other parts in the system. For example, the system asks the buffer pool for a page using its unique identifier (page_id) and it does not know whether that page is already in memory or whether the system has to go retrieve it from disk.

Task 2:
Implement an B+ Tree index in your database system. The index is responsible for fast data retrieval without having to search through every row in a database table, providing the basis for both rapid random lookups and efficient access of ordered records.



To enable logging and debug symbol in your project, you will need to reconfigure it like this:

$ mkdir build
$ cd build
$ cmake -DCMAKE_BUILD_TYPE=DEBUG ..
$ make


Strict 2PL
2 rules:
1. If a transaction T wants to read (or modify) an object, it first requests a shared(or exclusive) lock on the object
2. All locks held by a tx are released when the transaction is completed.

2PL:
1. The same
2. A transaction cannot request additional locks once it releases any lock (tx allow to release lock before the end)

Example usage of condition variable:
https://en.cppreference.com/w/cpp/thread/condition_variable/wait
lock manager uses condition var to wait/awake among threads


Example usage of future/promise:
std::promise<void> go;
std::shared_future<void> ready(go.get_future());

thread 1:
ready.wait(); // wait until value got set

thread 2:
go.set_value();

