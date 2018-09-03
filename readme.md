a new disk-oriented storage manager for the SQLite DBMS. Such a storage manager assumes that the primary storage location of the database is on disk.

using SQLite's Virtual Table interface

Task 1:
The buffer pool is responsible for moving physical pages back and forth from main memory to disk. It allows a DBMS to support databases that are larger than the amount of memory that is available to the system. Its operations are transparent to other parts in the system. For example, the system asks the buffer pool for a page using its unique identifier (page_id) and it does not know whether that page is already in memory or whether the system has to go retrieve it from disk.





To enable logging and debug symbol in your project, you will need to reconfigure it like this:

$ mkdir build
$ cd build
$ cmake -DCMAKE_BUILD_TYPE=DEBUG ..
$ make