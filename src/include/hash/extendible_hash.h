/*
 * extendible_hash.h : implementation of in-memory hash table using extendible
 * hashing
 *
 * Functionality: The buffer pool manager must maintain a page table to be able
 * to quickly map a PageId to its corresponding memory location; or alternately
 * report that the PageId does not match any currently-buffered page.
 */

#pragma once

#include <cstdlib>
#include <vector>
#include <string>

// Below fix the error: ‘shared_ptr’ is not a member of ‘std’
#include <memory> 

#include "hash/hash_table.h"

namespace cmudb {

template <typename K, typename V>
class ExtendibleHash : public HashTable<K, V> {
public:
  // constructor
  ExtendibleHash(size_t size);
  // helper function to generate hash addressing
  size_t HashKey(const K &key);
  // helper function to get global & local depth
  int GetGlobalDepth() const;
  int GetLocalDepth(int bucket_id) const;
  int GetNumBuckets() const;
  // lookup and modifier
  bool Find(const K &key, V &value) override;
  bool Remove(const K &key) override;
  void Insert(const K &key, const V &value) override;

  class Bucket {
    public:
    Bucket(size_t size, int depth, int id) 
    : mCapacity(size), mLocalDepth(depth), mId(id)
    {
      // Increase the capacity of the vector to a value that's greater or equal to new_cap. 
      list.reserve(size);
    }

    bool isFull() { return (list.size() >= mCapacity); }

    size_t mCapacity; // fixed array size
    //int mLastStoredPos; // last occupied slot index, start from 0
    int mLocalDepth;
    int mId;
    // std::array must be a compile-time constant. We use vector instead
    std::vector<std::pair<K,V>> list;
  };

private:
  // add your own member variables here
  size_t GetBucketIndexFromHash(size_t hash);

  // total num of bits needed to express the total num of buckets
  int mDepth; // gloabl depth

  int mTotalBucketSize;  // should be 2^mDepth
  int mBucketCapacity;
  std::vector<std::shared_ptr<Bucket>> mDirectory;


};
} // namespace cmudb
