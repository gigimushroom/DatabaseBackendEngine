#include <list>

#include "hash/extendible_hash.h"
#include "page/page.h"
#include <bitset>
#include <assert.h>
#include "common/logger.h"

namespace cmudb {

// we use least significant bits in this impl
// to get the right-most N bits
// int n = original_value & ((1 << N) - 1);

/*
 * constructor
 * array_size: fixed array size for each bucket
 */
template <typename K, typename V>
ExtendibleHash<K, V>::ExtendibleHash(size_t size) 
: mDepth(1), mTotalBucketSize(2) {
  
  std::shared_ptr<Bucket> b1(new Bucket(size, mDepth, 0));
  std::shared_ptr<Bucket> b2(new Bucket(size, mDepth, 1));
  mDirectory.push_back(b1);
  mDirectory.push_back(b2);
}

/*
 * helper function to calculate the hashing address of input key
 */
template <typename K, typename V>
size_t ExtendibleHash<K, V>::HashKey(const K &key) {
  std::hash<K> key_hash;
  return key_hash(key);
}

template <typename K, typename V>
size_t ExtendibleHash<K, V>::GetBucketIndexFromHash(size_t hash) {
  int n = hash & ((1 << mDepth) - 1);
  return n;
}

/*
 * helper function to return global depth of hash table
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetGlobalDepth() const {
  // std::lock_guard<std::mutex> lock(mutex_);
  return mDepth;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
  // TODO: err handling
  if (size_t(bucket_id) >= mDirectory.size()) {
    return -1;
  }

  return mDirectory[bucket_id]->mLocalDepth;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
  return mDirectory.size();
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
  return false;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
  return false;
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
  // get hash of key, find bucket index
  size_t index = GetBucketIndexFromHash(HashKey(key));

  // lookup bucket
  assert(mDirectory.size() - 1 >= index);
  auto bucket = mDirectory[index];
  
  // try to insert
  if (!bucket->isFull()) {
    bucket->list.push_back(std::make_pair(key,value));
    LOG_INFO("insert to map. Bucket index:%lu, Position: %lu", index, bucket->list.size()-1);
    //std::cout<<"key:" << key << " value:" << value << " bucket index:" << index;
  } else {
    LOG_INFO("too bad, bucket index:%lu is full", index);
  }

  // if full, split

}


template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
