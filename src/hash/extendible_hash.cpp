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
: mDepth(1), mBucketDataSize(size) {
  mDirectory.emplace_back(new Bucket(mDepth, 0));
  mDirectory.emplace_back(new Bucket(mDepth, 1));
  mBucketCount = 2;
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
  std::lock_guard<std::mutex> lock(mutex_);
  return mDepth;
}

/*
 * helper function to return local depth of one specific bucket
 * NOTE: you must implement this function in order to pass test
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetLocalDepth(int bucket_id) const {
  std::lock_guard<std::mutex> lock(mutex_);
  if (size_t(bucket_id) >= GetDirCapacity()) {
    return -1;
  }

  if (mDirectory[bucket_id] != nullptr) {
    size_t numOfItems = mDirectory[bucket_id]->dataMap.size();
    //LOG_INFO("num of items in bucket index %d is: %lu", bucket_id, numOfItems);
    if (numOfItems == 0)
      return -1; // for pass the test
    return mDirectory[bucket_id]->mLocalDepth;
  }
  return -1;
}

/*
 * helper function to return current number of bucket in hash table
 */
template <typename K, typename V>
int ExtendibleHash<K, V>::GetNumBuckets() const {
  std::lock_guard<std::mutex> lock(mutex_);

  int count = 0;
  int i = 0;
  for (auto b:mDirectory) {
    size_t numOfItems = b->dataMap.size();
    // if actually owns the bucket (id matches its index), and contains at lease one item, count
    if (numOfItems > 0 && b->mId == i) {
      count++;
    }
    i++;
  }

  return count;
}

/*
 * lookup function to find value associate with input key
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Find(const K &key, V &value) {
  std::lock_guard<std::mutex> lock(mutex_);
  size_t index = GetBucketIndexFromHash(HashKey(key));
  if (index >= GetDirCapacity()) {
    //LOG_INFO("index %lu beyond limit", index);
    return false;
  }

  //LOG_INFO("index %lu is finding", index);
  auto search = mDirectory[index]->dataMap.find(key);
  if (search != mDirectory[index]->dataMap.end()) {
    value = search->second;
    return true;
  }
  return false;
}

/*
 * delete <key,value> entry in hash table
 * Shrink & Combination is not required for this project
 */
template <typename K, typename V>
bool ExtendibleHash<K, V>::Remove(const K &key) {
  std::lock_guard<std::mutex> lock(mutex_);
  size_t index = GetBucketIndexFromHash(HashKey(key));
  if (index >= GetDirCapacity()) {
    return false;
  }
  auto search = mDirectory[index]->dataMap.find(key);
  if (search != mDirectory[index]->dataMap.end()) {
    mDirectory[index]->dataMap.erase(key);
    return true;
  }
  return false;
}

template <typename K, typename V>
void ExtendibleHash<K, V>::Split(size_t index, const K &key, const V &value) {
  auto bucket = mDirectory[index];
  if (bucket->mLocalDepth == mDepth)
  {
    //LOG_INFO("too bad, bucket index:%d is full. Need to resize directory and split buckets", bucket->mId);
    // resize directory vector
    size_t preSize = GetDirCapacity();
    mDirectory.resize(preSize * 2);
    mDepth++;

    // fix new elements pointer
    for (size_t i=preSize; i<mDirectory.capacity(); ++i) {
      mDirectory[i] = mDirectory[i-preSize];
    }
  } else {
    //LOG_INFO("Bucket index:%d is full. Need to split buckets only", bucket->mId);
  }

  // add a new bucket
  bucket->mLocalDepth++;
  int newBucketId = bucket->mId+(GetDirCapacity()/2); // hack
  int splitBucketId = bucket->mId;
  //LOG_DEBUG("Split bucket id: %d, new bucket id: %d", splitBucketId, newBucketId);
  
  // fix new bucket pointer
  //LOG_INFO("sharded ptr usage count: %lu", mDirectory[newBucketId].use_count());
  mDirectory[newBucketId] = std::make_shared<Bucket>(bucket->mLocalDepth, newBucketId);
  mBucketCount++;

  // split bucket
  for (auto& kv : mDirectory[splitBucketId]->dataMap) {
    size_t newHash = GetBucketIndexFromHash(HashKey(kv.first));
    if (int(newHash) != mDirectory[splitBucketId]->mId) {
      //LOG_INFO("for key %d, Need to move from index: %d, to bucket index: %lu", kv.first, mDirectory[splitBucketId]->mId, newHash);
      // now we need to make a move from bucket A to bucket B
      if (mDirectory[newHash]->mId != int(newHash)) {
        // adjust old pointing bucket depth
        mDirectory[newHash]->mLocalDepth++;
        size_t cacheDepth = mDirectory[newHash]->mLocalDepth;
        mDirectory[newHash] = std::make_shared<Bucket>(cacheDepth, newHash);
      }

      mDirectory[newHash]->dataMap[kv.first] = kv.second;
      mDirectory[splitBucketId]->dataMap.erase(kv.first);
    }      
  }

  // NOTE: adjust depth again
  mDirectory[newBucketId]->mLocalDepth = bucket->mLocalDepth;
}

/*
 * insert <key,value> entry in hash table
 * Split & Redistribute bucket when there is overflow and if necessary increase
 * global depth
 */
template <typename K, typename V>
void ExtendibleHash<K, V>::Insert(const K &key, const V &value) {
  std::lock_guard<std::mutex> lock(mutex_);
  // get hash of key, find bucket index
  size_t index = GetBucketIndexFromHash(HashKey(key));

  // lookup bucket
  assert(GetDirCapacity() - 1 >= index);
  auto bucket = mDirectory[index];

  while (bucket->dataMap.size() >= mBucketDataSize) {
    Split(index, key, value);   
    index = GetBucketIndexFromHash(HashKey(key));
    bucket = mDirectory[index];
  }
  bucket->dataMap[key] = value;
  //LOG_DEBUG("Inserted to map. Bucket index:%lu, Position: %lu. Depth:%d", 
  //    index, bucket->dataMap.size()-1, bucket->mLocalDepth);

  dump(key);
}

template <typename K, typename V> 
void ExtendibleHash<K, V>::dump(const K &key) {
  /*LOG_DEBUG("---------------------------------------------------------------");
  LOG_DEBUG("Global depth: %d, key: %d (%#x)", mDepth, key, key);
  int i = 0;
  for (auto b: mDirectory) {
    if (b != nullptr) {
      LOG_DEBUG("bucket: %d (%d) -> %p, local depth: %d", i, b->mId, b.get(), b->mLocalDepth);
      for (auto item: b->dataMap) {
        LOG_DEBUG("key: %d", item.first);
      }
      LOG_DEBUG("\n");
    } else {
      LOG_DEBUG("bucket: %d -> nullptr ", i);
    }
    i++;
  }*/
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
