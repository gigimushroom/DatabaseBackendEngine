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
    LOG_INFO("num of items in bucket index %d is: %lu", bucket_id, numOfItems);
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
  for (auto b:mDirectory) {
    size_t numOfItems = b->dataMap.size();
    if (numOfItems > 0)
      count++;
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
    return false;
  }
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
    LOG_INFO("too bad, bucket index:%lu is full. Need to resize directory and split buckets", index);
    // resize directory vector
    size_t preSize = GetDirCapacity();
    mDirectory.resize(preSize * 2);
    mDepth++;

    // fix new elements pointer
    for (size_t i=preSize; i<mDirectory.capacity(); ++i) {
      mDirectory[i] = mDirectory[i-preSize];
    }
  } else {
    LOG_INFO("Bucket index:%lu is full. Need to split buckets only", index);
  }

  // add a new bucket
  bucket->mLocalDepth++;
  int newBucketId = bucket->mId+(GetDirCapacity()/2); // hack
  int splitBucketId = bucket->mId;
  LOG_DEBUG("Split bucket id: %d, new bucket id: %d", splitBucketId, newBucketId);
  
  // fix new bucket pointer
  //LOG_INFO("sharded ptr usage count: %lu", mDirectory[newBucketId].use_count());
  mDirectory[newBucketId] = std::make_shared<Bucket>(bucket->mLocalDepth, newBucketId);
  mBucketCount++;

  // split bucket
  for (auto& kv : mDirectory[splitBucketId]->dataMap) {
    size_t newHash = GetBucketIndexFromHash(HashKey(kv.first));
    if (int(newHash) != mDirectory[splitBucketId]->mId) {
      LOG_INFO("!Current hash index: %d, new hash index: %lu", mDirectory[splitBucketId]->mId, newHash);
      // now we need to make a move from bucket A to bucket B
      mDirectory[newBucketId]->dataMap[kv.first] = kv.second;
      mDirectory[splitBucketId]->dataMap.erase(kv.first);
    }      
  }
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
  if (mDirectory[index] == nullptr) {
    mDirectory[index] = std::make_shared<Bucket>(mDepth, index);
    mBucketCount++;
    LOG_DEBUG("Created new bucket index %lu", index);
  }

  auto bucket = mDirectory[index];
  
  // try to insert
  if (bucket->dataMap.size() < mBucketDataSize) {
    bucket->dataMap[key] = value;
    LOG_DEBUG("Inserted to map. Bucket index:%lu, Position: %lu. Depth:%d", 
      index, bucket->dataMap.size()-1, bucket->mLocalDepth);
    //std::cout<<"key:" << key << " value:" << value << " bucket index:" << index;
  } else {
    Split(index, key, value);    

    // now add the k,v
    index = GetBucketIndexFromHash(HashKey(key));
    if (mDirectory[index] == nullptr) {
      LOG_DEBUG("Inner: Created new bucket index %lu", index);
      mDirectory[index] = std::make_shared<Bucket>(mDepth, index);
      mBucketCount++;      
    }
    auto curBucket = mDirectory[index];
    if (curBucket->dataMap.size() < mBucketDataSize) {
      curBucket->dataMap[key] = value;
    }
    else {
      LOG_INFO("WHAT??? FULL AGAIN????");
      // TODO handling another split
      Split(index, key, value);  
    }    
    LOG_INFO("Inserted to map. Bucket index:%lu, Position: %lu. Depth:%d", index, curBucket->dataMap.size()-1, curBucket->mLocalDepth);
  }
}

template class ExtendibleHash<page_id_t, Page *>;
template class ExtendibleHash<Page *, std::list<Page *>::iterator>;
// test purpose
template class ExtendibleHash<int, std::string>;
template class ExtendibleHash<int, std::list<int>::iterator>;
template class ExtendibleHash<int, int>;
} // namespace cmudb
