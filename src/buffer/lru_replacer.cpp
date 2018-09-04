/**
 * LRU implementation
 */
#include "buffer/lru_replacer.h"
#include "page/page.h"
#include "common/logger.h"

namespace cmudb {

template <typename T> LRUReplacer<T>::LRUReplacer() {}

template <typename T> LRUReplacer<T>::~LRUReplacer() {}

/*
 * Insert value into LRU
 */
template <typename T> void LRUReplacer<T>::Insert(const T &value) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto search = itemMap_.find(value);
  if (search != itemMap_.end()) {
    // find existing, remove it
    itemList_.erase(search->second);
    itemMap_.erase(search->first);
  }
  itemList_.push_front(value);
  itemMap_.insert(make_pair(value, itemList_.begin()));

  //LOG_DEBUG("inserted value %d", value);
}

/* If LRU is non-empty, pop the head member from LRU to argument "value", and
 * return true. If LRU is empty, return false
 */
template <typename T> bool LRUReplacer<T>::Victim(T &value) {
  std::lock_guard<std::mutex> lock(mutex_);
  if (!itemList_.empty()) {
    value = itemList_.back();
    itemMap_.erase(value);
    itemList_.pop_back();
    return true;
  }
  return false;
}

/*
 * Remove value from LRU. If removal is successful, return true, otherwise
 * return false
 */
template <typename T> bool LRUReplacer<T>::Erase(const T &value) {
  std::lock_guard<std::mutex> lock(mutex_);
  auto search = itemMap_.find(value);
  if (search != itemMap_.end()) {
    // we found it
    itemList_.erase(search->second);
    itemMap_.erase(search);

    //LOG_DEBUG("removed value %d", value);
    return true;
  }
  return false;
}

template <typename T> size_t LRUReplacer<T>::Size() {
  std::lock_guard<std::mutex> lock(mutex_);
  return itemMap_.size(); 
}

//template class LRUReplacer<Page *>;
// test only
template class LRUReplacer<int>;

} // namespace cmudb
