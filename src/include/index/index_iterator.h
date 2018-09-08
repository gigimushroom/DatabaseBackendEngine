/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "page/b_plus_tree_leaf_page.h"
#include "buffer/buffer_pool_manager.h"

namespace cmudb {

#define INDEXITERATOR_TYPE                                                     \
  IndexIterator<KeyType, ValueType, KeyComparator>

INDEX_TEMPLATE_ARGUMENTS
class IndexIterator {
public:
  // you may define your own constructor based on your member variables
  IndexIterator(BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *,
                int, BufferPoolManager *);

  IndexIterator() {}
  ~IndexIterator();

  bool isEnd() { return true; }

  const MappingType &operator*() {
    return leaf_->GetItem(0);; 
  }

  IndexIterator &operator++() { return *this; }

private:
  // add your own private member variables here
  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *leaf_;
  int index_;
  BufferPoolManager *buff_pool_manager_;
};


} // namespace cmudb
