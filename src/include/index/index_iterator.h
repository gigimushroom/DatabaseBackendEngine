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

  bool isEnd() { return (leaf_->GetNextPageId() == INVALID_PAGE_ID); }

  const MappingType &operator*() {
    return leaf_->GetItem(index_);
  }

  IndexIterator &operator++() { 
    index++;
    // check if we need to switch to right sibling leaf node
    if (index >= leafPage->GetSize()) {
      page_id_t next = leafPage->GetNextPageId();
      if (next == INVALID_PAGE_ID) {
        LOG_INFO("No more sibling in indexItr");
      } else {
        index = 0;
        bufferPoolManager.UnpinPage(leaf_->GetPageId(), false);
        leafPage =
            reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *> (bufferPoolManager.FetchPage(next)->GetData());
      }
    }
    return *this;
  }

private:
  // add your own private member variables here
  BPlusTreeLeafPage<KeyType, ValueType, KeyComparator> *leaf_;
  int index_;
  BufferPoolManager *buff_pool_manager_;
};


} // namespace cmudb
