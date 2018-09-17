/**
 * index_iterator.h
 * For range scan of b+ tree
 */
#pragma once
#include "page/b_plus_tree_leaf_page.h"
#include "buffer/buffer_pool_manager.h"
//#include "common/logger.h"

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

  bool isEnd() { 
    // if we go BEYOND our leaf node, and our leaf node does not have next, 
    // we are at the end
    if (leaf_->GetNextPageId() == INVALID_PAGE_ID && index_ >= leaf_->GetSize()) {
      return true;
    }
    return false; 
  }

  const MappingType &operator*() {
    return leaf_->GetItem(index_);
  }

  IndexIterator &operator++() { 
    index_++;
    // check if we need to switch to right sibling leaf node
    if (index_ >= leaf_->GetSize()) {
      page_id_t next = leaf_->GetNextPageId();
      if (next == INVALID_PAGE_ID) {
        //LOG_INFO("No more sibling in indexItr");
      } else {
        index_ = 0;
        buff_pool_manager_->UnpinPage(leaf_->GetPageId(), false);
        leaf_ =
            reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *> (buff_pool_manager_->FetchPage(next)->GetData());
        
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
