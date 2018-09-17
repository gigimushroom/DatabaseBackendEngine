/**
 * b_plus_tree_leaf_page.cpp
 */

#include <sstream>

#include "common/exception.h"
#include "common/rid.h"
#include "page/b_plus_tree_leaf_page.h"
#include "common/logger.h"
#include "page/b_plus_tree_internal_page.h"

namespace cmudb {

/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/

/**
 * Init method after creating a new leaf page
 * Including set page type, set current size to zero, set page id/parent id, set
 * next page id and set max size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id) {
  SetPageType(IndexPageType::LEAF_PAGE);
  SetSize(0);                            
  SetPageId(page_id);
  SetParentPageId(parent_id);

  SetNextPageId(INVALID_PAGE_ID);
  
  // header size is 24 bytes
  // Total record size divded by each record size is max allowed size
  // IMPORTANT: leave a always available slot for insertion! Otherwise, insert will cause memory stomp
  int size = (PAGE_SIZE - sizeof(B_PLUS_TREE_LEAF_PAGE_TYPE)) / sizeof(MappingType) - 1; 
  LOG_INFO("Max size of leaf page is: %d", size);
  SetMaxSize(size);
}

/**
 * Helper methods to set/get next page id
 */
INDEX_TEMPLATE_ARGUMENTS
page_id_t B_PLUS_TREE_LEAF_PAGE_TYPE::GetNextPageId() const {
  return next_page_id_;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::SetNextPageId(page_id_t next_page_id) {
  next_page_id_ = next_page_id;
}

/**
 * Helper method to find the first index i so that array[i].first >= key
 * NOTE: This method is only used when generating index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex(
    const KeyType &key, const KeyComparator &comparator) const {
  for (int i = 0; i < GetSize(); ++i) {
    if (comparator(array[i].first, key) >= 0) {
      return i;
    }
  }
  //LOG_INFO("B_PLUS_TREE_LEAF_PAGE_TYPE::KeyIndex: no key larger than %d found", key)
  return -1;
}

/*
 * Helper method to find and return the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_LEAF_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  return array[index].first;
}

/*
 * Helper method to find and return the key & value pair associated with input
 * "index"(a.k.a array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
const MappingType &B_PLUS_TREE_LEAF_PAGE_TYPE::GetItem(int index) {
  // replace with your own code
  return array[index];
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert key & value pair into leaf page ordered by key
 * @return  page size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::Insert(const KeyType &key,
                                       const ValueType &value,
                                       const KeyComparator &comparator) {
  MappingType pair;
  pair.first = key;
  pair.second = value;

  int upperBoundKeyIndex = KeyIndex(key, comparator);
  if (upperBoundKeyIndex == -1) {
    // all index larger than key, key should be inserted to last
    array[GetSize()] = pair;
  } else {
    // insert pair to upperBoundKeyIndex - 1
    // shuffle array to right
    for (int i = GetSize(); i >= upperBoundKeyIndex; i--) {
      array[i + 1] = array[i];
    }
    array[upperBoundKeyIndex] = pair;
  }

  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveHalfTo(
    BPlusTreeLeafPage *recipient,
    __attribute__((unused)) BufferPoolManager *buffer_pool_manager) {
  // remove from split index to the end
  int splitIndex = (GetSize() + 1) / 2;
  recipient->CopyHalfFrom(&array[splitIndex], GetSize() - splitIndex);

  IncreaseSize(-1 * (GetSize() - splitIndex));
  // adjust next page id since recipient is a new page
  recipient->SetNextPageId(GetNextPageId());
  SetNextPageId(recipient->GetPageId());
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyHalfFrom(MappingType *items, int size) {
  // it is a new page
  for (int i = 0; i < size; i++) {
    // array starts from index 0
    array[i] = items[i];
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * LOOKUP
 *****************************************************************************/
/*
 * For the given key, check to see whether it exists in the leaf page. If it
 * does, then store its corresponding value in input "value" and return true.
 * If the key does not exist, then return false
 */
INDEX_TEMPLATE_ARGUMENTS
bool B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup(const KeyType &key, ValueType &value,
                                        const KeyComparator &comparator) const {
  // TODO: Use binary search
  for(int i = 0; i < GetSize(); i++) {
    int result = comparator(array[i].first,key);
    if (result == 0) {
      //LOG_INFO("B_PLUS_TREE_LEAF_PAGE_TYPE::Lookup: Found a value based on key in index: %d", i);
      value = array[i].second;
      return true;
    }
  }                                          
  return false;
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * First look through leaf page to see whether delete key exist or not. If
 * exist, perform deletion, otherwise return immdiately.
 * NOTE: store key&value pair continuously after deletion
 * @return   page size after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord(
    const KeyType &key, const KeyComparator &comparator) {
  
  int keyIndex = -1;
  for (int i = 0; i < GetSize(); ++i) {
    if (comparator(array[i].first, key) == 0) {
      keyIndex = i;
      break;
    }
  }

  if (keyIndex == -1) {
    LOG_INFO("B_PLUS_TREE_LEAF_PAGE_TYPE::RemoveAndDeleteRecord: key not found");
    return 0; // Not found
  }

  // shuffle to left by 1
  for (int i = keyIndex; i < GetSize(); ++i) {
    array[i] = array[i+1];
  }
  IncreaseSize(-1);
  return GetSize();
}

/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page, then
 * update next page id
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveAllTo(BPlusTreeLeafPage *recipient,
                                           int, BufferPoolManager *) {
  // NOTE: This function assume current page is at the right hand of recipient

  recipient->CopyAllFrom(&array[0], GetSize());
  // leaf has no children
  
  // assumption: current page is at the right hand of recipient
  recipient->SetNextPageId(GetNextPageId());

  SetSize(0); // we are empty
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyAllFrom(MappingType *items, int size) {
  int startIndex = GetSize();
  for (int i = 0; i < size; i++) {
    array[startIndex + i] = items[i];
  }    
  IncreaseSize(size);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveFirstToEndOf(
    BPlusTreeLeafPage *recipient,
    BufferPoolManager *buffer_pool_manager) {

  recipient->CopyLastFrom(array[0]);
  // remove index 0 node
  for (int i = 0; i < GetSize(); ++i) {
    array[i] = array[i + 1];
  }

  auto *pPage = buffer_pool_manager->FetchPage(GetParentPageId());
  auto *parentNode =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(pPage->GetData());

  // if parent's node key is our removed key, change the node key
  int ourPageIdInParentIndex = parentNode->ValueIndex(GetPageId());
  parentNode->SetKeyAt(ourPageIdInParentIndex, KeyAt(0)); // Our new first key. Copy up to parent

  buffer_pool_manager->UnpinPage(pPage->GetPageId(), true);
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyLastFrom(const MappingType &item) {
  array[GetSize()] = item;
  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to "recipient" page, then
 * update relavent key & value pair in its parent page.
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::MoveLastToFrontOf(
    BPlusTreeLeafPage *recipient, int parentIndex,
    BufferPoolManager *buffer_pool_manager) {

  MappingType pair = array[GetSize() - 1];
  recipient->CopyFirstFrom(pair, parentIndex, buffer_pool_manager);

  auto *pPage = buffer_pool_manager->FetchPage(GetParentPageId());
  auto *parentNode =
        reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(pPage->GetData());

  // parentIndex should recipient's position in parent node
  // update the key content
  parentNode->SetKeyAt(parentIndex, pair.first); 

  // now we do clean up, including remove the last node
  buffer_pool_manager->UnpinPage(pPage->GetPageId(), true);

  // index is the the last one, so we don't need to shuffle, just reduce our size
  IncreaseSize(-1);
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_LEAF_PAGE_TYPE::CopyFirstFrom(
    const MappingType &item, int parentIndex,
    BufferPoolManager *buffer_pool_manager) {

  // move every item to the next, to give space to our new record
  // loop until i > 0
  for (int i = GetSize(); i > 0; i--) {
    array[i] = array[i - 1];
  }
  array[0] = item;
  IncreaseSize(1);
}

/*****************************************************************************
 * DEBUG
 *****************************************************************************/
INDEX_TEMPLATE_ARGUMENTS
std::string B_PLUS_TREE_LEAF_PAGE_TYPE::ToString(bool verbose) const {
  if (GetSize() == 0) {
    return "";
  }
  std::ostringstream stream;
  if (verbose) {
    stream << "[pageId: " << GetPageId() << " parentId: " << GetParentPageId()
           << "]<" << GetSize() << "> ";
  }
  int entry = 0;
  int end = GetSize();
  bool first = true;

  while (entry < end) {
    if (first) {
      first = false;
    } else {
      stream << " ";
    }
    stream << std::dec << array[entry].first;
    if (verbose) {
      stream << "(" << array[entry].second << ")";
    }
    ++entry;
  }
  return stream.str();
}

template class BPlusTreeLeafPage<GenericKey<4>, RID,
                                       GenericComparator<4>>;
template class BPlusTreeLeafPage<GenericKey<8>, RID,
                                       GenericComparator<8>>;
template class BPlusTreeLeafPage<GenericKey<16>, RID,
                                       GenericComparator<16>>;
template class BPlusTreeLeafPage<GenericKey<32>, RID,
                                       GenericComparator<32>>;
template class BPlusTreeLeafPage<GenericKey<64>, RID,
                                       GenericComparator<64>>;
} // namespace cmudb
