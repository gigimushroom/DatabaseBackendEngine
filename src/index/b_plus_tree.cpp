/**
 * b_plus_tree.cpp
 */
#include <iostream>
#include <string>
#include <assert.h>

#include "common/exception.h"
#include "common/logger.h"
#include "common/rid.h"
#include "index/b_plus_tree.h"
#include "page/header_page.h"

namespace cmudb {

INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(const std::string &name,
                                BufferPoolManager *buffer_pool_manager,
                                const KeyComparator &comparator,
                                page_id_t root_page_id)
    : index_name_(name), root_page_id_(root_page_id),
      buffer_pool_manager_(buffer_pool_manager), comparator_(comparator) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return (root_page_id_ == INVALID_PAGE_ID); }

/*****************************************************************************
 * SEARCH
 *****************************************************************************/
/*
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key,
                              std::vector<ValueType> &result,
                              Transaction *transaction) {
  
  auto *leaf = FindLeafPage(key);
  if (leaf == nullptr) {
    return false;
  }           

  result.resize(1);              
  ValueType v;
  if (leaf->Lookup(key, result[0], comparator_)) {
    return true; // return false if duplicate
  }
 
  return false;
}

/*****************************************************************************
 * INSERTION
 *****************************************************************************/
/*
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value,
                            Transaction *transaction) {
  bool result = true;
  if (IsEmpty()) {
    StartNewTree(key, value);
    UpdateRootPageId(true);    
  } else {
    result = InsertIntoLeaf(key, value, transaction);
  }    
  return result;
}
/*
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  LOG_INFO("Start new tree");
  page_id_t id;
  auto *page = buffer_pool_manager_->NewPage(id);
  if (page == nullptr) {
    LOG_INFO("StartNewTree failed due to buffer pool manager out of memory!");
    throw std::bad_alloc();
  }

  root_page_id_ = id;
  UpdateRootPageId(true);

  auto *lp =
        reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page->GetData());
  lp->Init(id, INVALID_PAGE_ID);
  lp->Insert(key, value, comparator_); 
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value,
                                    Transaction *transaction) {
  auto *leaf = FindLeafPage(key);
  if (leaf == nullptr) {
    return false;
  }                         
  ValueType v;
  if (leaf->Lookup(key, v, comparator_)) {
    return false; // return false if duplicate
  }

  auto originalSize = leaf->GetSize();
  auto newSize = leaf->Insert(key, value, comparator_);

  if (newSize > leaf->GetMaxSize()) {
    // we need to split
    LOG_INFO("insert into leaf causing split");
    B_PLUS_TREE_LEAF_PAGE_TYPE *newSiblingLeaf = Split(leaf);

    LOG_INFO("After split, old leaf is %s", leaf->ToString(false).c_str());

    LOG_INFO("After split, splited leaf is %s", newSiblingLeaf->ToString(false).c_str());

    KeyType keyInParent = newSiblingLeaf->GetItem(1).first;
    InsertIntoParent(leaf, keyInParent, newSiblingLeaf, nullptr);

    buffer_pool_manager_->UnpinPage(newSiblingLeaf->GetPageId(), true);
  }

  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);

  // checks
  if (originalSize == newSize) {
    LOG_INFO("InsertIntoLeaf original size equals to new size, insert failed");
  }
  return true;
}

/*
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N> N *BPLUSTREE_TYPE::Split(N *node) { 
  page_id_t id = -1;
  auto *page = buffer_pool_manager_->NewPage(id);
  if (page == nullptr) {
    LOG_INFO("Split failed due to buffer pool manager out of memory!");
    throw std::bad_alloc();
  }

  auto *BTreePage =
        reinterpret_cast<N *>(page->GetData());
  // Init method after creating a new leaf page
  BTreePage->Init(id, node->GetParentPageId());

  node->MoveHalfTo(BTreePage, buffer_pool_manager_); 

  return BTreePage;
}

/*
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node,
                                      const KeyType &key,
                                      BPlusTreePage *new_node,
                                      Transaction *transaction) {
  // find parent
  // When the insertion cause overflow from leaf page all the way upto the root
  // page, you should create a new root page and populate its elements.
  page_id_t parentPageId = old_node->GetParentPageId();
  if (parentPageId == INVALID_PAGE_ID) {
    Page *newPage = buffer_pool_manager_->NewPage(parentPageId);
    if (newPage == nullptr) {
      throw std::bad_alloc();
    }
    auto ip = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(newPage->GetData());
    ip->Init(parentPageId, INVALID_PAGE_ID);
    root_page_id_ = parentPageId; // set root is the new created parent page
    UpdateRootPageId(false);

    // assign old and new nodes parent
    old_node->SetParentPageId(parentPageId);
    new_node->SetParentPageId(parentPageId);

    // Note: important APi for new root to add 2 nodes
    ip->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());

    buffer_pool_manager_->UnpinPage(parentPageId, true);
  } else {
    auto *pPage = buffer_pool_manager_->FetchPage(parentPageId);
    auto parentNode =
          reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(pPage->GetData());

    // call InsertNodeAfter() for new node
    int parentCurSize = parentNode->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());  

    // if size overflow
    if (parentCurSize > parentNode->GetMaxSize()) {
      // we need to split, then insertIntoParent()
      BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *newSiblingParentNode = Split(parentNode);

      // we need to push up the first pair from new splitted node
      auto pair = newSiblingParentNode->PushUpIndex();      

      InsertIntoParent(parentNode, pair.first, newSiblingParentNode, nullptr);
    }
  }
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {
  if (IsEmpty()) {
    return;
  }

  auto *leaf = FindLeafPage(key);
  if (leaf == nullptr) {
    return;
  }

  auto shouldRemovePage = false;
  int totalSize = leaf->RemoveAndDeleteRecord(key, comparator_);     
  if (totalSize < (leaf->GetMinSize())) {
    shouldRemovePage = CoalesceOrRedistribute(leaf, transaction);
  }

  // unpin if after done
  buffer_pool_manager_->UnpinPage(leaf->GetPageId(), true);
  if (shouldRemovePage) {
    LOG_INFO("BPLUSTREE_TYPE::Remove: Leaf page from buffer pool should be already removed.");
    //auto deletePage = buffer_pool_manager_->DeletePage(leaf->GetPageId());
    return;
  }
}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction) {

  // if node size if within range [max/2, max], we are good to leave
  if (node->GetSize() >= (node->GetMinSize())) {
    return false;
  }

  page_id_t parentPageId = node->GetParentPageId();
  if (parentPageId == INVALID_PAGE_ID) {
    // we need to adjust root
    assert(root_page_id_ == node->GetPageId());
    return AdjustRoot(node);
  }
  
  // Our plan here is to try left or right sibling to redistribute, if neither of them failed, then we merge
  auto *rawPage = buffer_pool_manager_->FetchPage(parentPageId);
  auto pPage = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(rawPage->GetData());
    
  int index = pPage->ValueIndex(node->GetPageId());

  page_id_t v = INVALID_PAGE_ID;
  
  // if we can redistribute
  bool isLeftSibling = false;
  bool isRightSibling = false;

  // Check if left sibling can redistribute
  if (index - 1 > 0) {
    v = pPage->ValueIndex(index - 1);
    auto *siblingRawPage = buffer_pool_manager_->FetchPage(v);
    auto *sibling = reinterpret_cast<decltype(node)>(siblingRawPage->GetData());
    assert(sibling);
    isLeftSibling = true;

    if (sibling->GetSize() + node->GetSize() > node->GetMaxSize()) {
      Redistribute(sibling, node, index);
      buffer_pool_manager_->UnpinPage(v, false); 
      return false;
    }
    // unpin
    buffer_pool_manager_->UnpinPage(v, false); 
  } 
  
  // Check if right sibling can redistribute
  if (index + 1 < pPage->GetSize()) {
    v = pPage->ValueIndex(index + 1);
    auto *siblingRawPage = buffer_pool_manager_->FetchPage(v);
    auto *sibling = reinterpret_cast<decltype(node)>(siblingRawPage->GetData());
    isRightSibling = true;

    if (sibling->GetSize() + node->GetSize() > node->GetMaxSize()) {
      Redistribute(sibling, node, 0); // Right sibling set 'index" to 0
      buffer_pool_manager_->UnpinPage(v, false);
      return false;
    }
    // unpin
    buffer_pool_manager_->UnpinPage(v, false); 
  }

  assert(isLeftSibling || isRightSibling);
  
  // Prefer left sibling for merge
  if (isLeftSibling) {
    v = pPage->ValueIndex(index - 1);
  } else {
    v = pPage->ValueIndex(index + 1);
  }

  auto *siblingRawPage = buffer_pool_manager_->FetchPage(v);
  auto *sibling = reinterpret_cast<decltype(node)>(siblingRawPage->GetData());

  Coalesce(sibling, node, pPage, index, transaction);

  // unpin
  buffer_pool_manager_->UnpinPage(v, false);  

  // node needs coalesce. Leaf node needs to be deleted. So we mark as true.
  return true;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(
    N *&neighbor_node, N *&node,
    BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *&parent,
    int index, Transaction *transaction) {
  // Move all the key & value pairs from one page to its sibling page
  // We do this way due to we got the node index in parent side easily, so we could remove it
  node->MoveAllTo(neighbor_node, index, buffer_pool_manager_);

  bool result = buffer_pool_manager_->DeletePage(node->GetPageId());
  if (!result) {
    LOG_INFO("BPLUSTREE_TYPE::Coalesce: Failed to delete page from buffer pool manager");
  }
  // Remove node from its parent
  parent->Remove(index);
  
  auto shouldDelParent = false;
  if (parent->GetSize() < parent->GetMinSize()) {
    // parent needs to adjust
    shouldDelParent = CoalesceOrRedistribute(parent, transaction);
  }
  return shouldDelParent;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {
  if (index == 0) {
    neighbor_node->MoveFirstToEndOf(node, buffer_pool_manager_);
  } else {
    neighbor_node->MoveLastToFrontOf(node, index, buffer_pool_manager_);
  }
}

/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) {
  if (old_root_node->GetSize() == 2) {
    // case 2
    if (old_root_node->IsLeafPage()) {
      // when you delete the last element in whole b+ tree
      root_page_id_ = INVALID_PAGE_ID;
      UpdateRootPageId(false);
      return true;
    }

    // case 1
    // root only has one child which need to be deleted
    // child is the new root
    auto root = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(old_root_node);
    page_id_t childPageId = root->ValueAt(1);

    // set child page's parent to be invalid
    auto *rawPage = buffer_pool_manager_->FetchPage(childPageId);
    auto childPage = reinterpret_cast<BPlusTreePage *>(rawPage->GetData());
    childPage->SetParentPageId(INVALID_PAGE_ID);

    root_page_id_ = childPageId;
    UpdateRootPageId(false);

    buffer_pool_manager_->UnpinPage(childPageId, true); 
    return true; // root needs to be deleted
  }
  return false;
}

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin() {
  KeyType dummyKey;
  auto *lp = FindLeafPage(dummyKey, true);

  return INDEXITERATOR_TYPE(lp, 0, buffer_pool_manager_);
}

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) {
  auto *lp = FindLeafPage(key, false);

  return INDEXITERATOR_TYPE(lp, lp->KeyIndex(key, comparator_), buffer_pool_manager_);
}

/*****************************************************************************
 * UTILITIES AND DEBUG
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */
INDEX_TEMPLATE_ARGUMENTS
B_PLUS_TREE_LEAF_PAGE_TYPE *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key,
                                                         bool leftMost) {
  if (IsEmpty()) {
    return nullptr;
  }

  page_id_t page_id = root_page_id_;                                                       
  auto *rawPage = buffer_pool_manager_->FetchPage(page_id);
  BPlusTreePage *page =
        reinterpret_cast<BPlusTreePage *>(rawPage->GetData());

  while (!page->IsLeafPage() && page != nullptr) {
    // cast to internal page
    auto *internalPage =
        reinterpret_cast<BPlusTreeInternalPage<KeyType,page_id_t,KeyComparator> *>(rawPage->GetData());
    int unPinPage = internalPage->GetPageId();

    // if leftMost flag == true, find the left most leaf page
    if (leftMost) {
      page_id = internalPage->ValueAt(1);
    } else {
      page_id = internalPage->Lookup(key, comparator_);
    }
    
    rawPage = buffer_pool_manager_->FetchPage(page_id);
    page = reinterpret_cast<BPlusTreePage *>(rawPage->GetData());

    // unpin previous page
    buffer_pool_manager_->UnpinPage(unPinPage, false);
  }

  buffer_pool_manager_->UnpinPage(page_id, false);

  return reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(page);;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  HeaderPage *header_page = static_cast<HeaderPage *>(
      buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record)
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  else
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for debug only
 * print out whole b+tree sturcture, rank by rank
 */
INDEX_TEMPLATE_ARGUMENTS
std::string BPLUSTREE_TYPE::ToString(bool verbose) { 
  
  if (IsEmpty()) { return "Empty tree"; }

  std::string result;
  std::vector<page_id_t> v{root_page_id_};

  std::string caution;
  int depth = 0;
  while (!v.empty()) {
    std::vector<page_id_t> next;
    result += "\nNow visiting depth " + std::to_string(depth) + ": ";
    for (auto page_id : v) {
      result += "\n";
      auto *rawPage = buffer_pool_manager_->FetchPage(page_id);
      BPlusTreePage *item =
        reinterpret_cast<BPlusTreePage *>(rawPage->GetData());

      if (item->IsLeafPage()) {
        auto leaf = reinterpret_cast<B_PLUS_TREE_LEAF_PAGE_TYPE *>(item);
        result += leaf->ToString(verbose);
      } else {
        auto inner = reinterpret_cast<BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> *>(item);
        result += inner->ToString(verbose);
        for (int i = 0; i < inner->GetSize(); i++) {
          page_id_t page = inner->ValueAt(i);
          next.push_back((page));
        }
      }

      int cnt = buffer_pool_manager_->FetchPage(page_id)->GetPinCount();
      result += " ref: " + std::to_string(cnt);
      buffer_pool_manager_->UnpinPage(item->GetPageId(), false);
      if (cnt != 2) {
        caution += std::to_string(page_id) + " cnt:" + std::to_string(cnt);
      }

      buffer_pool_manager_->UnpinPage(item->GetPageId(), false);
    }
    swap(v, next);
    depth++;
  }
  //assert(caution.empty());
  return result + caution;
  
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;

    KeyType index_key;
    index_key.SetFromInteger(key);
    RID rid(key);
    Insert(index_key, rid, transaction);
  }
}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name,
                                    Transaction *transaction) {
  int64_t key;
  std::ifstream input(file_name);
  while (input) {
    input >> key;
    KeyType index_key;
    index_key.SetFromInteger(key);
    Remove(index_key, transaction);
  }
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

} // namespace cmudb
