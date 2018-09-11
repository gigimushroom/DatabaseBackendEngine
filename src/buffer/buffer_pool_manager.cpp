#include "buffer/buffer_pool_manager.h"
#include "common/logger.h"

namespace cmudb {

/*
 * BufferPoolManager Constructor
 * When log_manager is nullptr, logging is disabled (for test purpose)
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::BufferPoolManager(size_t pool_size,
                                                 DiskManager *disk_manager,
                                                 LogManager *log_manager)
    : pool_size_(pool_size), disk_manager_(disk_manager),
      log_manager_(log_manager) {
  // a consecutive memory space for buffer pool
  pages_ = new Page[pool_size_];
  page_table_ = new ExtendibleHash<page_id_t, Page *>(BUCKET_SIZE);
  replacer_ = new LRUReplacer<Page *>;
  free_list_ = new std::list<Page *>;

  // put all the pages into free list
  for (size_t i = 0; i < pool_size_; ++i) {
    free_list_->push_back(&pages_[i]);
  }
}

/*
 * BufferPoolManager Deconstructor
 * WARNING: Do Not Edit This Function
 */
BufferPoolManager::~BufferPoolManager() {
  delete[] pages_;
  delete page_table_;
  delete replacer_;
  delete free_list_;
}

/**
 * 1. search hash table.
 *  1.1 if exist, pin the page and return immediately
 *  1.2 if no exist, find a replacement entry from either free list or lru
 *      replacer. (NOTE: always find from free list first)
 * 2. If the entry chosen for replacement is dirty, write it back to disk.
 * 3. Delete the entry for the old page from the hash table and insert an
 * entry for the new page.
 * 4. Update page metadata, read page content from disk file and return page
 * pointer
 */
Page *BufferPoolManager::FetchPage(page_id_t page_id) {
  Page * page = nullptr;
  if (page_table_->Find(page_id, page)) {
    page->pin_count_++;
    //LOG_INFO("FetchPage: page id %d still in hashtable, count: %d", page_id, page->pin_count_);
    return page;
  } else {
    if (!free_list_->empty()) {
      page = free_list_->front();
      free_list_->pop_front();
    } else {
      // Every time we victim a page, we need to write to disk if dirty
      // Then remove old entry from hashtable, and inser new entry
      if (!replacer_->Victim(page)) {
        return nullptr;
      }
    }
  }

  if (page != nullptr) {
    if (page->is_dirty_) {
      // write existing data back to disk
      disk_manager_->WritePage(page->page_id_, page->GetData());
    }

    page_table_->Remove(page->page_id_);
    page_table_->Insert(page_id, page);

    page->pin_count_ = 1;
    page->is_dirty_ = false;    
    page->page_id_ = page_id;
    //LOG_INFO("FetchPage final: page id %d inserted, and pin count is %d", page_id, page->pin_count_);
    disk_manager_->ReadPage(page_id, page->GetData());
    return page;
  }

  return nullptr; 
}

/*
 * Implementation of unpin page
 * if pin_count>0, decrement it and if it becomes zero, put it back to
 * replacer if pin_count<=0 before this call, return false. is_dirty: set the
 * dirty flag of this page
 */
bool BufferPoolManager::UnpinPage(page_id_t page_id, bool is_dirty) {
  Page * page = nullptr;
  if (page_table_->Find(page_id, page)) {
    if (page->pin_count_ <= 0) {
      return false;
    }
    page->pin_count_--;
    if (page->pin_count_ == 0) {
      replacer_->Insert(page);
    }
    LOG_INFO("page id %d inserted to hashtable, pin count: %d", page_id, page->pin_count_);
    if (is_dirty) {
      page->is_dirty_ = true;
    }
    return true;
  }
  return false;
}

/*
 * Used to flush a particular page of the buffer pool to disk. Should call the
 * write_page method of the disk manager
 * if page is not found in page table, return false
 * NOTE: make sure page_id != INVALID_PAGE_ID
 */
bool BufferPoolManager::FlushPage(page_id_t page_id) { 
  Page * page = nullptr;
  if (page_id == INVALID_PAGE_ID || !page_table_->Find(page_id, page)) {
    return false;
  }
  disk_manager_->WritePage(page_id, page->GetData());
  return true; 
}

/**
 * User should call this method for deleting a page. This routine will call
 * disk manager to deallocate the page. First, if page is found within page
 * table, buffer pool manager should be reponsible for removing this entry out
 * of page table, reseting page metadata and adding back to free list. Second,
 * call disk manager's DeallocatePage() method to delete from disk file. If
 * the page is found within page table, but pin_count != 0, return false
 */
bool BufferPoolManager::DeletePage(page_id_t page_id) { 
  Page * page = nullptr;
  if (page_id == INVALID_PAGE_ID || !page_table_->Find(page_id, page)) {
    return false;
  }

  if (page->pin_count_ != 0) {
    return false;
  }

  page->ResetMemory();
  // add to free list, remove from lRU, and hashtable
  
  replacer_->Erase(page);
  page_table_->Remove(page_id);
  disk_manager_->DeallocatePage(page_id);

  page->page_id_ = INVALID_PAGE_ID;
  page->is_dirty_ = false;
  free_list_->push_back(page);

  return true; 
}

/**
 * User should call this method if needs to create a new page. This routine
 * will call disk manager to allocate a page.
 * Buffer pool manager should be responsible to choose a victim page either
 * from free list or lru replacer(NOTE: always choose from free list first),
 * update new page's metadata, zero out memory and add corresponding entry
 * into page table. return nullptr if all the pages in pool are pinned
 */
Page *BufferPoolManager::NewPage(page_id_t &page_id) {  
  Page * res = nullptr;
  if (!free_list_->empty()) {
    res = free_list_->front();
    free_list_->pop_front();
  } else {
    if (!replacer_->Victim(res)) {
      return nullptr;
    }
    LOG_INFO("page id %d is victim page, removed!", res->page_id_);
  }

  page_id = disk_manager_->AllocatePage();
  if (res->is_dirty_) {
    disk_manager_->WritePage(res->page_id_, res->GetData());
  }
  
  // remove old entry, add new entry
  page_table_->Remove(res->page_id_);
  page_table_->Insert(page_id, res);

  res->ResetMemory();
  res->pin_count_ = 1;
  res->page_id_ = page_id;
  res->is_dirty_ = false;
  
  return res;
}

} // namespace cmudb
