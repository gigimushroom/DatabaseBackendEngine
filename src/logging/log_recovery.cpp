/**
 * log_recovey.cpp
 */

#include "logging/log_recovery.h"
#include "page/table_page.h"

namespace cmudb {
/*
 * deserialize a log record from log buffer
 * @return: true means deserialize succeed, otherwise can't deserialize cause
 * incomplete log record
 */
bool LogRecovery::DeserializeLogRecord(const char *data,
                                             LogRecord &log_record) {
  //char * data = data + offset_;
  int32_t size_ = *reinterpret_cast<const int *>(data);  
  std::cout << "size" << size_ << std::endl;
  lsn_t lsn_ = *reinterpret_cast<const lsn_t *>(data + 4);;
  txn_id_t txn_id_ = *reinterpret_cast<const txn_id_t *>(data + 8);
  lsn_t prev_lsn_ = *reinterpret_cast<const lsn_t *>(data + 12);
  LogRecordType log_record_type_ = *reinterpret_cast<const LogRecordType *>(data + 16);
  
  if (size_ < 0 || lsn_ == INVALID_LSN || txn_id_ == INVALID_TXN_ID ||
    log_record_type_ == LogRecordType::INVALID) {
      return false;
  }

  log_record.size_ = size_;
  log_record.lsn_ = lsn_;
  log_record.txn_id_ = txn_id_;
  log_record.prev_lsn_ = prev_lsn_;
  log_record.log_record_type_ = log_record_type_;

  std::cout << "Logged:" << log_record.ToString().c_str() << std::endl; 

  switch(log_record_type_) {
    case LogRecordType::INSERT: {
      log_record.insert_rid_ = *reinterpret_cast<const int *>(data + 20);
      // | HEADER | tuple_rid | tuple_size | tuple_data
      // We skip header, RID, then desrialize.
      // First value is tuple size, then data
      log_record.insert_tuple_.DeserializeFrom(data + 20 + sizeof(RID));
      break;
    }
    case LogRecordType::MARKDELETE:
    case LogRecordType::ROLLBACKDELETE:
    case LogRecordType::APPLYDELETE:
    {
      log_record.delete_rid_ = *reinterpret_cast<const int *>(data + 20);
      log_record.delete_tuple_.DeserializeFrom(data + 20 + sizeof(RID));
      break;
    }
    case LogRecordType::UPDATE: {
      log_record.update_rid_ = *reinterpret_cast<const int *>(data + 20);
      log_record.old_tuple_.DeserializeFrom(data + 20 + sizeof(RID));
      // header + RID + sizeof(int) + old tuple size 
      log_record.new_tuple_.DeserializeFrom(data + 20 + sizeof(RID) + 4 + log_record.old_tuple_.GetLength());
      break;
    }
    case LogRecordType::NEWPAGE: {
      log_record.prev_page_id_ = *reinterpret_cast<const page_id_t *>(
        data + LogRecord::HEADER_SIZE);
      break;
    }
    default:
      break;
  }
  return true;
}

/*
 *redo phase on TABLE PAGE level(table/table_page.h)
 *read log file from the beginning to end (you must prefetch log records into
 *log buffer to reduce unnecessary I/O operations), remember to compare page's
 *LSN with log_record's sequence number, and also build active_txn_ table &
 *lsn_mapping_ table
 */
void LogRecovery::Redo() {
  ENABLE_LOGGING = false;
  offset_ = 0; // no check point

  std::cout << "Start log recovery redo process.." << std::endl;
  while (disk_manager_->ReadLog(log_buffer_, LOG_BUFFER_SIZE, offset_)) { // false means log eof
    int buffer_offset = 0;
    LogRecord log_record;
    while (DeserializeLogRecord(log_buffer_ + buffer_offset, log_record)) {
      active_txn_[log_record.txn_id_] = log_record.lsn_;

      if (log_record.log_record_type_ == LogRecordType::INSERT) {
        RID rid = log_record.insert_rid_;
        auto page = buffer_pool_manager_->FetchPage(rid.GetPageId());
        if (page->GetLSN() >= log_record.lsn_) {
          continue;
        }
        auto *tablePage = reinterpret_cast<TablePage *>(page);
        auto result = tablePage->InsertTuple(log_record.insert_tuple_, log_record.insert_rid_, 
          nullptr, nullptr, nullptr);
        assert(result);
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
      } else if (log_record.GetLogRecordType() == LogRecordType::NEWPAGE) {
          page_id_t pre_page_id = log_record.prev_page_id_;
          TablePage *page;
          // the first page
          if (pre_page_id == INVALID_PAGE_ID) {
            page = reinterpret_cast<TablePage *>(
                buffer_pool_manager_->NewPage(pre_page_id));
            assert(page != nullptr);
            page->WLatch();
            page->Init(pre_page_id, PAGE_SIZE, INVALID_PAGE_ID, nullptr, nullptr);
            page->WUnlatch();
          } else {
            page = reinterpret_cast<TablePage *>(
                buffer_pool_manager_->FetchPage(pre_page_id));
            assert(page != nullptr);

            if (page->GetNextPageId() == INVALID_PAGE_ID) {
              // alloc a new page
              page_id_t new_page_id;
              auto *new_page = reinterpret_cast<TablePage *>(
                  buffer_pool_manager_->NewPage(new_page_id));
              assert(new_page != nullptr);
              new_page->WLatch();
              new_page->Init(new_page_id, PAGE_SIZE, pre_page_id, nullptr, nullptr);
              new_page->WUnlatch();

              page->WLatch();
              page->SetNextPageId(new_page_id);
              page->WUnlatch();

              buffer_pool_manager_->UnpinPage(new_page_id, false);
            }
          }
          buffer_pool_manager_->UnpinPage(pre_page_id, true);
      } else if (log_record.log_record_type_ == LogRecordType::MARKDELETE) {

        RID rid = log_record.delete_rid_;
        auto page = buffer_pool_manager_->FetchPage(rid.GetPageId());
        if (page->GetLSN() >= log_record.lsn_) {
          continue;
        }
        auto *tablePage = reinterpret_cast<TablePage *>(page);
        auto result = tablePage->MarkDelete(log_record.delete_rid_, nullptr, nullptr, nullptr);
        assert(result);
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
      } else if (log_record.log_record_type_ == LogRecordType::ROLLBACKDELETE) {

        RID rid = log_record.delete_rid_;
        auto page = buffer_pool_manager_->FetchPage(rid.GetPageId());
        if (page->GetLSN() >= log_record.lsn_) {
          continue;
        }
        auto *tablePage = reinterpret_cast<TablePage *>(page);
        tablePage->RollbackDelete(log_record.delete_rid_, nullptr, nullptr);
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);          

      } else if (log_record.log_record_type_ == LogRecordType::APPLYDELETE) {

        RID rid = log_record.delete_rid_;
        auto page = buffer_pool_manager_->FetchPage(rid.GetPageId());
        if (page->GetLSN() >= log_record.lsn_) {
          continue;
        }
        auto *tablePage = reinterpret_cast<TablePage *>(page);
        tablePage->ApplyDelete(log_record.delete_rid_, nullptr, nullptr);
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);      

      } else if (log_record.log_record_type_ == LogRecordType::UPDATE) {

        RID rid = log_record.delete_rid_;
        auto page = buffer_pool_manager_->FetchPage(rid.GetPageId());
        if (page->GetLSN() >= log_record.lsn_) {
          continue;
        }
        auto *tablePage = reinterpret_cast<TablePage *>(page);
        bool res = tablePage->UpdateTuple(log_record.new_tuple_, log_record.old_tuple_,
          log_record.update_rid_, nullptr, nullptr, nullptr);
        assert(res);
        buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);            
        
      } 
      // tx is completed/failed, remove from active txn map. No redo.
      if (log_record.log_record_type_ == LogRecordType::COMMIT ||
            log_record.log_record_type_ == LogRecordType::ABORT) {
        active_txn_.erase(log_record.txn_id_);
      }
      
      lsn_mapping_[log_record.lsn_] = offset_ + buffer_offset;

      buffer_offset += log_record.size_;

      std::cout << "Buffer offset is " << buffer_offset << std::endl;
    }
    //reset buffer
    offset_ += LOG_BUFFER_SIZE;
  }

  std::cout << "End log recovery redo process.." << std::endl;
}

void LogRecovery::UndoInternal(LogRecord &log_record) {
  if (log_record.log_record_type_ == LogRecordType::INSERT) {
    std::cout << "UndoInternal for insert" << std::endl;
    RID rid = log_record.insert_rid_;
    auto page = buffer_pool_manager_->FetchPage(rid.GetPageId());
    auto *tablePage = reinterpret_cast<TablePage *>(page);
    tablePage->ApplyDelete(log_record.insert_rid_, nullptr, nullptr);
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);
  } else if (log_record.GetLogRecordType() == LogRecordType::NEWPAGE) {

  
  } else if (log_record.log_record_type_ == LogRecordType::MARKDELETE) {
    std::cout << "UndoInternal for MARKDELETE" << std::endl;
    RID rid = log_record.delete_rid_;
    auto page = buffer_pool_manager_->FetchPage(rid.GetPageId());
    auto *tablePage = reinterpret_cast<TablePage *>(page);
    tablePage->RollbackDelete(log_record.delete_rid_, nullptr, nullptr);
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);

  } else if (log_record.log_record_type_ == LogRecordType::ROLLBACKDELETE) {

    RID rid = log_record.delete_rid_;
    auto page = buffer_pool_manager_->FetchPage(rid.GetPageId());
    auto *tablePage = reinterpret_cast<TablePage *>(page);
    tablePage->MarkDelete(log_record.delete_rid_, nullptr, nullptr, nullptr);
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);          

  } else if (log_record.log_record_type_ == LogRecordType::APPLYDELETE) {
    std::cout << "UndoInternal for APPLYDELETE" << std::endl;
    RID rid = log_record.delete_rid_;
    auto page = buffer_pool_manager_->FetchPage(rid.GetPageId());
    auto *tablePage = reinterpret_cast<TablePage *>(page);
    bool res =tablePage->InsertTuple(log_record.delete_tuple_, log_record.delete_rid_, nullptr, nullptr, nullptr);
    assert(res);
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), true);      

  } else if (log_record.log_record_type_ == LogRecordType::UPDATE) {
    std::cout << "UndoInternal for UPDATE" << std::endl;
    RID rid = log_record.delete_rid_;
    auto page = buffer_pool_manager_->FetchPage(rid.GetPageId());
    auto *tablePage = reinterpret_cast<TablePage *>(page);
    bool res = tablePage->UpdateTuple(log_record.old_tuple_, log_record.new_tuple_,
      log_record.update_rid_, nullptr, nullptr, nullptr);
    assert(res);
    buffer_pool_manager_->UnpinPage(rid.GetPageId(), true); 
  } 
}

/*
 *undo phase on TABLE PAGE level(table/table_page.h)
 *iterate through active txn map and undo each operation
 */
void LogRecovery::Undo() {
  for (auto kv : active_txn_) {
    //auto txnid = kv.first;
    lsn_t lsn = kv.second;
    int offset = lsn_mapping_[lsn];

    LogRecord log_record;
    bool res = DeserializeLogRecord(log_buffer_ + offset, log_record);
    assert(res);
    UndoInternal(log_record);
    while (log_record.prev_lsn_ != INVALID_LSN) {
      // find previous and redo
      offset = lsn_mapping_[log_record.prev_lsn_];
      bool res = DeserializeLogRecord(log_buffer_ + offset, log_record);
      assert(res);
      UndoInternal(log_record);
    }
  }
  active_txn_.clear();
  lsn_mapping_.clear();
}

} // namespace cmudb
