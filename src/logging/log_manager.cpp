/**
 * log_manager.cpp
 */

#include "logging/log_manager.h"

namespace cmudb {

void LogManager::SwapBuffer() {
  char *tmp = nullptr;
  tmp = flush_buffer_;
  flush_buffer_ = log_buffer_;
  log_buffer_ = tmp;
  flush_size_ = log_buf_offset_;
  log_buf_offset_ = 0;
}

void LogManager::task1() {
  
  while (ENABLE_LOGGING) {
      std::cout << "Thread another loop\n";
      std::unique_lock<std::mutex> lk(latch_);
      auto now = std::chrono::system_clock::now();
      if(cv_.wait_until(lk, now + LOG_TIMEOUT, [](){ return false; })) {
        // buffer full or buffer manager signal
        std::cout << "Thread was wakenup to write log\n";
        disk_manager_->WriteLog(flush_buffer_, flush_size_);
        flush_size_ = 0; 
      }
      else {
        // time out, flush
        SwapBuffer();
        std::cout << "Thread timed out. log size is: " << flush_size_ << std::endl;
        disk_manager_->WriteLog(flush_buffer_, flush_size_);
        flush_size_ = 0;        
      }          
  }
}

/*
 * set ENABLE_LOGGING = true
 * Start a separate thread to execute flush to disk operation periodically
 * The flush can be triggered when the log buffer is full or buffer pool
 * manager wants to force flush (it only happens when the flushed page has a
 * larger LSN than persistent LSN)
 */
void LogManager::RunFlushThread() {

  ENABLE_LOGGING = true;
  flush_thread_ = new std::thread(&LogManager::task1, this);

}
/*
 * Stop and join the flush thread, set ENABLE_LOGGING = false
 */
void LogManager::StopFlushThread() {

  ENABLE_LOGGING = false;
  flush_thread_->join();
}

/*
 * append a log record into log buffer
 * you MUST set the log record's lsn within this method
 * @return: lsn that is assigned to this log record
 *
 *
 * example below
 * // First, serialize the must have fields(20 bytes in total)
 * log_record.lsn_ = next_lsn_++;
 * memcpy(log_buffer_ + offset_, &log_record, 20);
 * int pos = offset_ + 20;
 *
 * if (log_record.log_record_type_ == LogRecordType::INSERT) {
 *    memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
 *    pos += sizeof(RID);
 *    // we have provided serialize function for tuple class
 *    log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
 *  }
 *
 */
lsn_t LogManager::AppendLogRecord(LogRecord &log_record) {
  std::lock_guard<std::mutex> lock(latch_);

  if (log_buf_offset_ + log_record.size_ > LOG_BUFFER_SIZE) {
    SwapBuffer();
    // wake up flush thread
    cv_.notify_one();
  }

  log_record.lsn_ = next_lsn_++;

  std::cout << log_record.ToString().c_str() << std::endl;

  memcpy(log_buffer_ + log_buf_offset_, &log_record, 20);
  int pos = log_buf_offset_ + 20;

  if (log_record.log_record_type_ == LogRecordType::INSERT) {
     memcpy(log_buffer_ + pos, &log_record.insert_rid_, sizeof(RID));
     pos += sizeof(RID);
     // we have provided serialize function for tuple class
     log_record.insert_tuple_.SerializeTo(log_buffer_ + pos);
     
  } else if (log_record.log_record_type_ == LogRecordType::MARKDELETE ||
      log_record.log_record_type_ == LogRecordType::APPLYDELETE ||
      log_record.log_record_type_ == LogRecordType::ROLLBACKDELETE) {

     memcpy(log_buffer_ + pos, &log_record.delete_rid_, sizeof(RID));
     pos += sizeof(RID);
     log_record.delete_tuple_.SerializeTo(log_buffer_ + pos);
  } else if (log_record.log_record_type_ == LogRecordType::UPDATE) {
     memcpy(log_buffer_ + pos, &log_record.update_rid_, sizeof(RID));
     pos += sizeof(RID);
     
     log_record.old_tuple_.SerializeTo(log_buffer_ + pos);
     pos += log_record.old_tuple_.GetLength();

     log_record.new_tuple_.SerializeTo(log_buffer_ + pos);

  } else if (log_record.log_record_type_ == LogRecordType::NEWPAGE) {
     //prev_page_id
     memcpy(log_buffer_ + pos, &log_record.prev_page_id_, sizeof(page_id_t));
  }

  log_buf_offset_ += log_record.size_;
  
  return log_record.lsn_;
}

} // namespace cmudb
