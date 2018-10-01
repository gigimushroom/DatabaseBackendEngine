/**
 * lock_manager.cpp
 */

#include "concurrency/lock_manager.h"
#include "common/logger.h"

namespace cmudb {

bool LockManager::LockShared(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(mutex_);

  LockRequest& req = reqByRIDsMap_[rid];
  txn_id_t txnId = txn->GetTransactionId();

  if (req.granted_ids.empty()) {
    // Grant it    
    req.granted_ids.insert(txnId);
    req.oldest_id_ = txnId;
    LOG_INFO("LockShared granted for txn id %d, rid: %s", txnId, rid.ToString().c_str());
    return true;
  } else {
    if (req.lock_state_ == SHARED) {
      req.granted_ids.insert(txnId);
      LOG_INFO("LockShared granted for txn id %d, rid: %s", txnId, rid.ToString().c_str());
      return true;
    } else {
      
      // abort if tx id ls younger than current one. Number bigger means younger
      txn_id_t curOwnerId = *req.granted_ids.begin();
      if (curOwnerId < txnId) {
        LOG_INFO("Kill young tx. LockShared not granted for txn id %d, rid: %s", txnId, rid.ToString().c_str());
        txn->SetState(TransactionState::ABORTED);
        return false;
      }

      // block if there is exclusive lock already
      auto d = *req.granted_ids.begin();
      LOG_INFO("LockShared not granted for txn id %d, rid: %s. Due to already has exclusive lock from %d. Waiting...",
         txnId, rid.ToString().c_str(), d);
      
      LockRequest::WaitingItem item;
      item.lock_state_ = SHARED;
      item.txid = txnId;
      req.waiting_list_.push_back(item);

      // & means pass in this ptr ?
      cv.wait(lock, [&]{
        auto found = (req.granted_ids.find(txnId) != req.granted_ids.end());

        if (!found) {
          req.lock_state_ = SHARED;
          LOG_INFO("LockShared for txn id %d, rid: %s. Awake myself since current only shared lock.", txnId, rid.ToString().c_str());
          found = true;
        }
        return found;
      });
      LOG_INFO("After wait. LockShared granted for txn id %d, rid: %s.", txnId, rid.ToString().c_str());
      if (txnId < req.oldest_id_ || req.oldest_id_ == -1) {
        req.oldest_id_ = txnId;
      }
      req.lock_state_ = SHARED;
    }
  }  

  txn->GetSharedLockSet()->insert(rid);
  return true;
}

bool LockManager::LockExclusive(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(mutex_);

  LockRequest& req = reqByRIDsMap_[rid];
  txn_id_t txnId = txn->GetTransactionId();

  if (req.granted_ids.empty()) {
    // Grant it    
    req.granted_ids.insert(txnId);
    req.oldest_id_ = txnId;
    req.lock_state_ = EXCLUSIVE;
    LOG_INFO("LockExclusive granted for txn id %d, rid: %s", txnId, rid.ToString().c_str());
  } else {
    // abort if tx id ls younger than current one. Number bigger means younger
    txn_id_t curOwnerId = *req.granted_ids.begin();
    if (curOwnerId < txnId) {
      txn->SetState(TransactionState::ABORTED);
      return false;
    }

    if (req.granted_ids.find(txnId) != req.granted_ids.end()) {
      LOG_INFO("LockExclusive already granted for txn id %d, rid: %s", txnId, rid.ToString().c_str());
      return true;
    }

    // block if there is exclusive lock already
    LOG_INFO("LockExclusive not granted for txn id %d, rid: %s. Due to already has exclusive lock from %d. Waiting...",
        txnId, rid.ToString().c_str(), *req.granted_ids.begin());    
    LockRequest::WaitingItem item;
    item.lock_state_ = EXCLUSIVE;
    item.txid = txnId;
    req.waiting_list_.push_back(item);

    // & means pass in this ptr ?
    cv.wait(lock, [&]{
      bool found = (req.granted_ids.find(txnId) != req.granted_ids.end());
      return found;
    });

    LOG_INFO("LockExclusive for txn id %d, rid: %s. After wait", txnId, rid.ToString().c_str());
    req.oldest_id_ = txnId;
    req.lock_state_ = EXCLUSIVE;        
  }  

  txn->GetExclusiveLockSet()->insert(rid);
  return true;
}

bool LockManager::LockUpgrade(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(mutex_);

  if (txn->GetState() == TransactionState::ABORTED) {
    return false;
  }

  LockRequest& req = reqByRIDsMap_[rid];
  txn_id_t txnId = txn->GetTransactionId();

  if (req.granted_ids.find(txnId) != req.granted_ids.end()) {
    if (req.granted_ids.size() == 1) {
      // only me, upgrade successfully
      req.oldest_id_ = txnId;
      req.lock_state_ = EXCLUSIVE;
      LOG_INFO("LockUpgrade for txn id %d, rid: %s. Upgraded, we the current only one.", txnId, rid.ToString().c_str());
    } else {
      // we remove ourself, and push to the front of the waiting list
      req.granted_ids.erase(txnId);
      LockRequest::WaitingItem item;
      item.lock_state_ = EXCLUSIVE;
      item.txid = txnId;
      req.waiting_list_.push_front(item);
      LOG_INFO("LockUpgrade for txn id %d, rid: %s. Removed ourself from current. Added to waiting queue first node", txnId, rid.ToString().c_str());
      cv.wait(lock, [&]{
        bool found = (req.granted_ids.find(txnId) != req.granted_ids.end());
        return found;
      });

      if (txn->GetState() == TransactionState::ABORTED) {
        // cleanup
        return false;
      }

      LOG_INFO("LockUpgrade for txn id %d, rid: %s. Upgraded after wait.", txnId, rid.ToString().c_str());
      req.oldest_id_ = txnId;// we are the only one
      req.lock_state_ = EXCLUSIVE; 
    }     
  } else {    
    for (auto item : req.waiting_list_) {
      if (item.txid == txnId) {
        item.lock_state_ = EXCLUSIVE;
        LOG_INFO("LockUpgrade for txn id %d, rid: %s. Modified state, and leave", txnId, rid.ToString().c_str());
      }
    } 
  }  

  txn->GetSharedLockSet()->erase(rid);
  txn->GetExclusiveLockSet()->insert(rid);
  return true;
}

bool LockManager::Unlock(Transaction *txn, const RID &rid) {
  std::unique_lock<std::mutex> lock(mutex_);
  
  // If use strict 2PL, we only unlock if tx is completed.
  if (strict_2PL_) {
    if (txn->GetState() != TransactionState::ABORTED 
      || txn->GetState() != TransactionState::COMMITTED) {
        return false;
      }
  }

  if (txn->GetState() != TransactionState::ABORTED) {
    txn->SetState(TransactionState::SHRINKING);
  }  

  LockRequest& req = reqByRIDsMap_[rid];
  if (req.granted_ids.empty()) {
    return true;
  } 

  txn_id_t txnId = txn->GetTransactionId();
  if (req.granted_ids.find(txnId) != req.granted_ids.end()) {
    req.granted_ids.erase(txnId);
    req.oldest_id_ = -1;

    if (req.lock_state_ == SHARED) {
      txn->GetSharedLockSet()->erase(rid);
    } else {
      txn->GetExclusiveLockSet()->erase(rid);
    }
    
    // notify
    LOG_INFO("Unlock granted for txn id %d, rid: %s", txnId, rid.ToString().c_str());
    if (!req.waiting_list_.empty()) {
      LockRequest::WaitingItem next = req.waiting_list_.front();
      req.lock_state_ = next.lock_state_;
      req.granted_ids.insert(next.txid);
      req.waiting_list_.pop_front();
      req.oldest_id_ = next.txid;
    }
    cv.notify_all();
  }
  return true;
}

} // namespace cmudb
