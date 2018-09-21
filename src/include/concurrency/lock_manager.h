/**
 * lock_manager.h
 *
 * Tuple level lock manager, use wait-die to prevent deadlocks
 */

#pragma once

#include <condition_variable>
#include <list>
#include <memory>
#include <mutex>
#include <unordered_map>
#include <unordered_set>

#include "common/rid.h"
#include "concurrency/transaction.h"

namespace cmudb {

class LockManager {

public:
  enum LockState {SHARED, EXCLUSIVE};
  struct LockRequest {
    LockRequest() {}

    LockRequest(LockState state, txn_id_t id) {
      lock_state_ = state;
      granted_ids.insert(id);
      oldest_id_ = id;
    }
    LockState lock_state_ = SHARED;
    std::unordered_set<txn_id_t> granted_ids;
    int oldest_id_ = -1;

    struct WaitingItem {
      LockState lock_state_;
      int txid = -1;
    };

    std::list<WaitingItem> waiting_list_;
  };


  LockManager(bool strict_2PL) : strict_2PL_(strict_2PL){};

  /*** below are APIs need to implement ***/
  // lock:
  // return false if transaction is aborted
  // it should be blocked on waiting and should return true when granted
  // note the behavior of trying to lock locked rids by same txn is undefined
  // it is transaction's job to keep track of its current locks
  bool LockShared(Transaction *txn, const RID &rid);
  bool LockExclusive(Transaction *txn, const RID &rid);
  bool LockUpgrade(Transaction *txn, const RID &rid);

  // unlock:
  // release the lock hold by the txn
  bool Unlock(Transaction *txn, const RID &rid);
  /*** END OF APIs ***/

private:
  bool strict_2PL_;

  std::mutex mutex_;

  std::condition_variable cv;

  std::unordered_map<RID, LockRequest> reqByRIDsMap_;
};

} // namespace cmudb
