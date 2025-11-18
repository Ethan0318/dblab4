#include "transaction/lock_manager.h"

namespace huadb {

bool LockManager::LockTable(xid_t xid, LockType lock_type, oid_t oid) {
  // 对数据表加锁，成功加锁返回 true，如果数据表已被其他事务加锁，且锁的类型不相容，返回 false
  // 如果本事务已经持有该数据表的锁，根据需要升级锁的类型
  // LAB 3 BEGIN
  std::string key = "T" + std::to_string(oid);
  auto &holders = lock_table_[key];

  // 处理锁升级
  LockType new_type = lock_type;
  if (holders.find(xid) != holders.end()) {
    new_type = Upgrade(holders[xid], lock_type);
  }

  // 与其他事务的锁检测兼容性
  for (const auto &[other_xid, other_type] : holders) {
    if (other_xid == xid) {
      continue;
    }
    if (!Compatible(other_type, new_type) || !Compatible(new_type, other_type)) {
      return false;
    }
  }

  holders[xid] = new_type;
  txn_locks_[xid].insert(key);
  return true;
}

bool LockManager::LockRow(xid_t xid, LockType lock_type, oid_t oid, Rid rid) {
  // 对数据行加锁，成功加锁返回 true，如果数据行已被其他事务加锁，且锁的类型不相容，返回 false
  // 如果本事务已经持有该数据行的锁，根据需要升级锁的类型
  // LAB 3 BEGIN
  std::string key = "R" + std::to_string(oid) + "_" + std::to_string(rid.page_id_) + "_" + std::to_string(rid.slot_id_);
  auto &holders = lock_table_[key];

  LockType new_type = lock_type;
  if (holders.find(xid) != holders.end()) {
    new_type = Upgrade(holders[xid], lock_type);
  }

  for (const auto &[other_xid, other_type] : holders) {
    if (other_xid == xid) {
      continue;
    }
    if (!Compatible(other_type, new_type) || !Compatible(new_type, other_type)) {
      return false;
    }
  }

  holders[xid] = new_type;
  txn_locks_[xid].insert(key);
  return true;
}

void LockManager::ReleaseLocks(xid_t xid) {
  // 释放事务 xid 持有的所有锁
  // LAB 3 BEGIN
  if (txn_locks_.find(xid) == txn_locks_.end()) {
    return;
  }
  for (const auto &key : txn_locks_[xid]) {
    auto it = lock_table_.find(key);
    if (it == lock_table_.end()) {
      continue;
    }
    it->second.erase(xid);
    if (it->second.empty()) {
      lock_table_.erase(it);
    }
  }
  txn_locks_.erase(xid);
}

void LockManager::SetDeadLockType(DeadlockType deadlock_type) { deadlock_type_ = deadlock_type; }

bool LockManager::Compatible(LockType type_a, LockType type_b) const {
  // 判断锁是否相容
  // LAB 3 BEGIN
  if (type_a == LockType::IS) {
    return type_b != LockType::X;
  }
  if (type_a == LockType::IX) {
    return type_b == LockType::IS || type_b == LockType::IX;
  }
  if (type_a == LockType::S) {
    return type_b == LockType::IS || type_b == LockType::S;
  }
  if (type_a == LockType::SIX) {
    return type_b == LockType::IS;
  }
  // type_a == X
  return false;
}

LockType LockManager::Upgrade(LockType self, LockType other) const {
  // 升级锁类型
  // LAB 3 BEGIN
  if (self == other) {
    return self;
  }
  // IS 与 IX -> IX
  if ((self == LockType::IS && other == LockType::IX) || (self == LockType::IX && other == LockType::IS)) {
    return LockType::IX;
  }
  // IS 与 S -> S
  if ((self == LockType::IS && other == LockType::S) || (self == LockType::S && other == LockType::IS)) {
    return LockType::S;
  }
  // IX 与 S -> SIX
  if ((self == LockType::IX && other == LockType::S) || (self == LockType::S && other == LockType::IX)) {
    return LockType::SIX;
  }
  // 任何锁与 X -> X
  if (other == LockType::X || self == LockType::X) {
    return LockType::X;
  }
  // SIX 与其他锁，保持 SIX
  if (self == LockType::SIX || other == LockType::SIX) {
    return LockType::SIX;
  }
  return other;
}

}  // namespace huadb
