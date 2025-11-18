#include "executors/seqscan_executor.h"

#include "common/exceptions.h"

namespace huadb {

SeqScanExecutor::SeqScanExecutor(ExecutorContext &context, std::shared_ptr<const SeqScanOperator> plan)
    : Executor(context, {}), plan_(std::move(plan)) {}

void SeqScanExecutor::Init() {
  auto table = context_.GetCatalog().GetTable(plan_->GetTableOid());
  scan_ = std::make_unique<TableScan>(context_.GetBufferPool(), table, Rid{table->GetFirstPageId(), 0});
}

std::shared_ptr<Record> SeqScanExecutor::Next() {
  std::unordered_set<xid_t> active_xids;
  // 根据隔离级别，获取活跃事务的 xid（通过 context_ 获取需要的信息）
  // 通过 context_ 获取正确的锁，加锁失败时抛出异常
  // LAB 3 BEGIN
  auto isolation_level = context_.GetIsolationLevel();
  auto &txn_mgr = context_.GetTransactionManager();
  // Serializable 下锁定读取使用当前视图，其他情况按隔离级别处理
  if (isolation_level == IsolationLevel::READ_COMMITTED) {
    active_xids = txn_mgr.GetActiveTransactions();
  } else if (isolation_level == IsolationLevel::SERIALIZABLE && plan_->HasLock()) {
    isolation_level = IsolationLevel::READ_COMMITTED;
    active_xids = txn_mgr.GetActiveTransactions();
  } else {
    active_xids = txn_mgr.GetSnapshot(context_.GetXid());
  }

  auto lock_type = (context_.IsModificationSql() || plan_->HasLock()) ? LockType::IX : LockType::IS;
  if (!context_.GetLockManager().LockTable(context_.GetXid(), lock_type, plan_->GetTableOid())) {
    throw DbException("Cannot acquire table lock");
  }

  return scan_->GetNextRecord(context_.GetXid(), isolation_level, context_.GetCid(), active_xids);
}

}  // namespace huadb
