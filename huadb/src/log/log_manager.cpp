#include "log/log_manager.h"
#include <memory>
#include "table/table_page.h"

#include "common/exceptions.h"
#include "log/log_records/log_records.h"

namespace huadb {

LogManager::LogManager(Disk &disk, TransactionManager &transaction_manager, lsn_t next_lsn)
    : disk_(disk), transaction_manager_(transaction_manager), next_lsn_(next_lsn), flushed_lsn_(next_lsn - 1) {}

void LogManager::SetBufferPool(std::shared_ptr<BufferPool> buffer_pool) { buffer_pool_ = std::move(buffer_pool); }

void LogManager::SetCatalog(std::shared_ptr<Catalog> catalog) { catalog_ = std::move(catalog); }

lsn_t LogManager::GetNextLSN() const { return next_lsn_; }

void LogManager::Clear() {
  std::unique_lock lock(log_buffer_mutex_);
  log_buffer_.clear();
}

void LogManager::Flush() { Flush(NULL_LSN); }

void LogManager::SetDirty(oid_t oid, pageid_t page_id, lsn_t lsn) {
  if (dpt_.find({oid, page_id}) == dpt_.end()) {
    dpt_[{oid, page_id}] = lsn;
  }
}

lsn_t LogManager::AppendInsertLog(xid_t xid, oid_t oid, pageid_t page_id, slotid_t slot_id, db_size_t offset,
                                  db_size_t size, char *new_record) {
  if (att_.find(xid) == att_.end()) {
    throw DbException(std::to_string(xid) + " does not exist in att (in AppendInsertLog)");
  }
  auto log = std::make_shared<InsertLog>(NULL_LSN, xid, att_.at(xid), oid, page_id, slot_id, offset, size, new_record);
  lsn_t lsn = next_lsn_.fetch_add(log->GetSize(), std::memory_order_relaxed);
  log->SetLSN(lsn);
  att_[xid] = lsn;
  {
    std::unique_lock lock(log_buffer_mutex_);
    log_buffer_.push_back(std::move(log));
  }
  if (dpt_.find({oid, page_id}) == dpt_.end()) {
    dpt_[{oid, page_id}] = lsn;
  }
  return lsn;
}

lsn_t LogManager::AppendDeleteLog(xid_t xid, oid_t oid, pageid_t page_id, slotid_t slot_id) {
  if (att_.find(xid) == att_.end()) {
    throw DbException(std::to_string(xid) + " does not exist in att (in AppendDeleteLog)");
  }
  auto log = std::make_shared<DeleteLog>(NULL_LSN, xid, att_.at(xid), oid, page_id, slot_id);
  lsn_t lsn = next_lsn_.fetch_add(log->GetSize(), std::memory_order_relaxed);
  log->SetLSN(lsn);
  att_[xid] = lsn;
  {
    std::unique_lock lock(log_buffer_mutex_);
    log_buffer_.push_back(std::move(log));
  }
  if (dpt_.find({oid, page_id}) == dpt_.end()) {
    dpt_[{oid, page_id}] = lsn;
  }
  return lsn;
}

lsn_t LogManager::AppendNewPageLog(xid_t xid, oid_t oid, pageid_t prev_page_id, pageid_t page_id) {
  if (xid != DDL_XID && att_.find(xid) == att_.end()) {
    throw DbException(std::to_string(xid) + " does not exist in att (in AppendNewPageLog)");
  }
  xid_t log_xid;
  if (xid == DDL_XID) {
    log_xid = NULL_XID;
  } else {
    log_xid = att_.at(xid);
  }
  auto log = std::make_shared<NewPageLog>(NULL_LSN, xid, log_xid, oid, prev_page_id, page_id);
  lsn_t lsn = next_lsn_.fetch_add(log->GetSize(), std::memory_order_relaxed);
  log->SetLSN(lsn);

  if (xid != DDL_XID) {
    att_[xid] = lsn;
  }
  {
    std::unique_lock lock(log_buffer_mutex_);
    log_buffer_.push_back(std::move(log));
  }
  if (dpt_.find({oid, page_id}) == dpt_.end()) {
    dpt_[{oid, page_id}] = lsn;
  }
  if (prev_page_id != NULL_PAGE_ID && dpt_.find({oid, prev_page_id}) == dpt_.end()) {
    dpt_[{oid, prev_page_id}] = lsn;
  }
  return lsn;
}

lsn_t LogManager::AppendBeginLog(xid_t xid) {
  if (att_.find(xid) != att_.end()) {
    throw DbException(std::to_string(xid) + " already exists in att");
  }
  auto log = std::make_shared<BeginLog>(NULL_LSN, xid, NULL_LSN);
  lsn_t lsn = next_lsn_.fetch_add(log->GetSize(), std::memory_order_relaxed);
  log->SetLSN(lsn);
  att_[xid] = lsn;
  {
    std::unique_lock lock(log_buffer_mutex_);
    log_buffer_.push_back(std::move(log));
  }
  return lsn;
}

lsn_t LogManager::AppendCommitLog(xid_t xid) {
  if (att_.find(xid) == att_.end()) {
    throw DbException(std::to_string(xid) + " does not exist in att (in AppendCommitLog)");
  }
  auto log = std::make_shared<CommitLog>(NULL_LSN, xid, att_.at(xid));
  lsn_t lsn = next_lsn_.fetch_add(log->GetSize(), std::memory_order_relaxed);
  log->SetLSN(lsn);
  {
    std::unique_lock lock(log_buffer_mutex_);
    log_buffer_.push_back(std::move(log));
  }
  Flush(lsn);
  att_.erase(xid);
  return lsn;
}

lsn_t LogManager::AppendRollbackLog(xid_t xid) {
  if (att_.find(xid) == att_.end()) {
    throw DbException(std::to_string(xid) + " does not exist in att (in AppendRollbackLog)");
  }
  auto log = std::make_shared<RollbackLog>(NULL_LSN, xid, att_.at(xid));
  lsn_t lsn = next_lsn_.fetch_add(log->GetSize(), std::memory_order_relaxed);
  log->SetLSN(lsn);
  {
    std::unique_lock lock(log_buffer_mutex_);
    log_buffer_.push_back(std::move(log));
  }
  Flush(lsn);
  att_.erase(xid);
  return lsn;
}

lsn_t LogManager::Checkpoint(bool async) {
  auto begin_checkpoint_log = std::make_shared<BeginCheckpointLog>(NULL_LSN, NULL_XID, NULL_LSN);
  lsn_t begin_lsn = next_lsn_.fetch_add(begin_checkpoint_log->GetSize(), std::memory_order_relaxed);
  begin_checkpoint_log->SetLSN(begin_lsn);
  {
    std::unique_lock lock(log_buffer_mutex_);
    log_buffer_.push_back(std::move(begin_checkpoint_log));
  }

  auto end_checkpoint_log = std::make_shared<EndCheckpointLog>(NULL_LSN, NULL_XID, NULL_LSN, att_, dpt_);
  lsn_t end_lsn = next_lsn_.fetch_add(end_checkpoint_log->GetSize(), std::memory_order_relaxed);
  end_checkpoint_log->SetLSN(end_lsn);
  {
    std::unique_lock lock(log_buffer_mutex_);
    log_buffer_.push_back(std::move(end_checkpoint_log));
  }
  Flush(end_lsn);
  std::ofstream out(MASTER_RECORD_NAME);
  out << begin_lsn;
  return end_lsn;
}

void LogManager::FlushPage(oid_t table_oid, pageid_t page_id, lsn_t page_lsn) {
  Flush(page_lsn);
  dpt_.erase({table_oid, page_id});
}

void LogManager::Rollback(xid_t xid) {
  // 在 att_ 中查找事务 xid 的最后一条日志的 lsn
  // 依次获取 lsn 的 prev_lsn_，直到 NULL_LSN
  // 根据 lsn 和 flushed_lsn_ 的大小关系，判断日志在 buffer 中还是在磁盘中
  // 若日志在 buffer 中，通过 log_buffer_ 获取日志
  // 若日志在磁盘中，通过 disk_ 读取日志，count 参数可设置为 MAX_LOG_SIZE
  // 通过 LogRecord::DeserializeFrom 函数解析日志
  // 调用日志的 Undo 函数
  // LAB 2 BEGIN
  auto it = att_.find(xid);
  if (it == att_.end()) {
    return;
  }
  lsn_t cur_lsn = it->second;
  while (cur_lsn != NULL_LSN) {
    std::shared_ptr<LogRecord> log_record;
    if (flushed_lsn_ != NULL_LSN && cur_lsn <= flushed_lsn_) {
      auto buf = std::make_unique<char[]>(MAX_LOG_SIZE);
      disk_.ReadLog(static_cast<uint32_t>(cur_lsn), static_cast<uint32_t>(MAX_LOG_SIZE), buf.get());
      log_record = LogRecord::DeserializeFrom(cur_lsn, buf.get());
    } else {
      std::shared_lock lock(log_buffer_mutex_);
      for (const auto &lr : log_buffer_) {
        if (lr->GetLSN() == cur_lsn) {
          log_record = lr;
          break;
        }
      }
    }
    if (!log_record) {
      break;
    }
    log_record->Undo(*buffer_pool_, *catalog_, *this, NULL_LSN);
    cur_lsn = log_record->GetPrevLSN();
  }
}

void LogManager::Recover() {
  Analyze();
  Redo();
  Undo();
}

void LogManager::IncrementRedoCount() { redo_count_++; }

uint32_t LogManager::GetRedoCount() const { return redo_count_; }

void LogManager::Flush(lsn_t lsn) {
  size_t max_log_size = 0;
  lsn_t max_lsn = NULL_LSN;
  {
    std::unique_lock lock(log_buffer_mutex_);
    for (auto iterator = log_buffer_.cbegin(); iterator != log_buffer_.cend();) {
      const auto &log_record = *iterator;
      // 如果 lsn 为 NULL_LSN，表示 log_buffer_ 中所有日志都需要刷盘
      if (lsn != NULL_LSN && log_record->GetLSN() > lsn) {
        iterator++;
        continue;
      }
      auto log_size = log_record->GetSize();
      auto log = std::make_unique<char[]>(log_size);
      log_record->SerializeTo(log.get());
      disk_.WriteLog(log_record->GetLSN(), log_size, log.get());
      if (max_lsn == NULL_LSN || log_record->GetLSN() > max_lsn) {
        max_lsn = log_record->GetLSN();
        max_log_size = log_size;
      }
      iterator = log_buffer_.erase(iterator);
    }
  }
  // 如果 max_lsn 为 NULL_LSN，表示没有日志刷盘
  // 如果 flushed_lsn_ 为 NULL_LSN，表示还没有日志刷过盘
  if (max_lsn != NULL_LSN && (flushed_lsn_ == NULL_LSN || max_lsn > flushed_lsn_)) {
    flushed_lsn_ = max_lsn;
    lsn_t next_lsn = FIRST_LSN;
    if (disk_.FileExists(NEXT_LSN_NAME)) {
      std::ifstream in(NEXT_LSN_NAME);
      in >> next_lsn;
    }
    if (flushed_lsn_ + max_log_size > next_lsn) {
      std::ofstream out(NEXT_LSN_NAME);
      out << (flushed_lsn_ + max_log_size);
    }
  }
}

void LogManager::Analyze() {
  // 恢复 Master Record 中元信息
  // 恢复下次启动时要使用的日志起始 LSN
  if (disk_.FileExists(NEXT_LSN_NAME)) {
    std::ifstream in(NEXT_LSN_NAME);
    lsn_t next_lsn;
    in >> next_lsn;
    next_lsn_ = next_lsn;
  } else {
    next_lsn_ = FIRST_LSN;
  }
  flushed_lsn_ = next_lsn_ - 1;
  lsn_t checkpoint_lsn = 0;

  if (disk_.FileExists(MASTER_RECORD_NAME)) {
    std::ifstream in(MASTER_RECORD_NAME);
    in >> checkpoint_lsn;
  }

  // 从 checkpoint_lsn 开始扫描日志，重建 ATT 和 DPT
  xid_t max_xid = FIRST_XID;
  lsn_t scan_lsn = checkpoint_lsn;
  while (scan_lsn <= flushed_lsn_) {
    auto buf = std::make_unique<char[]>(MAX_LOG_SIZE);
    disk_.ReadLog(static_cast<uint32_t>(scan_lsn), static_cast<uint32_t>(MAX_LOG_SIZE), buf.get());
    auto log = LogRecord::DeserializeFrom(scan_lsn, buf.get());
    max_xid = std::max(max_xid, log->GetXid());

    switch (log->GetType()) {
      case LogType::BEGIN:
        // 事务开始：加入 ATT，最后一条日志 LSN 先记为 BEGIN 的 LSN
        att_[log->GetXid()] = log->GetLSN();
        break;

      case LogType::COMMIT:
      case LogType::ROLLBACK:
        // 事务结束：从 ATT 中移除
        att_.erase(log->GetXid());
        break;

      case LogType::INSERT: {
        auto il = std::dynamic_pointer_cast<InsertLog>(log);
        TablePageid tpid{il->GetOid(), il->GetPageId()};
        // DPT：第一次遇到这个页时记录 rec_lsn
        if (dpt_.find(tpid) == dpt_.end()) {
          dpt_[tpid] = il->GetLSN();
        }
        // 活跃事务的最后一条日志 LSN
        if (log->GetXid() != NULL_XID && log->GetXid() != DDL_XID) {
          att_[log->GetXid()] = log->GetLSN();
        }
        break;
      }

      case LogType::DELETE: {
        auto dl = std::dynamic_pointer_cast<DeleteLog>(log);
        TablePageid tpid{dl->GetOid(), dl->GetPageId()};
        if (dpt_.find(tpid) == dpt_.end()) {
          dpt_[tpid] = dl->GetLSN();
        }
        if (log->GetXid() != NULL_XID && log->GetXid() != DDL_XID) {
          att_[log->GetXid()] = log->GetLSN();
        }
        break;
      }

      case LogType::NEW_PAGE: {
        auto nl = std::dynamic_pointer_cast<NewPageLog>(log);
        // 旧页可能也变脏
        if (nl->GetPrevPageId() != NULL_PAGE_ID) {
          TablePageid prev{nl->GetOid(), nl->GetPrevPageId()};
          if (dpt_.find(prev) == dpt_.end()) {
            dpt_[prev] = nl->GetLSN();
          }
        }
        // 新页也可能脏
        TablePageid cur{nl->GetOid(), nl->GetPageId()};
        if (dpt_.find(cur) == dpt_.end()) {
          dpt_[cur] = nl->GetLSN();
        }
        // NEW_PAGE 对普通事务也要更新 ATT；DDL_XID/NULL_XID 不算事务
        if (log->GetXid() != NULL_XID && log->GetXid() != DDL_XID) {
          att_[log->GetXid()] = log->GetLSN();
        }
        break;
      }

      default:
        break;
    }

    scan_lsn += log->GetSize();
  }

  // 恢复 TransactionManager 中的 next_xid
  transaction_manager_.SetNextXid(max_xid + 1);
}


void LogManager::Redo() {
  // ARIES Redo：从 DPT 最小 rec_lsn 开始，带三条件重做
  if (dpt_.empty()) {
    return;
  }

  // 找到所有脏页中最小的 rec_lsn
  lsn_t start_lsn = flushed_lsn_;
  for (const auto &[tp, rec_lsn] : dpt_) {
    if (rec_lsn < start_lsn) {
      start_lsn = rec_lsn;
    }
  }

  lsn_t scan_lsn = start_lsn;
  while (scan_lsn <= flushed_lsn_) {
    auto buf = std::make_unique<char[]>(MAX_LOG_SIZE);
    disk_.ReadLog(static_cast<uint32_t>(scan_lsn), static_cast<uint32_t>(MAX_LOG_SIZE), buf.get());
    auto log = LogRecord::DeserializeFrom(scan_lsn, buf.get());

    // 找出这条日志对应的“页”
    TablePageid page_key{};
    bool has_page = false;
    switch (log->GetType()) {
      case LogType::INSERT: {
        auto il = std::dynamic_pointer_cast<InsertLog>(log);
        page_key = {il->GetOid(), il->GetPageId()};
        has_page = true;
        break;
      }
      case LogType::DELETE: {
        auto dl = std::dynamic_pointer_cast<DeleteLog>(log);
        page_key = {dl->GetOid(), dl->GetPageId()};
        has_page = true;
        break;
      }
      case LogType::NEW_PAGE: {
        // NEW_PAGE 逻辑修改的是 prev_page 的 next 指针
        auto nl = std::dynamic_pointer_cast<NewPageLog>(log);
        if (nl->GetPrevPageId() != NULL_PAGE_ID) {
          page_key = {nl->GetOid(), nl->GetPrevPageId()};
          has_page = true;
        }
        break;
      }
      default:
        break;
    }

    if (has_page) {
      auto it = dpt_.find(page_key);
      // 条件 1：页不在 DPT → 跳过
      if (it != dpt_.end()) {
        // 条件 2：日志 LSN < rec_lsn → 跳过
        if (log->GetLSN() >= it->second) {
          auto db_oid = catalog_->GetDatabaseOid(page_key.table_oid_);
          std::shared_ptr<Page> page;
          try {
            page = buffer_pool_->GetPage(db_oid, page_key.table_oid_, page_key.page_id_);
          } catch (DbException &) {
            // 页文件不存在时，恢复时创建并初始化一个空页
            page = buffer_pool_->NewPage(db_oid, page_key.table_oid_, page_key.page_id_);
            auto tp = std::make_unique<TablePage>(page);
            tp->Init();
          }
          auto tp = std::make_unique<TablePage>(page);
          // 条件 3：日志 LSN <= page_lsn → 跳过
          if (log->GetLSN() > tp->GetPageLSN()) {
            log->Redo(*buffer_pool_, *catalog_, *this);
            IncrementRedoCount();
          }
        }
      }
    }

    scan_lsn += log->GetSize();
  }
}


void LogManager::Undo() {
  // 根据活跃事务表，将所有活跃事务回滚
  // LAB 2 BEGIN
  std::vector<xid_t> active;
  for (const auto &[xid, lsn] : att_) {
    active.push_back(xid);
  }
  for (auto xid : active) {
    Rollback(xid);
    att_.erase(xid);
  }
}

}  // namespace huadb
