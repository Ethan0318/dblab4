#include "table/table_scan.h"

#include "table/table_page.h"

namespace huadb {

TableScan::TableScan(BufferPool &buffer_pool, std::shared_ptr<Table> table, Rid rid)
    : buffer_pool_(buffer_pool), table_(std::move(table)), rid_(rid) {}

std::shared_ptr<Record> TableScan::GetNextRecord(xid_t xid, IsolationLevel isolation_level, cid_t cid,
                                                 const std::unordered_set<xid_t> &active_xids) {
  // 根据事务隔离级别及活跃事务集合，判断记录是否可见
  // LAB 3 BEGIN

  // LAB 1 BEGIN
  if (rid_.page_id_ == NULL_PAGE_ID) {
    return nullptr;
  }

  auto page = buffer_pool_.GetPage(table_->GetDbOid(), table_->GetOid(), rid_.page_id_);
  auto table_page = std::make_unique<TablePage>(page);

  if (rid_.slot_id_ >= table_page->GetRecordCount()) {
    auto next_pid = table_page->GetNextPageId();
    if (next_pid == NULL_PAGE_ID) {
      rid_.page_id_ = NULL_PAGE_ID;
      return nullptr;
    }
    rid_.page_id_ = next_pid;
    rid_.slot_id_ = 0;
    page = buffer_pool_.GetPage(table_->GetDbOid(), table_->GetOid(), rid_.page_id_);
    table_page = std::make_unique<TablePage>(page);
  }

  auto record = table_page->GetRecord(rid_, table_->GetColumnList());
  if (record->IsDeleted()) {
    if (rid_.slot_id_ + 1 < table_page->GetRecordCount()) {
      rid_.slot_id_++;
    } else {
      rid_.slot_id_ = 0;
      rid_.page_id_ = table_page->GetNextPageId();
    }
    return GetNextRecord(xid, isolation_level, cid, active_xids);
  }

  record->SetRid(rid_);

  if (rid_.slot_id_ + 1 < table_page->GetRecordCount()) {
    rid_.slot_id_++;
  } else {
    rid_.slot_id_ = 0;
    rid_.page_id_ = table_page->GetNextPageId();
  }
  return record;
}

}  // namespace huadb
