#include "table/table_scan.h"

#include "table/table_page.h"

namespace huadb {

TableScan::TableScan(BufferPool &buffer_pool, std::shared_ptr<Table> table, Rid rid)
    : buffer_pool_(buffer_pool), table_(std::move(table)), rid_(rid) {}

std::shared_ptr<Record> TableScan::GetNextRecord(xid_t xid, IsolationLevel isolation_level, cid_t cid,
                                                 const std::unordered_set<xid_t> &active_xids) {
  // 根据事务隔离级别及活跃事务集合，判断记录是否可见
  // LAB 3 BEGIN
  while (true) {
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
      continue;
    }

    auto record = table_page->GetRecord(rid_, table_->GetColumnList());
    record->SetRid(rid_);

    // 计算下一个 rid
    if (rid_.slot_id_ + 1 < table_page->GetRecordCount()) {
      rid_.slot_id_++;
    } else {
      rid_.slot_id_ = 0;
      rid_.page_id_ = table_page->GetNextPageId();
    }

    // 兼容 Lab1：未传入事务信息时仍然使用 deleted_ 字段
    if (xid == NULL_XID) {
      if (record->IsDeleted()) {
        continue;
      }
      return record;
    }

    // Halloween：跳过当前 SQL 新插入的记录
    if (record->GetXmin() == xid && record->GetCid() == cid) {
      continue;
    }

    auto xmin = record->GetXmin();
    auto xmax = record->GetXmax();

    auto snapshot_rule = [&](xid_t txid) { return txid != NULL_XID && txid < xid && active_xids.find(txid) == active_xids.end(); };
    auto current_rule = [&](xid_t txid) { return txid != NULL_XID && active_xids.find(txid) == active_xids.end(); };

    bool xmin_visible = false;
    bool xmax_invisible = false;

    if (isolation_level == IsolationLevel::READ_COMMITTED) {
      xmin_visible = (xmin == xid) || current_rule(xmin);
      if (xmax == xid) {
        xmax_invisible = false;
      } else {
        xmax_invisible = (xmax == NULL_XID) || (active_xids.find(xmax) != active_xids.end());
      }
    } else {  // REPEATABLE_READ 或 SERIALIZABLE 的快照读
      xmin_visible = (xmin == xid) || snapshot_rule(xmin);
      if (xmax == xid) {
        xmax_invisible = false;
      } else {
        xmax_invisible =
            (xmax == NULL_XID) || (xmax > xid) || (active_xids.find(xmax) != active_xids.end());
      }
    }

    if (xmin_visible && xmax_invisible) {
      return record;
    }
  }
}

}  // namespace huadb
