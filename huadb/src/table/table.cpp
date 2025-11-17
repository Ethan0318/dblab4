#include "table/table.h"

#include "table/table_page.h"

namespace huadb {

Table::Table(BufferPool &buffer_pool, LogManager &log_manager, oid_t oid, oid_t db_oid, ColumnList column_list,
             bool new_table, bool is_empty)
    : buffer_pool_(buffer_pool),
      log_manager_(log_manager),
      oid_(oid),
      db_oid_(db_oid),
      column_list_(std::move(column_list)) {
  if (new_table || is_empty) {
    first_page_id_ = NULL_PAGE_ID;
  } else {
    first_page_id_ = 0;
  }
}

Rid Table::InsertRecord(std::shared_ptr<Record> record, xid_t xid, cid_t cid, bool write_log) {
  if (record->GetSize() > MAX_RECORD_SIZE) {
    throw DbException("Record size too large: " + std::to_string(record->GetSize()));
  }

  // 当 write_log 参数为 true 时开启写日志功能
  // 在插入记录时增加写 InsertLog 过程
  // 在创建新的页面时增加写 NewPageLog 过程
  // 设置页面的 page lsn
  // LAB 2 BEGIN

  // LAB 1 BEGIN
  pageid_t page_id;
  std::shared_ptr<Page> page;
  std::unique_ptr<TablePage> table_page;

  if (first_page_id_ == NULL_PAGE_ID) {
    page_id = 0;
    auto new_page = buffer_pool_.NewPage(db_oid_, oid_, page_id);
    auto new_table_page = std::make_unique<TablePage>(new_page);
    new_table_page->Init();
    first_page_id_ = page_id;
    table_page = std::move(new_table_page);
  } else {
    page_id = first_page_id_;
    page = buffer_pool_.GetPage(db_oid_, oid_, page_id);
    table_page = std::make_unique<TablePage>(page);
    while (table_page->GetFreeSpaceSize() < record->GetSize() && table_page->GetNextPageId() != NULL_PAGE_ID) {
      page_id = table_page->GetNextPageId();
      page = buffer_pool_.GetPage(db_oid_, oid_, page_id);
      table_page = std::make_unique<TablePage>(page);
    }
    if (table_page->GetFreeSpaceSize() < record->GetSize()) {
      pageid_t new_pid = page_id + 1;
      table_page->SetNextPageId(new_pid);
      auto new_page = buffer_pool_.NewPage(db_oid_, oid_, new_pid);
      auto new_table_page = std::make_unique<TablePage>(new_page);
      new_table_page->Init();
      table_page = std::move(new_table_page);
      page_id = new_pid;
    }
  }

  slotid_t slot_id = table_page->InsertRecord(record, xid, cid);
  return {page_id, slot_id};
}

void Table::DeleteRecord(const Rid &rid, xid_t xid, bool write_log) {
  // 增加写 DeleteLog 过程
  // 设置页面的 page lsn
  // LAB 2 BEGIN

  // 使用 TablePage 操作页面
  // LAB 1 BEGIN
  auto page = buffer_pool_.GetPage(db_oid_, oid_, rid.page_id_);
  auto table_page = std::make_unique<TablePage>(page);
  table_page->DeleteRecord(rid.slot_id_, xid);
}

Rid Table::UpdateRecord(const Rid &rid, xid_t xid, cid_t cid, std::shared_ptr<Record> record, bool write_log) {
  DeleteRecord(rid, xid, write_log);
  return InsertRecord(record, xid, cid, write_log);
}

void Table::UpdateRecordInPlace(const Record &record) {
  auto rid = record.GetRid();
  auto table_page = std::make_unique<TablePage>(buffer_pool_.GetPage(db_oid_, oid_, rid.page_id_));
  table_page->UpdateRecordInPlace(record, rid.slot_id_);
}

pageid_t Table::GetFirstPageId() const { return first_page_id_; }

oid_t Table::GetOid() const { return oid_; }

oid_t Table::GetDbOid() const { return db_oid_; }

const ColumnList &Table::GetColumnList() const { return column_list_; }

}  // namespace huadb
