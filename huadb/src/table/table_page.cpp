#include "table/table_page.h"

#include <sstream>

namespace huadb {

TablePage::TablePage(std::shared_ptr<Page> page) : page_(page) {
  page_data_ = page->GetData();
  db_size_t offset = 0;
  page_lsn_ = reinterpret_cast<lsn_t *>(page_data_);
  offset += sizeof(lsn_t);
  next_page_id_ = reinterpret_cast<pageid_t *>(page_data_ + offset);
  offset += sizeof(pageid_t);
  lower_ = reinterpret_cast<db_size_t *>(page_data_ + offset);
  offset += sizeof(db_size_t);
  upper_ = reinterpret_cast<db_size_t *>(page_data_ + offset);
  offset += sizeof(db_size_t);
  assert(offset == PAGE_HEADER_SIZE);
  slots_ = reinterpret_cast<Slot *>(page_data_ + PAGE_HEADER_SIZE);
}

void TablePage::Init() {
  *page_lsn_ = 0;
  *next_page_id_ = NULL_PAGE_ID;
  *lower_ = PAGE_HEADER_SIZE;
  *upper_ = DB_PAGE_SIZE;
  page_->SetDirty();
}

slotid_t TablePage::InsertRecord(std::shared_ptr<Record> record, xid_t xid, cid_t cid) {
  // LAB 1 BEGIN
  slotid_t slot_id = GetRecordCount();
  Slot *slot = reinterpret_cast<Slot *>(page_data_ + *lower_);
  slot->size_ = record->GetSize();
  *lower_ += sizeof(Slot);
  *upper_ -= slot->size_;
  slot->offset_ = *upper_;
  char *dest = reinterpret_cast<char *>(page_data_ + *upper_);
  record->SerializeTo(dest);
  page_->SetDirty();
  return slot_id;
}

void TablePage::DeleteRecord(slotid_t slot_id, xid_t xid) {
  // LAB 1 BEGIN
  Slot *slot = slots_ + slot_id;
  char *data = reinterpret_cast<char *>(page_data_ + slot->offset_);
  Record r;
  r.DeserializeHeaderFrom(data);
  r.SetDeleted(true);
  r.SerializeHeaderTo(data);
  page_->SetDirty();
}

void TablePage::UpdateRecordInPlace(const Record &record, slotid_t slot_id) {
  record.SerializeTo(page_data_ + slots_[slot_id].offset_);
  page_->SetDirty();
}

std::shared_ptr<Record> TablePage::GetRecord(Rid rid, const ColumnList &column_list) {
  // LAB 1 BEGIN
  Slot *slot = slots_ + rid.slot_id_;
  char *src = reinterpret_cast<char *>(page_data_ + slot->offset_);
  auto record = std::make_shared<Record>();
  record->DeserializeFrom(src, column_list);
  record->SetRid(rid);
  return record;
}

void TablePage::UndoDeleteRecord(slotid_t slot_id) {
  // LAB 2 BEGIN
  Slot *slot = slots_ + slot_id;
  char *data = reinterpret_cast<char *>(page_data_ + slot->offset_);
  Record r;
  r.DeserializeHeaderFrom(data);
  r.SetDeleted(false);
  r.SerializeHeaderTo(data);
  page_->SetDirty();
}

void TablePage::RedoInsertRecord(slotid_t slot_id, char *raw_record, db_size_t page_offset, db_size_t record_size) {
  // LAB 2 BEGIN
  // 写入记录内容
  memcpy(page_data_ + page_offset, raw_record, record_size);
  // 设置槽信息
  Slot *slot = reinterpret_cast<Slot *>(page_data_ + PAGE_HEADER_SIZE) + slot_id;
  slot->offset_ = page_offset;
  slot->size_ = record_size;
  // 维护 lower 与 upper
  db_size_t expected_lower = PAGE_HEADER_SIZE + (slot_id + 1) * sizeof(Slot);
  if (*lower_ < expected_lower) {
    *lower_ = expected_lower;
  }
  if (*upper_ > page_offset) {
    *upper_ = page_offset;
  }
  page_->SetDirty();
}

db_size_t TablePage::GetRecordCount() const { return (*lower_ - PAGE_HEADER_SIZE) / sizeof(Slot); }

lsn_t TablePage::GetPageLSN() const { return *page_lsn_; }

pageid_t TablePage::GetNextPageId() const { return *next_page_id_; }

db_size_t TablePage::GetLower() const { return *lower_; }

db_size_t TablePage::GetUpper() const { return *upper_; }

db_size_t TablePage::GetFreeSpaceSize() const {
  if (*upper_ < *lower_ + sizeof(Slot)) {
    return 0;
  } else {
    return *upper_ - *lower_ - sizeof(Slot);
  }
}

void TablePage::SetNextPageId(pageid_t page_id) {
  *next_page_id_ = page_id;
  page_->SetDirty();
}

void TablePage::SetPageLSN(lsn_t page_lsn) {
  *page_lsn_ = page_lsn;
  page_->SetDirty();
}

std::string TablePage::ToString() const {
  std::ostringstream oss;
  oss << "TablePage[" << std::endl;
  oss << "  page_lsn: " << *page_lsn_ << std::endl;
  oss << "  next_page_id: " << *next_page_id_ << std::endl;
  oss << "  lower: " << *lower_ << std::endl;
  oss << "  upper: " << *upper_ << std::endl;
  if (*lower_ > *upper_) {
    oss << "\n***Error: lower > upper***" << std::endl;
  }
  oss << "  slots: " << std::endl;
  for (size_t i = 0; i < GetRecordCount(); i++) {
    oss << "    " << i << ": offset " << slots_[i].offset_ << ", size " << slots_[i].size_ << " ";
    if (slots_[i].size_ <= RECORD_HEADER_SIZE) {
      oss << "***Error: record size smaller than header size***" << std::endl;
    } else if (slots_[i].offset_ + RECORD_HEADER_SIZE >= DB_PAGE_SIZE) {
      oss << "***Error: record offset out of page boundary***" << std::endl;
    } else {
      RecordHeader header;
      header.DeserializeFrom(page_data_ + slots_[i].offset_);
      oss << header.ToString() << std::endl;
    }
  }
  oss << "]\n";
  return oss.str();
}

}  // namespace huadb
