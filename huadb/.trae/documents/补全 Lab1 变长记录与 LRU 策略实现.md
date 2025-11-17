## 目标
- 按实验一要求补全变长记录页面组织与 LRU 缓存替换策略，确保通过 lab1 测试：`10-insert.test`、`20-delete.test`、`30-update.test`、`40-database.test`、`50-buffer_pool.test`。

## 代码改动
### 1. Table 层
- `src/table/table.cpp:21` 实现 `Table::InsertRecord`
  - 若 `first_page_id_ == NULL_PAGE_ID`：创建新页 `page_id=0`，`buffer_pool_.NewPage(db_oid_, oid_, 0)`，`TablePage::Init()`，更新 `first_page_id_`。
  - 从 `page_id=first_page_id_` 开始遍历：若 `free_space < record->GetSize()` 且存在 `next_page`，跳至下一页；否则在末页 `free_space` 不足时新建 `page_id = 当前页id + 1`，`SetNextPageId(new_pid)`，`NewPage` 并 `Init()`。
  - 在选定页面上调用 `table_page->InsertRecord(record, xid, cid)`，返回 `{page_id, slot_id}`。
- `src/table/table.cpp:43` 实现 `Table::DeleteRecord`
  - 通过 `buffer_pool_.GetPage(...)` 获取页，`TablePage(page).DeleteRecord(rid.slot_id_, xid)`。

### 2. TablePage 层
- `src/table/table_page.cpp:30` 实现 `TablePage::InsertRecord`
  - `slot_id = GetRecordCount()`；`Slot* slot = reinterpret_cast<Slot*>(page_data_ + *lower_)`。
  - `slot->size_ = record->GetSize()`；`*lower_ += sizeof(Slot)`。
  - `*upper_ -= slot->size_`；`slot->offset_ = *upper_`。
  - 将记录序列化到 `page_data_ + *upper_`；`page_->SetDirty()`；返回 `slot_id`。
- `src/table/table_page.cpp:43` 实现 `TablePage::DeleteRecord`
  - 读出记录头，置 `deleted=true`，回写记录头。
- `src/table/table_page.cpp:58` 实现 `TablePage::GetRecord`
  - 取 `slot = slots_ + rid.slot_id_`，从 `slot->offset_` 反序列化为 `Record`，设置 `rid`，返回。

### 3. TableScan 层
- `src/table/table_scan.cpp:10` 实现 `TableScan::GetNextRecord`
  - 若 `rid_.page_id_ == NULL_PAGE_ID` 返回 `nullptr`。
  - 获取当前页 `TablePage`，读取 `record = table_page->GetRecord(rid_, table_->GetColumnList())`。
  - 若 `record->IsDeleted()`：将 `rid_`推进到下一条（`slot_id_++` 或跳至下一页），继续读取直到遇到未删除记录或扫描结束。
  - 为返回的记录设置 `rid_`，推进 `rid_`到下一条后返回。
  - 推进规则：`slot_id_ + 1 < table_page->GetRecordCount()` 则 `slot_id_++`；否则跳页：`rid_.page_id_ = table_page->GetNextPageId()`，`rid_.slot_id_ = 0`，若下一页为 `NULL_PAGE_ID` 则结束。

### 4. LRU 缓存替换策略
- `src/storage/lru_buffer_strategy.h`
  - 增加成员 `size_t time[BUFFER_SIZE];` 与默认构造函数初始化为 `-1`；`#include "common/constants.h"`。
- `src/storage/lru_buffer_strategy.cpp`
  - `Access(frame_no)`：将所有非淘汰帧时间戳加一，当前帧置零。
  - `Evict()`：返回 `time[]` 最大值对应的下标。

## 关键约束与约定
- 页面分配：新增页面号按顺序递增（当前页 `+1`），空表首页号为 `0`。
- 仅按 Lab1 要求实现删除标记与读取过滤，不涉及事务/MVCC 日志逻辑。
- 不改动框架接口与文件结构，遵循现有类型与常量。

## 验证计划
- 运行 `make lab1`，确认五项测试均通过，尤其：
  - 插入超页容量时自动扩展新页，扫描能跨页读取。
  - 删除记录后扫描结果正确过滤。
  - 更新走“删后插”逻辑可正常工作。
  - LRU 在 `BUFFER_SIZE=5` 下产生预期淘汰与磁盘访问计数。

请确认以上实现方案，确认后我将据此提交具体代码修改。