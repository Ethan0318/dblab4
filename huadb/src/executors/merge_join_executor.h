#pragma once

#include <vector>

#include "executors/executor.h"
#include "operators/merge_join_operator.h"

namespace huadb {

class MergeJoinExecutor : public Executor {
 public:
  MergeJoinExecutor(ExecutorContext &context, std::shared_ptr<const MergeJoinOperator> plan,
                    std::shared_ptr<Executor> left, std::shared_ptr<Executor> right);
  void Init() override;
  std::shared_ptr<Record> Next() override;

 private:
  std::shared_ptr<const MergeJoinOperator> plan_;
  // Current cursors on left and right inputs.
  std::shared_ptr<Record> left_record_;
  std::shared_ptr<Record> right_record_;
  // Buffers for duplicate-key groups on both sides.
  std::vector<std::shared_ptr<Record>> left_group_;
  std::vector<std::shared_ptr<Record>> right_group_;
  size_t left_idx_ = 0;
  size_t right_idx_ = 0;
  bool initialized_ = false;
  bool group_ready_ = false;
};

}  // namespace huadb
