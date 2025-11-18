#include "executors/merge_join_executor.h"

namespace huadb {

MergeJoinExecutor::MergeJoinExecutor(ExecutorContext &context, std::shared_ptr<const MergeJoinOperator> plan,
                                     std::shared_ptr<Executor> left, std::shared_ptr<Executor> right)
    : Executor(context, {std::move(left), std::move(right)}), plan_(std::move(plan)) {}

void MergeJoinExecutor::Init() {
  children_[0]->Init();
  children_[1]->Init();
  left_record_ = nullptr;
  right_record_ = nullptr;
  left_group_.clear();
  right_group_.clear();
  left_idx_ = right_idx_ = 0;
  initialized_ = false;
  group_ready_ = false;
}

std::shared_ptr<Record> MergeJoinExecutor::Next() {
  if (!initialized_) {
    left_record_ = children_[0]->Next();
    right_record_ = children_[1]->Next();
    initialized_ = true;
  }

  while (true) {
    // Output cached cross product for duplicated keys
    if (group_ready_ && left_idx_ < left_group_.size()) {
      auto result = std::make_shared<Record>(*left_group_[left_idx_]);
      result->Append(*right_group_[right_idx_]);

      right_idx_++;
      if (right_idx_ >= right_group_.size()) {
        right_idx_ = 0;
        left_idx_++;
      }
      if (left_idx_ >= left_group_.size()) {
        group_ready_ = false;
        left_group_.clear();
        right_group_.clear();
      }
      return result;
    }

    if (!left_record_ || !right_record_) {
      return nullptr;
    }

    auto left_key = plan_->left_key_->Evaluate(left_record_);
    auto right_key = plan_->right_key_->Evaluate(right_record_);

    if (left_key.Less(right_key)) {
      left_record_ = children_[0]->Next();
      continue;
    }
    if (right_key.Less(left_key)) {
      right_record_ = children_[1]->Next();
      continue;
    }

    // left_key == right_key: collect duplicate ranges
    left_group_.clear();
    right_group_.clear();
    auto current_key = left_key;
    do {
      left_group_.push_back(left_record_);
      left_record_ = children_[0]->Next();
    } while (left_record_ && plan_->left_key_->Evaluate(left_record_).Equal(current_key));

    do {
      right_group_.push_back(right_record_);
      right_record_ = children_[1]->Next();
    } while (right_record_ && plan_->right_key_->Evaluate(right_record_).Equal(current_key));

    left_idx_ = 0;
    right_idx_ = 0;
    group_ready_ = true;
    // Loop again to output the first combination from the buffered groups.
  }
}

}  // namespace huadb
