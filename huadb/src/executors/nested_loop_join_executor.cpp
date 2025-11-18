#include "executors/nested_loop_join_executor.h"

namespace huadb {

NestedLoopJoinExecutor::NestedLoopJoinExecutor(ExecutorContext &context,
                                               std::shared_ptr<const NestedLoopJoinOperator> plan,
                                               std::shared_ptr<Executor> left, std::shared_ptr<Executor> right)
    : Executor(context, {std::move(left), std::move(right)}), plan_(std::move(plan)) {}

void NestedLoopJoinExecutor::Init() {
  children_[0]->Init();
  children_[1]->Init();
  left_record_ = nullptr;
}

std::shared_ptr<Record> NestedLoopJoinExecutor::Next() {
  while (true) {
    if (!left_record_) {
      left_record_ = children_[0]->Next();
      if (!left_record_) {
        return nullptr;
      }
      // restart right iterator for the new left tuple
      children_[1]->Init();
    }

    while (auto right_record = children_[1]->Next()) {
      bool matched = true;
      if (plan_->join_condition_) {
        auto cond = plan_->join_condition_->EvaluateJoin(left_record_, right_record);
        matched = !cond.IsNull() && cond.GetValue<bool>();
      }
      if (matched) {
        auto result = std::make_shared<Record>(*left_record_);
        result->Append(*right_record);
        return result;
      }
    }

    // No match for current left tuple, move to next.
    left_record_ = nullptr;
  }
}

}  // namespace huadb
