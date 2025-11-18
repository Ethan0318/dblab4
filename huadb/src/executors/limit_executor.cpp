#include "executors/limit_executor.h"

namespace huadb {

LimitExecutor::LimitExecutor(ExecutorContext &context, std::shared_ptr<const LimitOperator> plan,
                             std::shared_ptr<Executor> child)
    : Executor(context, {std::move(child)}), plan_(std::move(plan)) {}

void LimitExecutor::Init() {
  children_[0]->Init();
  count_ = 0;
  initialized_ = false;
}

std::shared_ptr<Record> LimitExecutor::Next() {
  if (!initialized_) {
    const auto offset = plan_->limit_offset_.value_or(0);
    for (uint32_t i = 0; i < offset; i++) {
      if (!children_[0]->Next()) {
        return nullptr;
      }
    }
    initialized_ = true;
  }

  if (plan_->limit_count_.has_value() && count_ >= plan_->limit_count_.value()) {
    return nullptr;
  }

  auto record = children_[0]->Next();
  if (!record) {
    return nullptr;
  }
  count_++;
  return record;
}

}  // namespace huadb
