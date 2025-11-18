#include "executors/orderby_executor.h"

#include <algorithm>

namespace huadb {

OrderByExecutor::OrderByExecutor(ExecutorContext &context, std::shared_ptr<const OrderByOperator> plan,
                                 std::shared_ptr<Executor> child)
    : Executor(context, {std::move(child)}), plan_(std::move(plan)) {}

void OrderByExecutor::Init() {
  children_[0]->Init();
  sorted_records_.clear();
  cursor_ = 0;
  sorted_ = false;
}

std::shared_ptr<Record> OrderByExecutor::Next() {
  if (!sorted_) {
    while (auto record = children_[0]->Next()) {
      sorted_records_.push_back(record);
    }
    std::sort(sorted_records_.begin(), sorted_records_.end(),
              [&](const std::shared_ptr<Record> &a, const std::shared_ptr<Record> &b) {
                for (const auto &order_by : plan_->order_bys_) {
                  auto lhs = order_by.second->Evaluate(a);
                  auto rhs = order_by.second->Evaluate(b);
                  if (lhs.Equal(rhs)) {
                    continue;
                  }
                  const bool asc = order_by.first != OrderByType::DESC;
                  return asc ? lhs.Less(rhs) : lhs.Greater(rhs);
                }
                return false;
              });
    sorted_ = true;
  }

  if (cursor_ >= sorted_records_.size()) {
    return nullptr;
  }
  return sorted_records_[cursor_++];
}

}  // namespace huadb
