#pragma once

#include "executors/executor.h"
#include "operators/limit_operator.h"

namespace huadb {

class LimitExecutor : public Executor {
 public:
  LimitExecutor(ExecutorContext &context, std::shared_ptr<const LimitOperator> plan, std::shared_ptr<Executor> child);
  void Init() override;
  std::shared_ptr<Record> Next() override;

 private:
  std::shared_ptr<const LimitOperator> plan_;
  // Number of tuples already produced.
  size_t count_ = 0;
  // Whether offset tuples have been skipped.
  bool initialized_ = false;
};

}  // namespace huadb
