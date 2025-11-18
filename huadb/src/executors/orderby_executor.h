#pragma once

#include <vector>

#include "executors/executor.h"
#include "operators/orderby_operator.h"

namespace huadb {

class OrderByExecutor : public Executor {
 public:
  OrderByExecutor(ExecutorContext &context, std::shared_ptr<const OrderByOperator> plan,
                  std::shared_ptr<Executor> child);
  void Init() override;
  std::shared_ptr<Record> Next() override;

 private:
  std::shared_ptr<const OrderByOperator> plan_;
  // Materialized and sorted records.
  std::vector<std::shared_ptr<Record>> sorted_records_;
  // Current output cursor inside sorted_records_.
  size_t cursor_ = 0;
  // Whether sorted_records_ has been populated.
  bool sorted_ = false;
};

}  // namespace huadb
