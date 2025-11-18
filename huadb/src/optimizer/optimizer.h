#pragma once

#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "catalog/catalog.h"
#include "operators/expressions/column_value.h"
#include "operators/expressions/comparison.h"
#include "operators/expressions/const.h"
#include "operators/expressions/logic.h"
#include "operators/operator.h"
#include "operators/seqscan_operator.h"

namespace huadb {

enum class JoinOrderAlgorithm { NONE, DP, GREEDY };
static constexpr JoinOrderAlgorithm DEFAULT_JOIN_ORDER_ALGORITHM = JoinOrderAlgorithm::NONE;

class Optimizer {
 public:
  Optimizer(Catalog &catalog, JoinOrderAlgorithm join_order_algorithm, bool enable_projection_pushdown);
  std::shared_ptr<Operator> Optimize(std::shared_ptr<Operator> plan);

 private:
  std::shared_ptr<Operator> SplitPredicates(std::shared_ptr<Operator> plan);
  std::shared_ptr<Operator> PushDown(std::shared_ptr<Operator> plan);
  std::shared_ptr<Operator> PushDownFilter(std::shared_ptr<Operator> plan);
  std::shared_ptr<Operator> PushDownProjection(std::shared_ptr<Operator> plan);
  std::shared_ptr<Operator> PushDownJoin(std::shared_ptr<Operator> plan);
  std::shared_ptr<Operator> PushDownSeqScan(std::shared_ptr<Operator> plan);

  std::shared_ptr<Operator> ReorderJoin(std::shared_ptr<Operator> plan);

  void CollectPredicates(const std::shared_ptr<OperatorExpression> &expr,
                         std::vector<std::shared_ptr<OperatorExpression>> &predicates);
  std::shared_ptr<ColumnValue> MakeColumnValue(const std::shared_ptr<Operator> &plan, const std::string &name,
                                               bool is_left);
  std::shared_ptr<SeqScanOperator> FindSeqScan(const std::shared_ptr<Operator> &plan);
  std::shared_ptr<ColumnList> CombineColumnList(const Operator &left, const Operator &right);
  bool PushPredicateToScan(std::shared_ptr<Operator> &plan, const std::shared_ptr<Comparison> &cmp);
  bool PushPredicateToJoin(std::shared_ptr<Operator> &plan, const std::shared_ptr<Comparison> &cmp);

  JoinOrderAlgorithm join_order_algorithm_;
  bool enable_projection_pushdown_;
  Catalog &catalog_;
};

}  // namespace huadb
