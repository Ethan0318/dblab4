#include "optimizer/optimizer.h"

#include <algorithm>
#include <functional>
#include <limits>

#include "fmt/format.h"
#include "operators/operators.h"

namespace huadb {

Optimizer::Optimizer(Catalog &catalog, JoinOrderAlgorithm join_order_algorithm, bool enable_projection_pushdown)
    : catalog_(catalog),
      join_order_algorithm_(join_order_algorithm),
      enable_projection_pushdown_(enable_projection_pushdown) {}

std::shared_ptr<Operator> Optimizer::Optimize(std::shared_ptr<Operator> plan) {
  plan = SplitPredicates(plan);
  plan = PushDown(plan);
  plan = ReorderJoin(plan);
  return plan;
}

std::shared_ptr<Operator> Optimizer::SplitPredicates(std::shared_ptr<Operator> plan) {
  if (!plan) {
    return plan;
  }
  for (auto &child : plan->children_) {
    child = SplitPredicates(child);
  }
  if (plan->GetType() != OperatorType::FILTER) {
    return plan;
  }

  auto filter = std::dynamic_pointer_cast<FilterOperator>(plan);
  std::vector<std::shared_ptr<OperatorExpression>> predicates;
  CollectPredicates(filter->predicate_, predicates);

  auto child = filter->children_[0];
  if (predicates.size() <= 1) {
    filter->children_[0] = child;
    return plan;
  }

  // build a chain of Filter nodes, each holding a single predicate
  for (auto it = predicates.rbegin(); it != predicates.rend(); ++it) {
    child = std::make_shared<FilterOperator>(std::make_shared<ColumnList>(child->OutputColumns()), child, *it);
  }
  return child;
}

std::shared_ptr<Operator> Optimizer::PushDown(std::shared_ptr<Operator> plan) {
  switch (plan->GetType()) {
    case OperatorType::FILTER:
      return PushDownFilter(std::move(plan));
    case OperatorType::PROJECTION:
      return PushDownProjection(std::move(plan));
    case OperatorType::NESTEDLOOP:
      return PushDownJoin(std::move(plan));
    case OperatorType::SEQSCAN:
      return PushDownSeqScan(std::move(plan));
    default: {
      for (auto &child : plan->children_) {
        child = PushDown(child);
      }
      return plan;
    }
  }
}

std::shared_ptr<Operator> Optimizer::PushDownFilter(std::shared_ptr<Operator> plan) {
  auto filter = std::dynamic_pointer_cast<FilterOperator>(plan);
  filter->children_[0] = PushDown(filter->children_[0]);

  auto cmp = std::dynamic_pointer_cast<Comparison>(filter->predicate_);
  if (!cmp) {
    return plan;
  }

  bool left_is_col = cmp->children_[0]->GetExprType() == OperatorExpressionType::COLUMN_VALUE;
  bool right_is_col = cmp->children_[1]->GetExprType() == OperatorExpressionType::COLUMN_VALUE;

  bool pushed = false;
  if (left_is_col && right_is_col) {
    // join predicate
    pushed = PushPredicateToJoin(filter->children_[0], cmp);
  } else if (left_is_col || right_is_col) {
    // single-table predicate
    pushed = PushPredicateToScan(filter->children_[0], cmp);
  }

  if (pushed) {
    // predicate was pushed into subtree, remove this filter node
    return filter->children_[0];
  }
  return plan;
}

std::shared_ptr<Operator> Optimizer::PushDownProjection(std::shared_ptr<Operator> plan) {
  // Advanced projection push-down is optional; keep recursive traversal for now.
  plan->children_[0] = PushDown(plan->children_[0]);
  return plan;
}

std::shared_ptr<Operator> Optimizer::PushDownJoin(std::shared_ptr<Operator> plan) {
  for (auto &child : plan->children_) {
    child = PushDown(child);
  }
  return plan;
}

std::shared_ptr<Operator> Optimizer::PushDownSeqScan(std::shared_ptr<Operator> plan) { return plan; }

std::shared_ptr<Operator> Optimizer::ReorderJoin(std::shared_ptr<Operator> plan) {
  if (!plan) {
    return plan;
  }
  for (auto &child : plan->children_) {
    child = ReorderJoin(child);
  }
  if (join_order_algorithm_ == JoinOrderAlgorithm::NONE) {
    return plan;
  }
  if (plan->GetType() != OperatorType::NESTEDLOOP) {
    return plan;
  }

  struct JoinEdge {
    std::string left_table;
    std::string right_table;
    std::string left_column;
    std::string right_column;
    ComparisonType cmp_type;
  };

  std::vector<std::shared_ptr<Operator>> leaves;
  std::vector<JoinEdge> edges;

  // Collect join predicates and leaves from the existing left-deep join tree.
  std::function<void(std::shared_ptr<Operator>)> collect_join_tree =
      [&](std::shared_ptr<Operator> node) -> void {
    if (node->GetType() != OperatorType::NESTEDLOOP) {
      leaves.push_back(node);
      return;
    }
    auto join = std::dynamic_pointer_cast<NestedLoopJoinOperator>(node);
    std::vector<std::shared_ptr<OperatorExpression>> predicates;
    CollectPredicates(join->join_condition_, predicates);
    for (const auto &pred : predicates) {
      auto cmp = std::dynamic_pointer_cast<Comparison>(pred);
      if (!cmp) {
        continue;
      }
      if (cmp->children_[0]->GetExprType() == OperatorExpressionType::COLUMN_VALUE &&
          cmp->children_[1]->GetExprType() == OperatorExpressionType::COLUMN_VALUE) {
        auto left_col = std::dynamic_pointer_cast<ColumnValue>(cmp->children_[0]);
        auto right_col = std::dynamic_pointer_cast<ColumnValue>(cmp->children_[1]);
        auto split_name = [](const std::string &name) {
          auto pos = name.find('.');
          if (pos == std::string::npos) {
            return std::pair<std::string, std::string>{name, ""};
          }
          return std::pair<std::string, std::string>{name.substr(0, pos), name.substr(pos + 1)};
        };
        auto [ltable, lcol] = split_name(left_col->name_);
        auto [rtable, rcol] = split_name(right_col->name_);
        edges.push_back({ltable, rtable, lcol, rcol, cmp->GetComparisonType()});
      }
    }
    collect_join_tree(join->children_[0]);
    collect_join_tree(join->children_[1]);
  };

  collect_join_tree(plan);
  if (leaves.size() <= 1) {
    return plan;
  }

  // Build mapping from table alias to leaf plan and statistics.
  std::unordered_map<std::string, std::shared_ptr<Operator>> alias2plan;
  std::unordered_map<std::string, std::string> alias2base;
  std::unordered_map<std::string, uint32_t> cardinality;
  std::unordered_map<std::string, size_t> degree;
  for (const auto &edge : edges) {
    degree[edge.left_table]++;
    degree[edge.right_table]++;
  }

  for (const auto &leaf : leaves) {
    auto scan = FindSeqScan(leaf);
    if (!scan) {
      return plan;
    }
    auto alias = scan->GetTableNameOrAlias();
    auto base = scan->GetTableName();
    alias2plan[alias] = leaf;
    alias2base[alias] = base;
    cardinality[alias] = catalog_.GetCardinality(base);
  }

  for (const auto &[alias, card] : cardinality) {
    if (card == INVALID_CARDINALITY) {
      return plan;
    }
  }

  auto choose_first = [&](const std::unordered_set<std::string> &candidates) {
    std::string chosen;
    uint32_t best_card = std::numeric_limits<uint32_t>::max();
    size_t best_degree = 0;
    for (const auto &name : candidates) {
      auto card = cardinality[name];
      auto deg = degree.count(name) ? degree[name] : 0;
      if (card < best_card || (card == best_card && deg > best_degree)) {
        chosen = name;
        best_card = card;
        best_degree = deg;
      }
    }
    return chosen;
  };

  std::unordered_set<std::string> remaining;
  for (const auto &[alias, _] : alias2plan) {
    remaining.insert(alias);
  }
  std::unordered_set<std::string> selected;

  std::string first = choose_first(remaining);
  if (first.empty()) {
    return plan;
  }
  remaining.erase(first);
  selected.insert(first);
  auto current_plan = alias2plan[first];
  double current_card = static_cast<double>(cardinality[first]);

  auto estimate_selectivity = [&](const JoinEdge &edge) {
    auto distinct_left = catalog_.GetDistinct(alias2base[edge.left_table], edge.left_column);
    auto distinct_right = catalog_.GetDistinct(alias2base[edge.right_table], edge.right_column);
    if (distinct_left == INVALID_DISTINCT || distinct_right == INVALID_DISTINCT) {
      return 0.1;  // fallback selectivity
    }
    auto max_distinct = std::max(distinct_left, distinct_right);
    if (max_distinct == 0) {
      return 1.0;
    }
    return 1.0 / static_cast<double>(max_distinct);
  };

  while (!remaining.empty()) {
    std::string best_table;
    double best_result = std::numeric_limits<double>::max();

    for (const auto &candidate : remaining) {
      for (const auto &edge : edges) {
        bool connect_left = selected.count(edge.left_table) > 0 && edge.right_table == candidate;
        bool connect_right = selected.count(edge.right_table) > 0 && edge.left_table == candidate;
        if (!connect_left && !connect_right) {
          continue;
        }
        auto sel = estimate_selectivity(edge);
        auto estimate = current_card * static_cast<double>(cardinality[candidate]) * sel;
        if (estimate < best_result) {
          best_result = estimate;
          best_table = candidate;
        }
      }
    }

    if (best_table.empty()) {
      // not connected, fall back to cartesian join
      best_table = *remaining.begin();
      best_result = current_card * static_cast<double>(cardinality[best_table]);
    }

    // predicates connecting the chosen table and already selected tables
    std::vector<JoinEdge> used_edges;
    for (const auto &edge : edges) {
      bool connected = (edge.left_table == best_table && selected.count(edge.right_table) > 0) ||
                       (edge.right_table == best_table && selected.count(edge.left_table) > 0);
      if (connected) {
        used_edges.push_back(edge);
      }
    }

    auto right_plan = alias2plan[best_table];
    std::shared_ptr<OperatorExpression> join_condition;
    for (const auto &edge : used_edges) {
      bool left_in_selected = selected.count(edge.left_table) > 0;
      bool right_in_selected = selected.count(edge.right_table) > 0;
      auto left_expr = MakeColumnValue(left_in_selected ? current_plan : right_plan,
                                       fmt::format("{}.{}", edge.left_table, edge.left_column), left_in_selected);
      auto right_expr = MakeColumnValue(right_in_selected ? current_plan : right_plan,
                                        fmt::format("{}.{}", edge.right_table, edge.right_column), right_in_selected);
      auto cmp = std::make_shared<Comparison>(edge.cmp_type, left_expr, right_expr);
      if (!join_condition) {
        join_condition = cmp;
      } else {
        join_condition = std::make_shared<Logic>(LogicType::AND, join_condition, cmp);
      }
    }
    if (!join_condition) {
      join_condition = std::make_shared<Const>(Value(true));
    }

    auto column_list = CombineColumnList(*current_plan, *right_plan);
    current_plan = std::make_shared<NestedLoopJoinOperator>(column_list, current_plan, right_plan, join_condition);
    current_card = best_result;

    selected.insert(best_table);
    remaining.erase(best_table);
  }

  return current_plan;
}

void Optimizer::CollectPredicates(const std::shared_ptr<OperatorExpression> &expr,
                                  std::vector<std::shared_ptr<OperatorExpression>> &predicates) {
  if (!expr) {
    return;
  }
  if (expr->GetExprType() == OperatorExpressionType::LOGIC) {
    auto logic = std::dynamic_pointer_cast<Logic>(expr);
    if (logic->GetLogicType() == LogicType::AND) {
      CollectPredicates(logic->children_[0], predicates);
      CollectPredicates(logic->children_[1], predicates);
      return;
    }
  }
  predicates.push_back(expr);
}

std::shared_ptr<ColumnValue> Optimizer::MakeColumnValue(const std::shared_ptr<Operator> &plan, const std::string &name,
                                                        bool is_left) {
  auto idx = plan->OutputColumns().GetColumnIndex(name);
  auto &col = plan->OutputColumns().GetColumn(idx);
  return std::make_shared<ColumnValue>(idx, col.type_, col.name_, col.GetMaxSize(), is_left);
}

std::shared_ptr<SeqScanOperator> Optimizer::FindSeqScan(const std::shared_ptr<Operator> &plan) {
  if (!plan) {
    return nullptr;
  }
  if (plan->GetType() == OperatorType::SEQSCAN) {
    return std::dynamic_pointer_cast<SeqScanOperator>(plan);
  }
  for (const auto &child : plan->children_) {
    auto result = FindSeqScan(child);
    if (result) {
      return result;
    }
  }
  return nullptr;
}

std::shared_ptr<ColumnList> Optimizer::CombineColumnList(const Operator &left, const Operator &right) {
  auto column_list = std::make_shared<ColumnList>();
  for (const auto &col : left.OutputColumns().GetColumns()) {
    column_list->AddColumn(col);
  }
  for (const auto &col : right.OutputColumns().GetColumns()) {
    column_list->AddColumn(col);
  }
  return column_list;
}

bool Optimizer::PushPredicateToScan(std::shared_ptr<Operator> &plan, const std::shared_ptr<Comparison> &cmp) {
  auto left_col = std::dynamic_pointer_cast<ColumnValue>(cmp->children_[0]);
  auto right_col = std::dynamic_pointer_cast<ColumnValue>(cmp->children_[1]);
  auto target_col = left_col ? left_col : right_col;
  auto other_expr = left_col ? cmp->children_[1] : cmp->children_[0];

  auto pos = target_col->name_.find('.');
  auto table_name = pos == std::string::npos ? target_col->name_ : target_col->name_.substr(0, pos);

  if (plan->GetType() == OperatorType::SEQSCAN) {
    auto scan = std::dynamic_pointer_cast<SeqScanOperator>(plan);
    if (scan->GetTableNameOrAlias() != table_name) {
      return false;
    }
    auto predicate =
        std::make_shared<Comparison>(cmp->GetComparisonType(), MakeColumnValue(plan, target_col->name_, true), other_expr);
    auto column_list = std::make_shared<ColumnList>(plan->OutputColumns());
    plan = std::make_shared<FilterOperator>(column_list, plan, predicate);
    return true;
  }

  for (size_t i = 0; i < plan->children_.size(); i++) {
    auto &child = plan->children_[i];
    if (child->OutputColumns().TryGetColumnIndex(target_col->name_)) {
      if (PushPredicateToScan(child, cmp)) {
        plan->children_[i] = child;
        return true;
      }
    }
  }
  return false;
}

bool Optimizer::PushPredicateToJoin(std::shared_ptr<Operator> &plan, const std::shared_ptr<Comparison> &cmp) {
  auto left_col = std::dynamic_pointer_cast<ColumnValue>(cmp->children_[0]);
  auto right_col = std::dynamic_pointer_cast<ColumnValue>(cmp->children_[1]);
  if (!left_col || !right_col) {
    return false;
  }

  if (plan->GetType() == OperatorType::NESTEDLOOP) {
    auto join = std::dynamic_pointer_cast<NestedLoopJoinOperator>(plan);
    bool left_in_left = join->children_[0]->OutputColumns().TryGetColumnIndex(left_col->name_).has_value();
    bool left_in_right = join->children_[1]->OutputColumns().TryGetColumnIndex(left_col->name_).has_value();
    bool right_in_left = join->children_[0]->OutputColumns().TryGetColumnIndex(right_col->name_).has_value();
    bool right_in_right = join->children_[1]->OutputColumns().TryGetColumnIndex(right_col->name_).has_value();

    if ((left_in_left && right_in_right) || (left_in_right && right_in_left)) {
      // columns live on different sides of this join, attach predicate here
      std::string left_name = left_in_left ? left_col->name_ : right_col->name_;
      std::string right_name = left_in_left ? right_col->name_ : left_col->name_;
      auto lhs = MakeColumnValue(join->children_[0], left_name, true);
      auto rhs = MakeColumnValue(join->children_[1], right_name, false);
      auto new_cmp = std::make_shared<Comparison>(cmp->GetComparisonType(), lhs, rhs);
      auto const_val = std::dynamic_pointer_cast<Const>(join->join_condition_);
      bool is_true = const_val && const_val->value_.GetType() == Type::BOOL && !const_val->value_.IsNull() &&
                     const_val->value_.GetValue<bool>();
      if (is_true) {
        join->join_condition_ = new_cmp;
      } else {
        join->join_condition_ = std::make_shared<Logic>(LogicType::AND, join->join_condition_, new_cmp);
      }
      return true;
    }

    bool pushed = false;
    if (left_in_left || right_in_left) {
      pushed = PushPredicateToJoin(join->children_[0], cmp);
      if (pushed) {
        return true;
      }
    }
    if (left_in_right || right_in_right) {
      pushed = PushPredicateToJoin(join->children_[1], cmp);
      if (pushed) {
        return true;
      }
    }
    return false;
  }

  for (size_t i = 0; i < plan->children_.size(); i++) {
    auto &child = plan->children_[i];
    if (child->OutputColumns().TryGetColumnIndex(left_col->name_) ||
        child->OutputColumns().TryGetColumnIndex(right_col->name_)) {
      if (PushPredicateToJoin(child, cmp)) {
        plan->children_[i] = child;
        return true;
      }
    }
  }
  return false;
}

}  // namespace huadb
