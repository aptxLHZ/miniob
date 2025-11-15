// ================== 请用这个最终版本替换 .cpp 文件 ==================
/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sql/operator/group_by_vec_physical_operator.h"
#include "storage/common/chunk.h"
#include "sql/expr/expression.h"

RC GroupByVecPhysicalOperator::open(Trx *trx)
{
  RC rc = RC::SUCCESS;
  if (opened_) {
    return rc;
  }

  if (children().empty() || children()[0] == nullptr) {
      return RC::INVALID_ARGUMENT;
  }
  if (OB_FAIL(children()[0]->open(trx))) {
    LOG_WARN("Failed to open child operator");
    return rc;
  }

  hash_table_ = std::make_unique<StandardAggregateHashTable>(aggregations_);
  Chunk child_chunk;
  
  while (rc == RC::SUCCESS) {
    rc = children()[0]->next(child_chunk);
    if (rc == RC::RECORD_EOF) {
        rc = RC::SUCCESS; 
        break;
    }
    if (rc != RC::SUCCESS || child_chunk.rows() == 0) {
        break;
    }

    Chunk groups_chunk;
    Chunk aggrs_chunk;
    
    // 1. 计算 Group By 键
    for (const auto& expr : group_by_exprs_) {
        Column col(expr->value_type(), expr->value_length(), child_chunk.rows());
        if (OB_FAIL(expr->get_column(child_chunk, col))) {
            LOG_WARN("Failed to evaluate group by expression");
            return rc;
        }
        groups_chunk.add_column(make_unique<Column>(std::move(col)), 0);
    }
    
    // 2. 计算聚合函数的输入值
    for (auto& aggr_expr_base : aggregations_) {
        auto* aggr_expr = static_cast<AggregateExpr*>(aggr_expr_base);
        Expression* child_expr = aggr_expr->child().get();

        if (child_expr != nullptr) {
            Column col(child_expr->value_type(), child_expr->value_length(), child_chunk.rows());
            if (OB_FAIL(child_expr->get_column(child_chunk, col))) {
                LOG_WARN("Failed to evaluate aggregation child expression");
                return rc;
            }
            aggrs_chunk.add_column(make_unique<Column>(std::move(col)), 0);
        } else {
            // 对于 COUNT(*), child_expr 是 nullptr.
            // 我们需要添加一个占位列, 但 add_chunk 内部会处理.
            // 关键是确保 aggrs_chunk 的行数信息是正确的。
            // 只要 groups_chunk 有正确的行数，或者 aggrs_chunk 至少有一列有正确的行数，
            // add_chunk 里的循环就能正确执行。
            // 我们在这里什么都不做, add_chunk 看到 aggrs_chunk.column_num() < aggr_idx 会知道如何处理.
            // 错! hash_table_->add_chunk 需要 aggrs_chunk 的列数和聚合函数数量一致
            Column placeholder_col(AttrType::INTS, sizeof(int32_t), child_chunk.rows());
            placeholder_col.set_count(child_chunk.rows());
            aggrs_chunk.add_column(make_unique<Column>(std::move(placeholder_col)), 0);
        }
    }
    
    if (OB_FAIL(hash_table_->add_chunk(groups_chunk, aggrs_chunk))) {
      LOG_WARN("Failed to add chunk to hash table");
      return rc;
    }

    child_chunk.reset_data();
  }
  if (rc != RC::SUCCESS) {
      return rc;
  }

  ht_scanner_ = std::make_unique<StandardAggregateHashTable::Scanner>(hash_table_.get());
  ht_scanner_->open_scan();

  opened_ = true;
  return rc;
}

RC GroupByVecPhysicalOperator::next(Chunk &chunk)
{
  if (!opened_) {
    return RC::INTERNAL;
  }
  chunk.reset_data(); // 清空数据，但保留列定义

  // 如果 chunk 还没有列，我们需要为它初始化列
  if (chunk.column_num() == 0) {
      // 1. 添加 Group By 的列
      int col_id = 0;
      for (const auto& expr : group_by_exprs_) {
          auto col = make_unique<Column>(expr->value_type(), expr->value_length());
          chunk.add_column(std::move(col), col_id++);
      }
      // 2. 添加聚合函数的列
      for (const auto& expr : aggregations_) {
          auto col = make_unique<Column>(expr->value_type(), expr->value_length());
          chunk.add_column(std::move(col), col_id++);
      }
  }

  // 现在 chunk 已经有了正确的列结构，可以安全地传给 scanner 去填充数据了
  return ht_scanner_->next(chunk);
}

RC GroupByVecPhysicalOperator::close()
{
  hash_table_.reset();
  ht_scanner_.reset();
  opened_ = false;

  if (!children().empty() && children()[0] != nullptr) {
    return children()[0]->close();
  }
  return RC::SUCCESS;
}