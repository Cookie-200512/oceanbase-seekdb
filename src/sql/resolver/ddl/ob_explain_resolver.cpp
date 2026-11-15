/*
 * Copyright (c) 2025 OceanBase.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#define USING_LOG_PREFIX SQL_RESV
#include "sql/resolver/ddl/ob_explain_resolver.h"
#include "sql/resolver/ob_resolver.h"
#include "sql/resolver/dml/ob_select_resolver.h"
#include "sql/parser/ob_parser.h"

using namespace oceanbase;
using namespace sql;
using namespace common;
const static int64_t EXPLAIN_FORMAT = 0;
const static int64_t EXPLAIN_DISPLAY_OPTION = 1;
const static int64_t EXPLAIN_INTO_TABLE = 2;
const static int64_t EXPLAIN_STMT_ID = 3;
const static int64_t EXPLAIN_CHILD_STMT = 4;

// DESCRIBE spells an inferred type the way a column declaration does, which is not what an ObObjType
// is called: the boolean column of a file is a TINYINT and its bigint is an ObIntType. A null answer
// is a type no file probe infers, and with it a describe this path cannot answer.
static const char *file_sql_describe_type(const ObObjType type)
{
  const char *name = NULL;
  switch (type) {
    case ObTinyIntType: name = "BOOLEAN"; break;
    case ObIntType: name = "BIGINT"; break;
    case ObDoubleType: name = "DOUBLE"; break;
    case ObDateType: name = "DATE"; break;
    case ObDateTimeType: name = "DATETIME"; break;
    case ObVarcharType: name = "VARCHAR"; break;
    default: break;
  }
  return name;
}

// The names below go into a quoted literal as they are, so a name that could close the quote early
// or hide an escape is a describe this path does not answer.
static bool file_sql_is_literal_name(const ObString &name)
{
  bool valid = !name.empty();
  for (int64_t i = 0; valid && i < name.length(); ++i) {
    const char c = name.ptr()[i];
    valid = (c > ' ' && c <= '~' && '\'' != c && '\\' != c && '`' != c);
  }
  return valid;
}

// DESCRIBE of a file table shows the schema its probe inferred instead of a plan. The keyword is
// gone by the time the parser hands the statement over, so such a describe is exactly an explain of
// a select over the file, and this is the one place that has both the select and the probe. The rows
// are written out as a select of literals and resolved like any other query, which is what brings
// the six field headers, the printed NULLs and the result set itself from the ordinary select path:
// a file table has no column schema to build them from anywhere else. A null describe_stmt means
// this is not a describe that can be answered that way, and EXPLAIN keeps to its own path.
static int file_sql_build_describe(ObResolverParams &params,
                                   ObSQLSessionInfo *session_info,
                                   ObIAllocator *alloc,
                                   ObSelectStmt &select_stmt,
                                   ObSelectStmt *&describe_stmt)
{
  int ret = OB_SUCCESS;
  TableItem *table_item = NULL;
  ObFileTableDef *file_def = NULL;
  const int64_t col_cnt = select_stmt.get_select_item_size();
  int64_t name_len = 0;
  describe_stmt = NULL;
  if (OB_ISNULL(session_info) || OB_ISNULL(alloc)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("invalid resolver state", K(ret), KP(session_info), KP(alloc));
  } else if (1 != select_stmt.get_table_size()
             || OB_ISNULL(table_item = select_stmt.get_table_item(0))
             || !table_item->is_file_table()
             || OB_ISNULL(file_def = table_item->get_file_table_def())) {
    // not a scan of one file, so there is no inferred schema to show
  } else if (col_cnt <= 0 || col_cnt != file_def->inferred_column_count_
             || col_cnt != file_def->column_nulls_.length()) {
    // the probe did not describe every column of the list
  } else {
    bool describable = true;
    for (int64_t i = 0; describable && i < col_cnt; ++i) {
      const ObRawExpr *expr = select_stmt.get_select_item(i).expr_;
      const ObColumnRefRawExpr *col_expr = NULL;
      if (OB_ISNULL(expr) || T_REF_COLUMN != expr->get_expr_type()
          || NULL == file_sql_describe_type(expr->get_data_type())) {
        describable = false;
      } else if (FALSE_IT(col_expr = static_cast<const ObColumnRefRawExpr *>(expr))) {
      } else if (col_expr->get_table_id() != table_item->table_id_
                 || !file_sql_is_literal_name(col_expr->get_column_name())) {
        describable = false;
      } else {
        name_len += col_expr->get_column_name().length();
      }
    }
    if (!describable) {
      // a projection that is not the whole file read as it was inferred
    } else {
      const int64_t buf_len = 512 + name_len + 96 * col_cnt;
      char *buf = NULL;
      int64_t pos = 0;
      if (OB_ISNULL(buf = static_cast<char *>(alloc->alloc(buf_len)))) {
        ret = OB_ALLOCATE_MEMORY_FAILED;
        LOG_WARN("failed to alloc the describe sql", K(ret), K(buf_len), K(col_cnt));
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
        const ObColumnRefRawExpr *col_expr = static_cast<const ObColumnRefRawExpr *>(
            select_stmt.get_select_item(i).expr_);
        const ObString name = col_expr->get_column_name();
        const char *type_name = file_sql_describe_type(col_expr->get_data_type());
        const char *null_name = ('\0' == file_def->column_nulls_.ptr()[i]) ? "NO" : "YES";
        // only the first branch names the columns, which is where the headers come from
        const char *fmt = (0 == i)
            ? "select '%.*s' as `column_name`, '%s' as `column_type`, '%s' as `null`, "
              "NULL as `key`, NULL as `default`, NULL as `extra`"
            : " union all select '%.*s', '%s', '%s', NULL, NULL, NULL";
        if (OB_FAIL(databuff_printf(buf, buf_len, pos, fmt, name.length(), name.ptr(),
                                    type_name, null_name))) {
          LOG_WARN("failed to write the describe sql", K(ret), K(i), K(name));
        }
      }
      if (OB_FAIL(ret)) {
      } else {
        ObString sql(static_cast<int32_t>(pos), buf);
        ObParser parser(*alloc, session_info->get_sql_mode());
        ParseResult parse_result;
        if (OB_FAIL(parser.parse(sql, parse_result))) {
          LOG_WARN("failed to parse the describe sql", K(ret), K(sql));
        } else if (OB_ISNULL(parse_result.result_tree_)
                   || 1 != parse_result.result_tree_->num_child_
                   || OB_ISNULL(parse_result.result_tree_->children_[0])) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected describe parse result", K(ret), K(sql));
        } else {
          ObSelectResolver describe_resolver(params);
          if (OB_FAIL(describe_resolver.resolve(*parse_result.result_tree_->children_[0]))) {
            LOG_WARN("failed to resolve the describe sql", K(ret), K(sql));
          } else if (OB_ISNULL(describe_stmt = describe_resolver.get_select_stmt())) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("unexpected null describe stmt", K(ret), K(sql));
          }
        }
      }
    }
  }
  return ret;
}

int ObExplainResolver::resolve(const ParseNode &parse_tree)
{
  int ret = OB_SUCCESS;
  ObExplainStmt *explain_stmt = NULL;
  if (!(T_EXPLAIN == parse_tree.type_  && 5 == parse_tree.num_child_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("invalid EXPLAIN syntax",
             "type", parse_tree.type_,
             "# of child", parse_tree.num_child_);
  } else if (NULL == (explain_stmt = create_stmt<ObExplainStmt>())) {
    ret = OB_SQL_RESOLVER_NO_MEMORY;
    LOG_WARN("failed to create explain stmt");
  } else {
    ParseNode *child_node = parse_tree.children_[EXPLAIN_CHILD_STMT];
    ObStmt *child_stmt = NULL;
    ObExplainDisplayOpt opt;
    opt.with_tree_line_ = true;
    if (OB_ISNULL(child_node)) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("empty parse tree for stmt");
    } else if (!(T_SELECT == child_node->type_
                 || T_DELETE == child_node->type_
                 || T_INSERT == child_node->type_
                 || T_UPDATE == child_node->type_
                 || T_REPLACE == child_node->type_)) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("invalid stmt type for EXPLAIN", "type", child_node->type_);
      LOG_USER_ERROR(OB_NOT_SUPPORTED, "stmt type for EXPLAIN");
    } else {
      ObResolver resolver(params_);
      if (OB_FAIL(resolver.resolve(ObResolver::IS_NOT_PREPARED_STMT,
                                   *child_node,
                                   child_stmt))) {
      } else if (OB_ISNULL(child_stmt)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("failed to generate stmt for child stmt");
      } else {
        explain_stmt->set_explain_query_stmt(static_cast<ObDMLStmt *>(child_stmt));
        ParseNode *format = parse_tree.children_[EXPLAIN_FORMAT];
        if (NULL != format) {
          switch (format->type_) {
          case T_OUTLINE:
          {
            explain_stmt->set_explain_format(EXPLAIN_OUTLINE);
            opt.with_tree_line_ = true;
          } break;
          case T_EXTENDED:
          {
            explain_stmt->set_explain_format(EXPLAIN_EXTENDED);
            opt.with_tree_line_ = true;
          } break;
          case T_PARTITIONS:
          {
            explain_stmt->set_explain_format(EXPLAIN_PARTITIONS);
            opt.with_tree_line_ = true;
          } break;
          case T_TRADITIONAL:
          {
            explain_stmt->set_explain_format(EXPLAIN_TRADITIONAL);
            opt.with_tree_line_ = true;
          } break;
          case T_FORMAT_JSON:
          {
            explain_stmt->set_explain_format(EXPLAIN_FORMAT_JSON);
            opt.with_tree_line_ = false;
          } break;
          case T_BASIC: {
            explain_stmt->set_explain_format(EXPLAIN_BASIC);
            opt.with_tree_line_ = true;
            break;
          }
          case T_EXTENDED_NOADDR: {
            explain_stmt->set_explain_format(EXPLAIN_EXTENDED_NOADDR);
            opt.with_tree_line_ = true;
            break;
          }
          case T_PLANREGRESS: {
            explain_stmt->set_explain_format(EXPLAIN_PLANREGRESS);
            opt.with_tree_line_ = true;
            break;
          }
          default:
            ret = OB_ERROR;
            LOG_WARN("unknown explain format", "format", format->type_);
            break;
          }
        }
      }
    }
    ParseNode *opt_node = parse_tree.children_[EXPLAIN_DISPLAY_OPTION];
    if (OB_SUCC(ret) && NULL != opt_node) {
      switch (opt_node->type_) {
      case T_PRETTY_COLOR:
        opt.with_color_ = true;
        // pass
      case T_PRETTY:
        opt.with_tree_line_ = true;
        break;
      default:
        ret = OB_ERR_UNEXPECTED;
      }
    }
    if (OB_SUCC(ret)) {
      explain_stmt->set_display_opt(opt);
    }
    ParseNode *into_table_node = parse_tree.children_[EXPLAIN_INTO_TABLE];
    if (OB_SUCC(ret) && NULL != into_table_node) {
      ObString into_table(into_table_node->str_len_, into_table_node->str_value_);
      explain_stmt->set_into_table(into_table);
    }
    ParseNode *statement_id_node = parse_tree.children_[EXPLAIN_STMT_ID];
    if (OB_SUCC(ret) && NULL != statement_id_node) {
      ObString statement_id(statement_id_node->str_len_, statement_id_node->str_value_);
      explain_stmt->set_statement_id(statement_id);
    }
    // A describe of a file table answers with the schema of the file instead, which replaces the
    // statement handed to the rest of the pipeline with an ordinary select.
    if (OB_SUCC(ret) && NULL != child_stmt && stmt::T_SELECT == child_stmt->get_stmt_type()) {
      ObSelectStmt *describe_stmt = NULL;
      if (OB_FAIL(file_sql_build_describe(params_, session_info_, allocator_,
                                          *static_cast<ObSelectStmt *>(child_stmt),
                                          describe_stmt))) {
        LOG_WARN("failed to describe the file table", K(ret));
      } else if (NULL != describe_stmt) {
        set_basic_stmt(describe_stmt);
      }
    }
  }

  return ret;
}
