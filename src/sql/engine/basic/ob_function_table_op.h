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

#ifndef OCEANBASE_BASIC_OB_FUNCTION_TABLE_OP_H_
#define OCEANBASE_BASIC_OB_FUNCTION_TABLE_OP_H_

#include "sql/engine/ob_operator.h"
#include "sql/engine/basic/ob_chunk_datum_store.h"
#include "sql/engine/basic/ob_parquet_reader.h"
#include "lib/charset/ob_charset.h"
#include "lib/allocator/page_arena.h"

namespace oceanbase
{
namespace sql
{

class ObExpr;
class ObCSVGeneralParser;

// Parameters of a file table function (read_csv/read_json/read_parquet) that are decided at
// resolve time and have to survive plan caching. Kept POD-only (delimiter chars are stored as
// int64 codes instead of ObString) so the struct can be serialized by value without arena copy.
struct ObFileTableParams
{
  ObFileTableParams()
    : file_format_(0), parse_header_(true), skip_header_lines_(1), field_term_(','),
      line_term_('\n'), enclosed_char_('"'), escaped_char_(INT64_MAX), sample_row_limit_(100),
      inferred_column_count_(0)
  {}
  OB_UNIS_VERSION(1);
public:
  int64_t file_format_;            // 0: csv, 1: jsonl, 2: parquet
  bool parse_header_;
  int64_t skip_header_lines_;
  int64_t field_term_;             // delimiter char code, INT64_MAX means unset
  int64_t line_term_;
  int64_t enclosed_char_;
  int64_t escaped_char_;
  int64_t sample_row_limit_;       // -1 means probe the whole file
  int64_t inferred_column_count_;  // column count probed by the resolver, also needed when
                                   // column_exprs_ is empty, e.g. select count(*) from read_csv
  TO_STRING_KV(K_(file_format), K_(parse_header), K_(skip_header_lines), K_(field_term),
               K_(line_term), K_(enclosed_char), K_(escaped_char), K_(sample_row_limit),
               K_(inferred_column_count));
};

class ObFunctionTableSpec : public ObOpSpec
{
OB_UNIS_VERSION_V(1);
public:
  ObFunctionTableSpec(common::ObIAllocator &alloc, const ObPhyOperatorType type)
    : ObOpSpec(alloc, type), value_expr_(nullptr), column_exprs_(alloc), has_correlated_expr_(false),
      file_params_(), column_names_(alloc)
  {}
  ObExpr *value_expr_;
  common::ObFixedArray<ObExpr*, common::ObIAllocator> column_exprs_;
  bool has_correlated_expr_;
  ObFileTableParams file_params_;
  // The column names a jsonl scan looks a value up by, in the order the resolver probed them. Empty
  // for a csv, whose columns are positional. They are names rather than a layout because a json row
  // says the key of each value instead of its place, and they live here and not in file_params_
  // because that struct is copied by value and stays POD.
  common::ObFixedArray<common::ObString, common::ObIAllocator> column_names_;
};

class ObFunctionTableOp : public ObOperator
{
public:
  ObFunctionTableOp(ObExecContext &exec_ctx, const ObOpSpec &spec, ObOpInput *input)
    : ObOperator(exec_ctx, spec, input),
    node_idx_(0),
    already_calc_(false),
    row_count_(0),
    col_count_(0),
    value_table_(NULL),
    file_fd_(-1),
    file_read_buf_(nullptr),
    file_read_buf_size_(0),
    file_escape_buf_(nullptr),
    file_valid_len_(0),
    file_parse_pos_(0),
    file_row_idx_(0),
    file_eof_(false),
    csv_parser_(nullptr),
    file_json_scratch_("FileSqlJson"),
    parquet_reader_(nullptr)
  {}

  virtual int inner_open() override;
  virtual int inner_rescan() override;
  virtual int inner_get_next_row() override;
  //virtual int inner_get_next_batch(int64_t max_row_cnt) override;
  virtual int inner_close() override;
  virtual void destroy() override;
private:
  int inner_get_next_row_udf();
  int inner_get_next_row_sys_func();
  // file table scan, used when the spec carries file_params_; defined in a later batch
  int inner_get_next_row_file();
  // the newline delimited json form of the same scan, which reads a row by key instead of by place
  int inner_get_next_row_jsonl();
  int open_file_reader();
  int release_file_reader();
  // A glob scan lists a directory instead of reading a file, so it shares the streaming state but
  // none of the opening or the parsing: the names it matches are collected once at open.
  int open_glob_reader();
  int inner_get_next_row_glob();
  // A parquet scan takes its rows out of the column the reader decoded when it opened, so it uses
  // neither the streaming buffer nor the field parser.
  int open_parquet_reader();
  int inner_get_next_row_parquet();
  int get_current_result(common::ObObj &result);
  int64_t node_idx_;
  bool already_calc_;
  int64_t row_count_;
  int64_t col_count_;
  common::ObObj value_;
  pl::ObPLCollection *value_table_;
  int (ObFunctionTableOp::*next_row_func_)();
  // streaming state of a file table, lives only for the current execution
  int file_fd_;
  char *file_read_buf_;
  int64_t file_read_buf_size_;
  // where the parser collects a field whose quotes hid an escape, as wide as the read buffer
  char *file_escape_buf_;
  int64_t file_valid_len_;
  int64_t file_parse_pos_;
  int64_t file_row_idx_;
  bool file_eof_;
  ObCSVGeneralParser *csv_parser_;
  // the tree and the printed numbers of the json row being written, released one row early so the
  // datums of the previous row keep pointing at it until the next row starts
  common::ObArenaAllocator file_json_scratch_;
  // the decoded column of a parquet scan, released in release_file_reader because the operator
  // arena runs no destructor for it
  ObParquetReader *parquet_reader_;
};

} // end namespace sql
} // end namespace oceanbase

#endif /* OCEANBASE_BASIC_OB_FUNCTION_TABLE_OP_H_ */
