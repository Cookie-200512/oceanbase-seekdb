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

#define USING_LOG_PREFIX SQL_ENG

#include "sql/engine/basic/ob_function_table_op.h"
#include "sql/engine/ob_exec_context.h"
#include "sql/engine/expr/ob_expr_lob_utils.h"
#include "sql/engine/cmd/ob_load_data_parser.h"
#include "common/timezone/ob_time_convert.h"
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <dirent.h>
#include <fnmatch.h>


namespace oceanbase
{
using namespace common;
namespace sql
{

OB_SERIALIZE_MEMBER(ObFileTableParams, file_format_, parse_header_, skip_header_lines_,
                    field_term_, line_term_, enclosed_char_, escaped_char_, sample_row_limit_,
                    inferred_column_count_);

// The names of a jsonl scan have to survive the plan cache with their bytes, and ObFixedArray
// writes only how many elements it holds, so the spec is written by hand: file_params_ keeps its
// by-value serialization and the names follow as a count plus one string each.
OB_DEF_SERIALIZE(ObFunctionTableSpec)
{
  INIT_SUCC(ret);
  BASE_SER((ObFunctionTableSpec, ObOpSpec));
  OB_UNIS_ENCODE(value_expr_);
  OB_UNIS_ENCODE(column_exprs_);
  OB_UNIS_ENCODE(has_correlated_expr_);
  OB_UNIS_ENCODE(file_params_);
  int32_t name_count = column_names_.count();
  OB_UNIS_ENCODE(name_count);
  for (int64_t i = 0; OB_SUCC(ret) && i < column_names_.count(); ++i) {
    OB_UNIS_ENCODE(column_names_.at(i));
  }
  return ret;
}

OB_DEF_SERIALIZE_SIZE(ObFunctionTableSpec)
{
  int64_t len = 0;
  BASE_ADD_LEN((ObFunctionTableSpec, ObOpSpec));
  OB_UNIS_ADD_LEN(value_expr_);
  OB_UNIS_ADD_LEN(column_exprs_);
  OB_UNIS_ADD_LEN(has_correlated_expr_);
  OB_UNIS_ADD_LEN(file_params_);
  int32_t name_count = column_names_.count();
  OB_UNIS_ADD_LEN(name_count);
  for (int64_t i = 0; i < column_names_.count(); ++i) {
    OB_UNIS_ADD_LEN(column_names_.at(i));
  }
  return len;
}

OB_DEF_DESERIALIZE(ObFunctionTableSpec)
{
  INIT_SUCC(ret);
  BASE_DESER((ObFunctionTableSpec, ObOpSpec));
  OB_UNIS_DECODE(value_expr_);
  OB_UNIS_DECODE(column_exprs_);
  OB_UNIS_DECODE(has_correlated_expr_);
  OB_UNIS_DECODE(file_params_);
  int32_t name_count = 0;
  OB_UNIS_DECODE(name_count);
  if (OB_FAIL(ret)) {
  } else if (name_count < 0) {
    ret = OB_DESERIALIZE_ERROR;
    LOG_WARN("invalid file table column name count", K(ret), K(name_count));
  } else if (name_count > 0 && OB_FAIL(column_names_.init(name_count))) {
    LOG_WARN("failed to init the file table column names", K(ret), K(name_count));
  }
  for (int64_t i = 0; OB_SUCC(ret) && i < name_count; ++i) {
    ObString name;
    ObString copied;
    OB_UNIS_DECODE(name);
    // a decoded string only points at the buffer being read, which the plan does not own; the
    // array was built on the plan allocator, so that is where the copy belongs
    if (OB_FAIL(ret)) {
      LOG_WARN("failed to read the file table column name", K(ret), K(i));
    } else if (OB_ISNULL(column_names_.get_allocator())) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("unexpected null file table allocator", K(ret), K(i));
    } else if (OB_FAIL(ob_write_string(*column_names_.get_allocator(), name, copied))) {
      LOG_WARN("failed to copy the file table column name", K(ret), K(i), K(name));
    } else if (OB_FAIL(column_names_.push_back(copied))) {
      LOG_WARN("failed to add the file table column name", K(ret), K(i), K(copied));
    }
  }
  return ret;
}

namespace
{

// The scanner holds one chunk of the file at a time, so a row that straddles the chunk boundary is
// kept aside and the next read appends behind it. Same size as the probe buffer of the resolver.
const int64_t FILE_SQL_READ_CHUNK_SIZE = 64 * 1024;
// ObDMLResolver::check_file_table_path canonicalises a file path to at most this many bytes.
const int64_t FILE_SQL_PATH_BUF_SIZE = 4096;
// one byte per delimiter plus two terminators
const int64_t FILE_SQL_TERM_BUF_SIZE = 4;
// ObFileTableDef::file_format_ carries the kind of read_* the resolver resolved. Spelled out here
// instead of reused from the resolver, whose file scope the Unity build shares with this one.
const int64_t FILE_SQL_SPEC_FORMAT_CSV = 0;
const int64_t FILE_SQL_SPEC_FORMAT_JSONL = 1;
// The third entry of ObFileTableFuncKind, which is a directory pattern rather than a file to read.
const int64_t FILE_SQL_SPEC_FORMAT_GLOB = 3;

// A json row is the text between two newlines. Blanks around it carry nothing, and a CRLF file
// keeps the carriage return inside the row.
inline void file_sql_trim_json_line(common::ObString &line)
{
  while (!line.empty() && (' ' == line.ptr()[0] || '\t' == line.ptr()[0])) {
    line.assign_ptr(line.ptr() + 1, line.length() - 1);
  }
  while (!line.empty()
         && (' ' == line.ptr()[line.length() - 1] || '\t' == line.ptr()[line.length() - 1]
             || '\r' == line.ptr()[line.length() - 1])) {
    line.assign_ptr(line.ptr(), line.length() - 1);
  }
}

inline bool file_sql_is_digit(char c) { return c >= '0' && c <= '9'; }

// strntoll and strntodv2 skip leading blanks and stop at the first character that is not part of a
// number, so a field only counts as a number when it starts like one and ends where the converter
// stopped. This is the same shape test the resolver probe ran to choose the column type.
inline bool file_sql_is_number_start(const char *ptr, const int64_t len, const bool allow_dot)
{
  bool valid = false;
  if (len > 0) {
    const char first = ('-' == ptr[0] || '+' == ptr[0]) ? (len > 1 ? ptr[1] : '\0') : ptr[0];
    valid = file_sql_is_digit(first) || (allow_dot && '.' == first);
  }
  return valid;
}

static int file_sql_report_wrong_value(const char *type_name, const common::ObString &value,
                                       const int64_t row_no)
{
  int ret = OB_ERR_TRUNCATED_WRONG_VALUE;
  const common::ObString type_str(type_name);
  LOG_USER_ERROR(OB_ERR_TRUNCATED_WRONG_VALUE, type_str.length(), type_str.ptr(),
                 value.length(), value.ptr());
  LOG_WARN("csv value does not fit the type inferred for the column", K(ret), K(type_str),
           K(value), K(row_no));
  return ret;
}

// Turns one CSV field into the object type the resolver inferred for its column. LOAD DATA does no
// conversion at all and the SQL casts are too lenient to be usable here (cast('Alice' as signed)
// answers 0), so every type goes through a checked converter and a value that does not fit stops
// the scan instead of silently becoming a 0 or a NULL.
static int file_sql_convert_field(const ObExpr &expr, const common::ObString &value,
                                  const int64_t row_no, common::ObObj &obj)
{
  int ret = OB_SUCCESS;
  obj.reset();
  switch (expr.datum_meta_.get_type()) {
  case ObTinyIntType: {
    // BOOLEAN: the four spellings the probe accepts, decided by shape and never by a cast.
    if (1 == value.length() && ('0' == value.ptr()[0] || '1' == value.ptr()[0])) {
      obj.set_tinyint('1' == value.ptr()[0] ? 1 : 0);
    } else if (4 == value.length() && 0 == value.case_compare("true")) {
      obj.set_tinyint(1);
    } else if (5 == value.length() && 0 == value.case_compare("false")) {
      obj.set_tinyint(0);
    } else {
      ret = file_sql_report_wrong_value("boolean", value, row_no);
    }
    break;
  }
  case ObIntType: {
    if (!file_sql_is_number_start(value.ptr(), value.length(), false)) {
      ret = file_sql_report_wrong_value("integer", value, row_no);
    } else {
      char *end_ptr = NULL;
      int err = 0;
      const int64_t number = ObCharset::strntoll(value.ptr(), value.length(), 10, &end_ptr, &err);
      if (0 != err || value.ptr() + value.length() != end_ptr) {
        ret = file_sql_report_wrong_value("integer", value, row_no);
      } else {
        obj.set_int(number);
      }
    }
    break;
  }
  case ObDoubleType: {
    if (!file_sql_is_number_start(value.ptr(), value.length(), true)) {
      ret = file_sql_report_wrong_value("decimal", value, row_no);
    } else {
      char *end_ptr = NULL;
      int err = 0;
      const double number = ObCharset::strntodv2(value.ptr(), value.length(), &end_ptr, &err);
      if (0 != err || value.ptr() + value.length() != end_ptr) {
        ret = file_sql_report_wrong_value("decimal", value, row_no);
      } else {
        obj.set_double(number);
      }
    }
    break;
  }
  case ObDateType: {
    int32_t date_value = 0;
    if (OB_FAIL(ObTimeConverter::str_to_date(value, date_value))) {
      LOG_WARN("failed to read a date field of the csv row", K(ret), K(value), K(row_no));
      ret = OB_INVALID_DATE_VALUE;
      LOG_USER_ERROR(OB_INVALID_DATE_VALUE, value.length(), value.ptr(), "");
    } else {
      obj.set_date(date_value);
    }
    break;
  }
  case ObDateTimeType: {
    int64_t datetime_value = 0;
    const ObTimeZoneInfo *tz_info = NULL;
    const ObTimeConvertCtx cvrt_ctx(tz_info, false);
    if (OB_FAIL(ObTimeConverter::str_to_datetime(value, cvrt_ctx, datetime_value))) {
      LOG_WARN("failed to read a datetime field of the csv row", K(ret), K(value), K(row_no));
      ret = OB_INVALID_DATE_VALUE;
      LOG_USER_ERROR(OB_INVALID_DATE_VALUE, value.length(), value.ptr(), "");
    } else {
      obj.set_datetime(datetime_value);
    }
    break;
  }
  case ObVarcharType: {
    // The datum keeps this pointer until the next row, which is the contract of a row at a time
    // scan: the read buffer is only recycled in the call that follows this one.
    obj.set_varchar(value);
    break;
  }
  default:
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("inferred type has no csv converter", K(ret), K(expr.datum_meta_));
    break;
  }
  return ret;
}

// ObCSVGeneralParser instantiates both of its call sites, so a row handler has to accept the batch
// form even though USE_HANDLE_BATCH_LINES stays false and that branch never runs. The row itself is
// written by the caller from get_fields_per_line(), which still holds the line the handler saw.
class FileSqlScanRowHandler
{
public:
  explicit FileSqlScanRowHandler(int64_t &field_cnt) : field_cnt_(field_cnt) {}
  int operator()(const ObCSVGeneralParser::HandleOneLineParam &param)
  {
    field_cnt_ = param.field_cnt_;
    return OB_SUCCESS;
  }
  int operator()(const ObCSVGeneralParser::HandleBatchLinesParam &param)
  {
    UNUSED(param);
    return OB_NOT_SUPPORTED;
  }
private:
  int64_t &field_cnt_;
};

} // end namespace

int ObFunctionTableOp::inner_open()
{
  int ret = OB_SUCCESS;
  node_idx_ = 0;
  already_calc_ = false;
  if (OB_ISNULL(MY_SPEC.value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (MY_SPEC.file_params_.inferred_column_count_ > 0) {
    next_row_func_ = &ObFunctionTableOp::inner_get_next_row_file;
    if (OB_FAIL(open_file_reader())) {
      LOG_WARN("failed to open the file reader", K(ret), K(MY_SPEC.file_params_));
    }
  } else if (ObExtendType == MY_SPEC.value_expr_->datum_meta_.type_) {
    next_row_func_ = &ObFunctionTableOp::inner_get_next_row_udf;
  } else {
    next_row_func_ = &ObFunctionTableOp::inner_get_next_row_sys_func;
  }
  return ret;
}

int ObFunctionTableOp::inner_rescan()
{
  int ret = OB_SUCCESS;
  if (OB_FAIL(ObOperator::inner_rescan())) {
  } else {
    node_idx_ = 0;
    if (MY_SPEC.has_correlated_expr_) {
      row_count_ = 0;
      col_count_ = 0;
      value_table_ = NULL;
      already_calc_ = false;
    }
    if (MY_SPEC.file_params_.inferred_column_count_ > 0) {
      // A rescan reads the file again from its first row, and the path expression of a correlated
      // file table may have changed since, so the reader is released and opened afresh.
      if (OB_FAIL(release_file_reader())) {
        LOG_WARN("failed to release the file reader", K(ret));
      } else if (OB_FAIL(open_file_reader())) {
        LOG_WARN("failed to reopen the file reader", K(ret));
      }
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_close()
{
  int ret = OB_SUCCESS;
  node_idx_ = 0;
  row_count_ = 0;
  col_count_ = 0;
  value_table_ = NULL;
  if (MY_SPEC.file_params_.inferred_column_count_ > 0
      && OB_FAIL(release_file_reader())) {
    LOG_WARN("failed to release the file reader", K(ret));
  }
  return ret;
}

void ObFunctionTableOp::destroy()
{
  ObOperator::destroy();
}


int ObFunctionTableOp::get_current_result(ObObj &result)
{
  int ret = OB_SUCCESS;
  void *data = NULL;
  CK (already_calc_);
  if (node_idx_ < 0 || node_idx_ >= row_count_ * col_count_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("fail to get current result in table function",
              K(node_idx_), K(row_count_), K(col_count_));
  }
  do {
    CK (node_idx_ >= 0);
    if (OB_SUCC(ret) && node_idx_ >= row_count_) {
      ret = OB_ITER_END;
    }
    CK (OB_NOT_NULL(value_table_));
    OX (data = value_table_->get_data());
    CK (OB_NOT_NULL(data));
    OX (result = (static_cast<ObObj*>(data))[node_idx_++]);
  } while (OB_SUCC(ret) && result.get_meta().get_type() == ObMaxType);
  return ret;
}

int ObFunctionTableOp::inner_get_next_row()
{
  return (this->*next_row_func_)();
}

int ObFunctionTableOp::inner_get_next_row_udf()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  clear_evaluated_flag();
  if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_ISNULL(MY_SPEC.value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
  } else if (ObExtendType != MY_SPEC.value_expr_->datum_meta_.type_) {
    ret = OB_ERR_UNEXPECTED;
  } else if (FALSE_IT(plan_ctx->set_autoinc_id_tmp(0))) {
  } else if (OB_FAIL(ctx_.check_status())) {
  } else {
    ObDatum *value = nullptr;
    if (!already_calc_) {
      if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, value))) {
      } else if (value->is_null()) {
        //do nothing
      } else if (OB_ISNULL(value_table_ 
                 = reinterpret_cast<pl::ObPLCollection*>(value->get_ext()))) {
        ret = OB_ERR_UNEXPECTED;
      } else {
        row_count_ = value_table_->is_inited() ? value_table_->get_count() : 0;
        col_count_ = value_table_->get_column_count();
      }
      already_calc_ = true;
    }
    if (OB_SUCC(ret) && OB_UNLIKELY(node_idx_ >= row_count_)) {
      ret = OB_ITER_END;
    }
    CK (MY_SPEC.column_exprs_.count() >= col_count_);
    ObObj obj_stack[col_count_];
    if (OB_SUCC(ret)) {
      if (nullptr != value_table_ 
          && ObExtendType == value_table_->get_element_type().get_obj_type()) {
        pl::ObPLComposite *composite = NULL;
        pl::ObPLRecord *record = NULL;
        ObObj record_obj;
        OZ (get_current_result(record_obj));
        if (OB_FAIL(ret)) {
        } else if (ObUserDefinedSQLType == record_obj.get_type()) {
          obj_stack[0] = record_obj;   
        } else if (record_obj.is_pl_extend()) {
          CK (OB_NOT_NULL(composite = reinterpret_cast<pl::ObPLComposite*>(record_obj.get_ext())));
          if (OB_SUCC(ret)) {
            if (composite->is_record()) {
              OX (record = static_cast<pl::ObPLRecord*>(composite));
              CK (record->get_count() == col_count_);
              for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
                OZ (record->get_element(i, obj_stack[i]));
              }
            } else if (composite->is_collection()) {
              OX (obj_stack[0] = record_obj);
            } else {
              ret = OB_ERR_UNEXPECTED;
            }
          }
        } else {
          ret = OB_ERR_UNEXPECTED;
          LOG_ERROR("unexpected here", K(ret), K(record_obj), K(record_obj.meta_));
        }
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
          if (OB_FAIL(get_current_result(obj_stack[i]))) {
          }
        }
      }
    }
    if (OB_SUCC(ret)) {
      for (int64_t i = 0; OB_SUCC(ret) && i < col_count_; ++i) {
        if (obj_stack[i].is_null()) {
          MY_SPEC.column_exprs_.at(i)->locate_datum_for_write(eval_ctx_).set_null();
        } else {
          const ObObjDatumMapType &datum_map = MY_SPEC.column_exprs_.at(i)->obj_datum_map_;
          ObExpr * const &expr = MY_SPEC.column_exprs_.at(i);
          ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
          if (OB_FAIL(datum.from_obj(obj_stack[i], datum_map))) {
          } else if (is_lob_storage(obj_stack[i].get_type()) &&
                     OB_FAIL(ob_adjust_lob_datum(get_exec_ctx(), obj_stack[i],
                                                 expr->obj_meta_, datum_map,
                                                 get_exec_ctx().get_allocator(), datum))) {
          }
        }
        if (OB_SUCC(ret)) {
          MY_SPEC.column_exprs_.at(i)->set_evaluated_projected(eval_ctx_);
        }
      }
    }
  }
  return ret;
}

int ObFunctionTableOp::inner_get_next_row_sys_func()
{
  int ret = OB_SUCCESS;
  ObPhysicalPlanCtx *plan_ctx = nullptr;
  ObDatum *value = nullptr;
  clear_evaluated_flag();
  if (OB_ISNULL(plan_ctx = ctx_.get_physical_plan_ctx())) {
    ret = OB_ERR_UNEXPECTED;
  } else if (OB_FAIL(ctx_.check_status())) {
  } else if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, value))) {
    if (OB_ITER_END != ret) {
    }
  } else {
    MY_SPEC.column_exprs_.at(0)->locate_datum_for_write(eval_ctx_).set_datum(*value);
    MY_SPEC.column_exprs_.at(0)->set_evaluated_projected(eval_ctx_);
  }
  return ret;
}


// The file arrives to the executor as the const value expression of the table, which the resolver
// already resolved, canonicalised and checked against secure_file_priv.
int ObFunctionTableOp::open_file_reader()
{
  int ret = OB_SUCCESS;
  const int64_t col_cnt = MY_SPEC.file_params_.inferred_column_count_;
  const int64_t field_term = MY_SPEC.file_params_.field_term_;
  const int64_t line_term = MY_SPEC.file_params_.line_term_;
  ObDatum *path_datum = NULL;
  common::ObString real_path;
  char path_buf[FILE_SQL_PATH_BUF_SIZE + 1];
  int fd = -1;
  if (FILE_SQL_SPEC_FORMAT_GLOB == MY_SPEC.file_params_.file_format_) {
    // a pattern names no file to read, so the scan opens a listing instead
    ret = open_glob_reader();
  } else if (OB_UNLIKELY(col_cnt <= 0) || OB_ISNULL(MY_SPEC.value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected file table spec", K(ret), K(col_cnt), KP(MY_SPEC.value_expr_));
  } else if (OB_UNLIKELY(INT64_MAX == field_term || INT64_MAX == line_term)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected file table delimiters", K(ret), K(field_term), K(line_term));
  } else if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, path_datum))) {
    LOG_WARN("failed to eval the file path", K(ret));
  } else if (OB_ISNULL(path_datum) || path_datum->is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "file path");
    LOG_WARN("unexpected null file path", K(ret), KP(path_datum));
  } else if (FALSE_IT(real_path = path_datum->get_string())) {
  } else if (OB_UNLIKELY(real_path.empty() || real_path.length() > FILE_SQL_PATH_BUF_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "file path");
    LOG_WARN("invalid file path", K(ret), K(real_path));
  } else {
    MEMCPY(path_buf, real_path.ptr(), real_path.length());
    path_buf[real_path.length()] = '\0';
    do {
      fd = ::open(path_buf, O_RDONLY | O_CLOEXEC);
    } while (-1 == fd && EINTR == errno);
    if (-1 == fd) {
      ret = OB_FILE_NOT_EXIST;
      LOG_USER_ERROR(OB_FILE_NOT_EXIST);
      LOG_WARN("failed to open the file for scanning", K(ret), K(real_path), "sys_err", errno);
    } else {
      ObIAllocator &alloc = ctx_.get_allocator();
      file_fd_ = fd;
      file_valid_len_ = 0;
      file_parse_pos_ = 0;
      file_row_idx_ = 0;
      file_eof_ = false;
      if (OB_ISNULL(file_read_buf_)) {
        file_read_buf_ = static_cast<char *>(alloc.alloc(FILE_SQL_READ_CHUNK_SIZE));
        if (OB_ISNULL(file_read_buf_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc the file read buffer", K(ret));
        } else {
          file_read_buf_size_ = FILE_SQL_READ_CHUNK_SIZE;
        }
      }
      // The parser asks for an escape buffer at least as wide as the bytes it is handed, so it
      // mirrors the read buffer and needs no bounds of its own.
      if (OB_SUCC(ret) && FILE_SQL_SPEC_FORMAT_CSV == MY_SPEC.file_params_.file_format_
          && OB_ISNULL(file_escape_buf_)) {
        file_escape_buf_ = static_cast<char *>(alloc.alloc(FILE_SQL_READ_CHUNK_SIZE));
        if (OB_ISNULL(file_escape_buf_)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc the file escape buffer", K(ret));
        }
      }
      // The parser and its format are built from the spec only, so a rescan reuses them and only
      // the delimiters below have to stay alive as long as the parser does. A jsonl scan reads its
      // own rows out of the same buffer and needs neither.
      if (OB_SUCC(ret) && FILE_SQL_SPEC_FORMAT_CSV == MY_SPEC.file_params_.file_format_
          && OB_ISNULL(csv_parser_)) {
        char *mem = static_cast<char *>(alloc.alloc(sizeof(ObCSVGeneralParser)
                                                    + FILE_SQL_TERM_BUF_SIZE));
        if (OB_ISNULL(mem)) {
          ret = OB_ALLOCATE_MEMORY_FAILED;
          LOG_WARN("failed to alloc the csv parser", K(ret));
        } else {
          char *term_buf = mem + sizeof(ObCSVGeneralParser);
          term_buf[0] = static_cast<char>(field_term);
          term_buf[1] = static_cast<char>(line_term);
          ObCSVGeneralFormat format;
          format.field_term_str_.assign_ptr(term_buf, 1);
          format.line_term_str_.assign_ptr(term_buf + 1, 1);
          // an empty field is a NULL, which is how a CSV fixture spells a missing value, and fields
          // past the inferred width are dropped the same way the type probe dropped them.
          format.field_enclosed_char_ = MY_SPEC.file_params_.enclosed_char_;
          format.field_escaped_char_ = MY_SPEC.file_params_.escaped_char_;
          format.cs_type_ = common::CHARSET_UTF8MB4;
          format.file_column_nums_ = col_cnt;
          format.empty_field_as_null_ = true;
          format.ignore_extra_fields_ = true;
          csv_parser_ = new (mem) ObCSVGeneralParser();
          if (OB_FAIL(csv_parser_->init(format))) {
            LOG_WARN("failed to init the csv parser", K(ret), K(format), K(col_cnt));
          }
        }
      }
    }
  }
  return ret;
}

// A glob scan answers with the names of the files its pattern matches, which is a directory listing
// rather than a file to read: the resolver canonicalised the pattern and left it in the value
// expression, so all the scan does is one pass over the directory to measure the matches and one to
// write them as '\0' terminated paths. The list is built at open and kept for the whole scan, since
// the operator arena runs no destructor for anything held across rows, and a rescan refills the same
// buffer whenever the new list still fits it. The directory handle never outlives this call, so
// release_file_reader has nothing to close.
int ObFunctionTableOp::open_glob_reader()
{
  int ret = OB_SUCCESS;
  ObDatum *path_datum = NULL;
  ObString pattern;
  char pattern_buf[FILE_SQL_PATH_BUF_SIZE + 2];
  char dir_buf[FILE_SQL_PATH_BUF_SIZE + 2];
  const char *dir_name = NULL;
  const char *name_pattern = NULL;
  int64_t dir_len = 0;
  if (OB_ISNULL(MY_SPEC.value_expr_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected null glob path expr", K(ret));
  } else if (OB_FAIL(MY_SPEC.value_expr_->eval(eval_ctx_, path_datum))) {
    LOG_WARN("failed to eval the glob pattern", K(ret));
  } else if (OB_ISNULL(path_datum) || path_datum->is_null()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "file path");
    LOG_WARN("unexpected null glob pattern", K(ret), KP(path_datum));
  } else if (FALSE_IT(pattern = path_datum->get_string())) {
  } else if (OB_UNLIKELY(pattern.empty() || pattern.length() > FILE_SQL_PATH_BUF_SIZE)) {
    ret = OB_INVALID_ARGUMENT;
    LOG_USER_ERROR(OB_INVALID_ARGUMENT, "file path");
    LOG_WARN("invalid glob pattern", K(ret), K(pattern));
  } else {
    MEMCPY(pattern_buf, pattern.ptr(), pattern.length());
    pattern_buf[pattern.length()] = '\0';
    char *slash = strrchr(pattern_buf, '/');
    // the resolver turns every pattern into an absolute path, so the separator is always there
    if (NULL == slash) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "file path");
      LOG_WARN("glob pattern has no directory", K(ret), K(pattern));
    } else {
      MEMCPY(dir_buf, pattern_buf, slash - pattern_buf);
      dir_len = slash - pattern_buf;
      // the names hang off the directory, which is also the path every row starts with; a pattern
      // of '/' keeps the single separator it already had as the whole directory
      dir_buf[dir_len++] = '/';
      dir_buf[dir_len] = '\0';
      dir_name = dir_buf;
      name_pattern = slash + 1;
      DIR *dir = ::opendir(dir_name);
      if (NULL == dir) {
        ret = OB_FILE_NOT_EXIST;
        LOG_USER_ERROR(OB_FILE_NOT_EXIST);
        LOG_WARN("failed to open the directory of a glob", K(ret), K(pattern),
                 "sys_err", errno);
      } else {
        int64_t total = 0;
        struct dirent *entry = NULL;
        while (NULL != (entry = ::readdir(dir))) {
          // the two names every directory carries are never an answer to a pattern
          if (0 != fnmatch(name_pattern, entry->d_name, 0)
              || 0 == strcmp(".", entry->d_name) || 0 == strcmp("..", entry->d_name)) {
          } else {
            total += dir_len + static_cast<int64_t>(strlen(entry->d_name)) + 1;
          }
        }
        char *buf = file_read_buf_;
        if (total > file_read_buf_size_) {
          buf = static_cast<char *>(ctx_.get_allocator().alloc(total));
          if (OB_ISNULL(buf)) {
            ret = OB_ALLOCATE_MEMORY_FAILED;
            LOG_WARN("failed to alloc the glob list", K(ret), K(total));
          } else {
            file_read_buf_ = buf;
            file_read_buf_size_ = total;
          }
        }
        int64_t pos = 0;
        ::rewinddir(dir);
        while (OB_SUCC(ret) && NULL != (entry = ::readdir(dir))) {
          if (0 != fnmatch(name_pattern, entry->d_name, 0)
              || 0 == strcmp(".", entry->d_name) || 0 == strcmp("..", entry->d_name)) {
            // the same test as the one that measured, so the bytes fill the buffer exactly
          } else {
            const int64_t name_len = static_cast<int64_t>(strlen(entry->d_name));
            MEMCPY(buf + pos, dir_buf, dir_len);
            MEMCPY(buf + pos + dir_len, entry->d_name, name_len);
            buf[pos + dir_len + name_len] = '\0';
            pos += dir_len + name_len + 1;
          }
        }
        ::closedir(dir);
        if (OB_SUCC(ret) && OB_UNLIKELY(pos != total)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("the glob list did not fill the measured bytes", K(ret), K(pos), K(total));
        } else {
          file_valid_len_ = total;
          file_parse_pos_ = 0;
          file_row_idx_ = 0;
          file_eof_ = true;
        }
      }
    }
  }
  return ret;
}

int ObFunctionTableOp::release_file_reader()
{
  int ret = OB_SUCCESS;
  if (-1 != file_fd_) {
    if (0 != ::close(file_fd_)) {
      // the file was only ever read, so a failed close cannot cost more than the descriptor
      LOG_WARN("failed to close the scanned file", K_(file_fd), "sys_err", errno);
    }
    file_fd_ = -1;
  }
  file_valid_len_ = 0;
  file_parse_pos_ = 0;
  file_row_idx_ = 0;
  file_eof_ = false;
  // the pages of the json scratch are freed here rather than by a destructor, which the operator
  // arena never runs
  file_json_scratch_.reset();
  return ret;
}

int ObFunctionTableOp::inner_get_next_row_file()
{
  int ret = OB_SUCCESS;
  const int64_t col_cnt = MY_SPEC.file_params_.inferred_column_count_;
  clear_evaluated_flag();
  if (FILE_SQL_SPEC_FORMAT_GLOB == MY_SPEC.file_params_.file_format_) {
    // the next row is another matched name, not another line of a file
    ret = inner_get_next_row_glob();
  } else if (FILE_SQL_SPEC_FORMAT_JSONL == MY_SPEC.file_params_.file_format_) {
    // a json row names its values, so it is read by the producer that looks them up by name
    ret = inner_get_next_row_jsonl();
  } else if (OB_UNLIKELY(col_cnt <= 0) || OB_ISNULL(csv_parser_)
      || OB_UNLIKELY(MY_SPEC.column_exprs_.count() != col_cnt)
      || OB_UNLIKELY(csv_parser_->get_fields_per_line().count() != col_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected file table state", K(ret), K(col_cnt),
             "expr_cnt", MY_SPEC.column_exprs_.count(),
             "field_cnt", OB_ISNULL(csv_parser_) ? 0 : csv_parser_->get_fields_per_line().count());
  } else {
    bool got_row = false;
    bool need_read = false;
    // The resolver took the column names from the first line and kept that line out of its type
    // sample, so the scan starts one line later. file_row_idx_ counts physical lines, header
    // included, which also keeps the row number of a conversion error on the right line.
    bool drop_header = MY_SPEC.file_params_.parse_header_ && 0 == file_row_idx_;
    int64_t field_cnt = 0;
    ObSEArray<ObCSVGeneralParser::LineErrRec, 8> err_records;
    while (OB_SUCC(ret) && !got_row) {
      if (need_read || file_parse_pos_ >= file_valid_len_) {
        need_read = false;
        if (file_eof_) {
          ret = OB_ITER_END;
        } else {
          // keep the bytes of the row that has no boundary inside this chunk yet
          const int64_t keep = file_valid_len_ - file_parse_pos_;
          ssize_t read_len = 0;
          if (keep >= file_read_buf_size_) {
            ret = OB_NOT_SUPPORTED;
            LOG_USER_ERROR(OB_NOT_SUPPORTED, "a csv row longer than the file read buffer");
            LOG_WARN("no row boundary inside the read buffer", K(ret), K(keep),
                     K(file_read_buf_size_));
          } else {
            if (keep > 0) {
              MEMMOVE(file_read_buf_, file_read_buf_ + file_parse_pos_, keep);
            }
            file_valid_len_ = keep;
            file_parse_pos_ = 0;
            do {
              read_len = ::read(file_fd_, file_read_buf_ + file_valid_len_,
                                file_read_buf_size_ - file_valid_len_);
            } while (-1 == read_len && EINTR == errno);
            if (read_len < 0) {
              ret = OB_IO_ERROR;
              LOG_WARN("failed to read the scanned file", K(ret), "sys_err", errno);
            } else {
              file_valid_len_ += read_len;
              file_eof_ = (0 == read_len);
            }
          }
        }
      } else {
        // One row per call: the parser stops as soon as it emitted its row budget and rewinds to
        // the start of the next line, so the fields it leaves behind stay the row being returned.
        const char *pos = file_read_buf_ + file_parse_pos_;
        const char *end = file_read_buf_ + file_valid_len_;
        const int64_t old_pos = file_parse_pos_;
        int64_t rows = 1;
        field_cnt = 0;
        FileSqlScanRowHandler handler(field_cnt);
        // the extra parentheses keep the comma of the template argument list out of OB_FAIL, which
        // the preprocessor would otherwise read as an argument separator
        if (OB_FAIL((csv_parser_->scan<FileSqlScanRowHandler, true>(
                pos, end, rows, file_escape_buf_, file_escape_buf_ + file_read_buf_size_,
                handler, err_records, file_eof_)))) {
          LOG_WARN("failed to scan the csv chunk", K(ret), K(rows), K_(file_parse_pos));
        } else if (OB_FALSE_IT(file_parse_pos_ = pos - file_read_buf_)) {
        } else if (field_cnt > 0 && drop_header) {
          drop_header = false;
          ++file_row_idx_;
        } else if (field_cnt > 0) {
          got_row = true;
        } else if (file_parse_pos_ == old_pos) {
          // the last line of the chunk was refused and rewound, so the parser cannot move until it
          // sees the byte that closes the line; read more instead of scanning the same bytes again
          need_read = true;
        }
      }
    }
    // A blank line carries no field at all, so the loop above reads on instead of returning a row
    // of nothing but NULLs.
    if (OB_SUCC(ret)) {
      const ObIArray<ObCSVGeneralParser::FieldValue> &fields = csv_parser_->get_fields_per_line();
      for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
        ObExpr *expr = MY_SPEC.column_exprs_.at(i);
        if (OB_ISNULL(expr)) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("unexpected null column expr", K(ret), K(i));
        } else if (0 != fields.at(i).is_null_) {
          expr->locate_datum_for_write(eval_ctx_).set_null();
          expr->set_evaluated_projected(eval_ctx_);
        } else {
          ObObj obj;
          common::ObString value(fields.at(i).len_, fields.at(i).ptr_);
          if (value.length() > 0 && '\r' == value.ptr()[value.length() - 1]) {
            // a CRLF file leaves the carriage return inside the field, as it does for the probe
            value.assign_ptr(value.ptr(), value.length() - 1);
          }
          if (OB_FAIL(file_sql_convert_field(*expr, value, file_row_idx_ + 1, obj))) {
            LOG_WARN("failed to convert a csv field", K(ret), K(i), K(value), K_(file_row_idx));
          } else {
            ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
            if (OB_FAIL(datum.from_obj(obj, expr->obj_datum_map_))) {
              LOG_WARN("failed to write the csv field", K(ret), K(i), K(value));
            } else {
              expr->set_evaluated_projected(eval_ctx_);
            }
          }
        }
      }
      if (OB_SUCC(ret)) {
        ++file_row_idx_;
      }
    }
  }
  return ret;
}

// One row of a newline delimited json file: the line is parsed and then every inferred column is
// looked up by its key, because a json row says the name of each value instead of its place. A key
// the row does not carry and a null value both read as a NULL, which is what a field past the end of
// a csv row already meant, and a value that is there is the text a csv cell would have held, so the
// same converter fills the datum.
int ObFunctionTableOp::inner_get_next_row_jsonl()
{
  int ret = OB_SUCCESS;
  const int64_t col_cnt = MY_SPEC.file_params_.inferred_column_count_;
  ObSEArray<ObString, 16> row_names;
  ObSEArray<ObString, 16> row_texts;
  ObSEArray<bool, 16> row_is_nulls;
  if (OB_UNLIKELY(col_cnt <= 0)
      || OB_UNLIKELY(MY_SPEC.column_names_.count() != col_cnt)
      || OB_UNLIKELY(MY_SPEC.column_exprs_.count() != col_cnt)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected jsonl file table state", K(ret), K(col_cnt),
             "name_cnt", MY_SPEC.column_names_.count(), "expr_cnt", MY_SPEC.column_exprs_.count());
  } else {
    bool got_row = false;
    while (OB_SUCC(ret) && !got_row) {
      int64_t nl = -1;
      for (int64_t i = file_parse_pos_; i < file_valid_len_; ++i) {
        if ('\n' == file_read_buf_[i]) {
          nl = i;
          break;
        }
      }
      if (nl < 0 && !file_eof_) {
        // the row has no end inside this chunk yet, so keep it and read behind it
        const int64_t keep = file_valid_len_ - file_parse_pos_;
        ssize_t read_len = 0;
        if (keep >= file_read_buf_size_) {
          ret = OB_NOT_SUPPORTED;
          LOG_USER_ERROR(OB_NOT_SUPPORTED, "a json row longer than the file read buffer");
          LOG_WARN("no row boundary inside the read buffer", K(ret), K(keep),
                   K(file_read_buf_size_));
        } else {
          if (keep > 0) {
            MEMMOVE(file_read_buf_, file_read_buf_ + file_parse_pos_, keep);
          }
          file_valid_len_ = keep;
          file_parse_pos_ = 0;
          do {
            read_len = ::read(file_fd_, file_read_buf_ + keep, file_read_buf_size_ - keep);
          } while (-1 == read_len && EINTR == errno);
          if (read_len < 0) {
            ret = OB_IO_ERROR;
            LOG_WARN("failed to read the scanned file", K(ret), "sys_err", errno);
          } else {
            file_valid_len_ += read_len;
            file_eof_ = (0 == read_len);
          }
        }
      } else if (file_parse_pos_ >= file_valid_len_) {
        ret = OB_ITER_END;
      } else {
        // at the end of the file a row needs no closing newline to still be a row
        const int64_t line_end = (nl < 0) ? file_valid_len_ : nl;
        ObString line(static_cast<int32_t>(line_end - file_parse_pos_),
                      file_read_buf_ + file_parse_pos_);
        file_parse_pos_ = (nl < 0) ? file_valid_len_ : nl + 1;
        ++file_row_idx_;
        file_sql_trim_json_line(line);
        if (line.empty()) {
          // a blank line carries no row, which is also how a file ending in a newline closes
        } else {
          file_json_scratch_.reset();
          row_names.reuse();
          row_texts.reuse();
          row_is_nulls.reuse();
          if (OB_FAIL(file_sql_json_line_to_fields(file_json_scratch_, line, row_names, row_texts,
                                                   row_is_nulls))) {
            LOG_WARN("failed to read a json row", K(ret), K(line), K_(file_row_idx));
          } else {
            got_row = true;
            for (int64_t i = 0; OB_SUCC(ret) && i < col_cnt; ++i) {
              ObExpr *expr = MY_SPEC.column_exprs_.at(i);
              const ObString &name = MY_SPEC.column_names_.at(i);
              int64_t hit = -1;
              for (int64_t j = 0; hit < 0 && j < row_names.count(); ++j) {
                const ObString &key = row_names.at(j);
                if (key.length() == name.length()
                    && (key.empty() || 0 == MEMCMP(key.ptr(), name.ptr(), name.length()))) {
                  hit = j;
                }
              }
              if (OB_ISNULL(expr)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("unexpected null column expr", K(ret), K(i), K(name));
              } else if (hit < 0 || row_is_nulls.at(hit)) {
                expr->locate_datum_for_write(eval_ctx_).set_null();
                expr->set_evaluated_projected(eval_ctx_);
              } else {
                ObObj obj;
                const ObString value = row_texts.at(hit);
                if (OB_FAIL(file_sql_convert_field(*expr, value, file_row_idx_, obj))) {
                  LOG_WARN("failed to convert a json field", K(ret), K(i), K(name), K(value),
                           K_(file_row_idx));
                } else {
                  ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
                  if (OB_FAIL(datum.from_obj(obj, expr->obj_datum_map_))) {
                    LOG_WARN("failed to write the json field", K(ret), K(i), K(name), K(value));
                  } else {
                    expr->set_evaluated_projected(eval_ctx_);
                  }
                }
              }
            }
          }
        }
      }
    }
  }
  return ret;
}

// One matched path per row, out of the list open_glob_reader wrote at open. Nothing is parsed here:
// a row is the run of bytes up to the terminator that follows the read cursor, and the buffer holds
// the whole list, so the datums of every row stay readable until the next scan refreshes it.
int ObFunctionTableOp::inner_get_next_row_glob()
{
  int ret = OB_SUCCESS;
  ObExpr *expr = MY_SPEC.column_exprs_.count() > 0 ? MY_SPEC.column_exprs_.at(0) : NULL;
  if (OB_UNLIKELY(1 != MY_SPEC.file_params_.inferred_column_count_)
      || OB_UNLIKELY(MY_SPEC.column_exprs_.count() != 1) || OB_ISNULL(expr)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unexpected glob file table state", K(ret), K(MY_SPEC.file_params_),
             "expr_cnt", MY_SPEC.column_exprs_.count());
  } else if (file_parse_pos_ >= file_valid_len_) {
    ret = OB_ITER_END;
  } else {
    const char *start = file_read_buf_ + file_parse_pos_;
    const int64_t left = file_valid_len_ - file_parse_pos_;
    const int64_t name_len = static_cast<int64_t>(strnlen(start, left));
    const ObString path(static_cast<int32_t>(name_len), start);
    file_parse_pos_ += name_len + 1;
    ++file_row_idx_;
    ObObj obj;
    if (OB_FAIL(file_sql_convert_field(*expr, path, file_row_idx_, obj))) {
      LOG_WARN("failed to convert a glob path", K(ret), K(path), K_(file_row_idx));
    } else {
      ObDatum &datum = expr->locate_datum_for_write(eval_ctx_);
      if (OB_FAIL(datum.from_obj(obj, expr->obj_datum_map_))) {
        LOG_WARN("failed to write a glob path", K(ret), K(path));
      } else {
        expr->set_evaluated_projected(eval_ctx_);
      }
    }
  }
  return ret;
}

} // end namespace sql
} // end namespace oceanbase
