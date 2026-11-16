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

#include "sql/engine/basic/ob_parquet_reader.h"
#include <errno.h>
#include <fcntl.h>
#include <unistd.h>
#include <sys/stat.h>

namespace oceanbase
{
using namespace common;
namespace sql
{

namespace
{

// Thrift compact protocol field type ids, as the parquet footer and page headers use them.
const int8_t PQ_STOP = 0;
const int8_t PQ_TRUE = 1;
const int8_t PQ_FALSE = 2;
const int8_t PQ_BYTE = 3;
const int8_t PQ_I16 = 4;
const int8_t PQ_I32 = 5;
const int8_t PQ_I64 = 6;
const int8_t PQ_DOUBLE = 7;
const int8_t PQ_BINARY = 8;
const int8_t PQ_LIST = 9;
const int8_t PQ_SET = 10;
const int8_t PQ_MAP = 11;
const int8_t PQ_STRUCT = 12;

// parquet.thrift PhysicalType, FieldRepetitionType, CompressionCodec and PageType values.
const int32_t PQ_TYPE_INT32 = 1;
const int32_t PQ_REP_OPTIONAL = 1;
const int32_t PQ_CODEC_UNCOMPRESSED = 0;
const int32_t PQ_CODEC_SNAPPY = 1;
const int32_t PQ_PAGE_DATA_PAGE_V2 = 3;
const int32_t PQ_ENCODING_PLAIN = 0;

const int64_t PQ_MAGIC_SIZE = 4;
const int64_t PQ_FOOTER_TAIL_SIZE = 8;
const int64_t PQ_MAX_FILE_SIZE = 8 * 1024 * 1024;
const int32_t PQ_MAX_NEST_DEPTH = 16;

struct PqCur
{
  PqCur() : buf_(NULL), len_(0), pos_(0) {}
  const char *buf_;
  int64_t len_;
  int64_t pos_;
  OB_INLINE bool left(const int64_t n) const { return pos_ + n <= len_; }
  TO_STRING_KV(K_(len), K_(pos));
};

struct PqFieldHead
{
  PqFieldHead() : id_(0), type_(PQ_STOP) {}
  int16_t id_;
  int8_t type_;
  TO_STRING_KV(K_(id), K_(type));
};

// The whole struct walk shares this, so a footer that nests deeper than the thrift layout of
// parquet.thrift can reach is rejected instead of running off the buffer.
int pq_read_varint(PqCur &c, uint64_t &v)
{
  int ret = OB_SUCCESS;
  v = 0;
  int64_t shift = 0;
  bool done = false;
  while (OB_SUCC(ret) && !done && c.pos_ < c.len_) {
    const uint8_t b = static_cast<const uint8_t>(c.buf_[c.pos_++]);
    v |= (static_cast<uint64_t>(b & 0x7f)) << shift;
    if (0 == (b & 0x80)) {
      done = true;
    } else if ((shift += 7) >= 64) {
      ret = OB_ERR_UNEXPECTED;
      LOG_WARN("parquet varint too long", K(ret), K(c));
    }
  }
  if (!done) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parquet varint truncated", K(ret), K(c));
  }
  return ret;
}

int pq_read_zigzag32(PqCur &c, int32_t &v)
{
  int ret = OB_SUCCESS;
  uint64_t uv = 0;
  if (OB_FAIL(pq_read_varint(c, uv))) {
    LOG_WARN("failed to read the parquet zigzag varint", K(ret), K(c));
  } else {
    v = static_cast<int32_t>((uv >> 1) ^ (~(uv & 1) + 1));
  }
  return ret;
}

int pq_read_zigzag64(PqCur &c, int64_t &v)
{
  int ret = OB_SUCCESS;
  uint64_t uv = 0;
  if (OB_FAIL(pq_read_varint(c, uv))) {
    LOG_WARN("failed to read the parquet zigzag varint", K(ret), K(c));
  } else {
    v = static_cast<int64_t>((uv >> 1) ^ (~(uv & 1) + 1));
  }
  return ret;
}

// The binary payload is only ever borrowed out of the file image, which stays alive as long as
// the reader does, so nothing is copied here.
int pq_read_binary(PqCur &c, const char *&ptr, int64_t &len)
{
  int ret = OB_SUCCESS;
  uint64_t ulen = 0;
  if (OB_FAIL(pq_read_varint(c, ulen))) {
    LOG_WARN("failed to read the parquet binary length", K(ret), K(c));
  } else if (ulen > static_cast<uint64_t>(c.len_ - c.pos_)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parquet binary runs past the buffer", K(ret), "len", ulen, K(c));
  } else {
    ptr = c.buf_ + c.pos_;
    len = static_cast<int64_t>(ulen);
    c.pos_ += len;
  }
  return ret;
}

int pq_read_i32(PqCur &c, int32_t &v)
{
  return pq_read_zigzag32(c, v);
}

int pq_read_i64(PqCur &c, int64_t &v)
{
  return pq_read_zigzag64(c, v);
}

// One field header: the stop byte ends the struct, a zero delta means the id is spelled out in
// the zigzag varint that follows.
int pq_read_field_head(PqCur &c, int16_t &last_id, PqFieldHead &head)
{
  int ret = OB_SUCCESS;
  if (c.pos_ >= c.len_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parquet struct ran past the buffer", K(ret), K(c));
  } else {
    const uint8_t fh = static_cast<const uint8_t>(c.buf_[c.pos_++]);
    head.type_ = static_cast<int8_t>(fh & 0x0f);
    if (PQ_STOP == head.type_) {
      head.id_ = 0;
    } else {
      const int16_t delta = static_cast<int16_t>((fh >> 4) & 0x0f);
      if (0 == delta) {
        int32_t id = 0;
        if (OB_FAIL(pq_read_zigzag32(c, id))) {
          LOG_WARN("failed to read the explicit parquet field id", K(ret), K(c));
        } else {
          head.id_ = static_cast<int16_t>(id);
        }
      } else {
        head.id_ = static_cast<int16_t>(last_id + delta);
      }
      if (OB_SUCC(ret)) {
        last_id = head.id_;
      }
    }
  }
  return ret;
}

int pq_skip_value(PqCur &c, const int8_t type, const int32_t depth);

int pq_skip_struct(PqCur &c, const int32_t depth)
{
  int ret = OB_SUCCESS;
  int16_t last_id = 0;
  PqFieldHead head;
  bool done = false;
  while (OB_SUCC(ret) && !done) {
    if (OB_FAIL(pq_read_field_head(c, last_id, head))) {
      LOG_WARN("failed to read the parquet field header", K(ret), K(c));
    } else if (PQ_STOP == head.type_) {
      done = true;
    } else if (OB_FAIL(pq_skip_value(c, head.type_, depth))) {
      LOG_WARN("failed to skip a parquet field", K(ret), K(head), K(c));
    }
  }
  return ret;
}

int pq_skip_value(PqCur &c, const int8_t type, const int32_t depth)
{
  int ret = OB_SUCCESS;
  if (depth > PQ_MAX_NEST_DEPTH) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("parquet thrift nesting too deep", K(ret), K(depth));
  } else if (PQ_TRUE == type || PQ_FALSE == type) {
    // the payload of a compact bool is carried by the type id itself
  } else if (PQ_BYTE == type) {
    if (!c.left(1)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      ++c.pos_;
    }
  } else if (PQ_I16 == type || PQ_I32 == type || PQ_I64 == type) {
    uint64_t v = 0;
    ret = pq_read_varint(c, v);
  } else if (PQ_DOUBLE == type) {
    if (!c.left(8)) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      c.pos_ += 8;
    }
  } else if (PQ_BINARY == type) {
    const char *ptr = NULL;
    int64_t len = 0;
    ret = pq_read_binary(c, ptr, len);
  } else if (PQ_LIST == type || PQ_SET == type) {
    if (c.pos_ >= c.len_) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      const uint8_t hdr = static_cast<const uint8_t>(c.buf_[c.pos_++]);
      int64_t cnt = (hdr >> 4) & 0x0f;
      const int8_t elem_type = static_cast<int8_t>(hdr & 0x0f);
      if (15 == cnt) {
        uint64_t big = 0;
        if (OB_FAIL(pq_read_varint(c, big))) {
          LOG_WARN("failed to read the parquet list length", K(ret), K(c));
        } else {
          cnt = static_cast<int64_t>(big);
        }
      }
      for (int64_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
        if (OB_FAIL(pq_skip_value(c, elem_type, depth))) {
          LOG_WARN("failed to skip a parquet list element", K(ret), K(i), K(cnt), K(c));
        }
      }
    }
  } else if (PQ_MAP == type) {
    uint64_t cnt = 0;
    if (OB_FAIL(pq_read_varint(c, cnt))) {
      LOG_WARN("failed to read the parquet map size", K(ret), K(c));
    } else if (0 == cnt) {
      // an empty map carries no key/value header either
    } else if (c.pos_ >= c.len_) {
      ret = OB_ERR_UNEXPECTED;
    } else {
      const uint8_t kv = static_cast<const uint8_t>(c.buf_[c.pos_++]);
      const int8_t key_type = static_cast<int8_t>((kv >> 4) & 0x0f);
      const int8_t val_type = static_cast<int8_t>(kv & 0x0f);
      for (uint64_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
        if (OB_FAIL(pq_skip_value(c, key_type, depth))) {
          LOG_WARN("failed to skip a parquet map key", K(ret), K(i), K(c));
        } else if (OB_FAIL(pq_skip_value(c, val_type, depth))) {
          LOG_WARN("failed to skip a parquet map value", K(ret), K(i), K(c));
        }
      }
    }
  } else if (PQ_STRUCT == type) {
    ret = pq_skip_struct(c, depth + 1);
  } else {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("unknown parquet thrift field type", K(ret), K(type), K(c));
  }
  return ret;
}

// A list of structs is the only shape the reader walks element by element, and it is the shape
// every interesting part of a parquet footer has (schema, row groups, columns).
int pq_read_struct_list_head(PqCur &c, int64_t &cnt)
{
  int ret = OB_SUCCESS;
  cnt = 0;
  if (c.pos_ >= c.len_) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("empty parquet list", K(ret), K(c));
  } else {
    const uint8_t hdr = static_cast<const uint8_t>(c.buf_[c.pos_++]);
    int64_t n = (hdr >> 4) & 0x0f;
    const int8_t elem_type = static_cast<int8_t>(hdr & 0x0f);
    if (15 == n) {
      uint64_t big = 0;
      if (OB_FAIL(pq_read_varint(c, big))) {
        LOG_WARN("failed to read the parquet list length", K(ret), K(c));
      } else {
        n = static_cast<int64_t>(big);
      }
    }
    if (OB_FAIL(ret)) {
    } else if (PQ_STRUCT != elem_type) {
      ret = OB_NOT_SUPPORTED;
      LOG_WARN("parquet list of non structs", K(ret), K(elem_type), K(c));
    } else {
      cnt = n;
    }
  }
  return ret;
}

// A SchemaElement. Only the leaf fields a scan needs are read; the logical type annotations,
// encodings and the rest are skipped, which is what lets a footer written by another tool still
// be understood.
int pq_parse_schema_elem(PqCur &c, const int32_t depth, ObParquetReader::ColumnSchema &col,
                         bool &has_name)
{
  int ret = OB_SUCCESS;
  int16_t last_id = 0;
  PqFieldHead head;
  bool done = false;
  int32_t num_children = 0;
  has_name = false;
  col.name_.reset();
  col.type_ = ObNullType;
  col.nullable_ = false;
  bool got_type = false;
  while (OB_SUCC(ret) && !done) {
    if (OB_FAIL(pq_read_field_head(c, last_id, head))) {
      LOG_WARN("failed to read the parquet schema field header", K(ret), K(c));
    } else if (PQ_STOP == head.type_) {
      done = true;
    } else if (1 == head.id_ && PQ_I32 == head.type_) {
      int32_t type = 0;
      if (OB_FAIL(pq_read_i32(c, type))) {
        LOG_WARN("failed to read the parquet physical type", K(ret), K(c));
      } else {
        got_type = (type > 0);
        col.type_ = (PQ_TYPE_INT32 == type) ? ObIntType : ObNullType;
      }
    } else if (3 == head.id_ && PQ_I32 == head.type_) {
      int32_t rep = 0;
      if (OB_FAIL(pq_read_i32(c, rep))) {
        LOG_WARN("failed to read the parquet repetition type", K(ret), K(c));
      } else {
        col.nullable_ = (PQ_REP_OPTIONAL == rep);
      }
    } else if (4 == head.id_ && PQ_BINARY == head.type_) {
      const char *ptr = NULL;
      int64_t len = 0;
      if (OB_FAIL(pq_read_binary(c, ptr, len))) {
        LOG_WARN("failed to read the parquet column name", K(ret), K(c));
      } else {
        col.name_.assign_ptr(ptr, static_cast<int32_t>(len));
        has_name = true;
      }
    } else if (5 == head.id_ && PQ_I32 == head.type_) {
      if (OB_FAIL(pq_read_i32(c, num_children))) {
        LOG_WARN("failed to read the parquet child count", K(ret), K(c));
      }
    } else if (OB_FAIL(pq_skip_value(c, head.type_, depth))) {
      LOG_WARN("failed to skip a parquet schema field", K(ret), K(head), K(c));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (got_type && num_children > 0) {
    // A node that both holds values and has children is a repeated group, which is out of scope.
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("parquet nested column", K(ret), K(num_children), K(col));
  }
  return ret;
}

int pq_parse_column_meta(PqCur &c, const int32_t depth, int32_t &type, int32_t &codec,
                         int64_t &num_values, int64_t &data_page_offset)
{
  int ret = OB_SUCCESS;
  int16_t last_id = 0;
  PqFieldHead head;
  bool done = false;
  type = 0;
  codec = -1;
  num_values = 0;
  data_page_offset = 0;
  while (OB_SUCC(ret) && !done) {
    if (OB_FAIL(pq_read_field_head(c, last_id, head))) {
      LOG_WARN("failed to read the parquet column meta field header", K(ret), K(c));
    } else if (PQ_STOP == head.type_) {
      done = true;
    } else if (1 == head.id_ && PQ_I32 == head.type_) {
      if (OB_FAIL(pq_read_i32(c, type))) {
        LOG_WARN("failed to read the parquet column type", K(ret), K(c));
      }
    } else if (4 == head.id_ && PQ_I32 == head.type_) {
      if (OB_FAIL(pq_read_i32(c, codec))) {
        LOG_WARN("failed to read the parquet codec", K(ret), K(c));
      }
    } else if (5 == head.id_ && PQ_I64 == head.type_) {
      if (OB_FAIL(pq_read_i64(c, num_values))) {
        LOG_WARN("failed to read the parquet value count", K(ret), K(c));
      }
    } else if (9 == head.id_ && PQ_I64 == head.type_) {
      if (OB_FAIL(pq_read_i64(c, data_page_offset))) {
        LOG_WARN("failed to read the parquet page offset", K(ret), K(c));
      }
    } else if (OB_FAIL(pq_skip_value(c, head.type_, depth))) {
      LOG_WARN("failed to skip a parquet column meta field", K(ret), K(head), K(c));
    }
  }
  return ret;
}

int pq_parse_column_chunk(PqCur &c, const int32_t depth, int32_t &type, int32_t &codec,
                          int64_t &num_values, int64_t &data_page_offset)
{
  int ret = OB_SUCCESS;
  int16_t last_id = 0;
  PqFieldHead head;
  bool done = false;
  bool got_meta = false;
  while (OB_SUCC(ret) && !done) {
    if (OB_FAIL(pq_read_field_head(c, last_id, head))) {
      LOG_WARN("failed to read the parquet column chunk field header", K(ret), K(c));
    } else if (PQ_STOP == head.type_) {
      done = true;
    } else if (3 == head.id_ && PQ_STRUCT == head.type_) {
      if (OB_FAIL(pq_parse_column_meta(c, depth + 1, type, codec, num_values, data_page_offset))) {
        LOG_WARN("failed to read the parquet column meta data", K(ret), K(c));
      } else {
        got_meta = true;
      }
    } else if (OB_FAIL(pq_skip_value(c, head.type_, depth))) {
      LOG_WARN("failed to skip a parquet column chunk field", K(ret), K(head), K(c));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (!got_meta) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parquet column chunk has no meta data", K(ret), K(c));
  }
  return ret;
}

int pq_parse_row_group(PqCur &c, const int32_t depth, int32_t &type, int32_t &codec,
                       int64_t &num_values, int64_t &data_page_offset, int64_t &chunk_cnt)
{
  int ret = OB_SUCCESS;
  int16_t last_id = 0;
  PqFieldHead head;
  bool done = false;
  while (OB_SUCC(ret) && !done) {
    if (OB_FAIL(pq_read_field_head(c, last_id, head))) {
      LOG_WARN("failed to read the parquet row group field header", K(ret), K(c));
    } else if (PQ_STOP == head.type_) {
      done = true;
    } else if (1 == head.id_ && PQ_LIST == head.type_) {
      int64_t cnt = 0;
      if (OB_FAIL(pq_read_struct_list_head(c, cnt))) {
        LOG_WARN("failed to read the parquet column chunk list", K(ret), K(c));
      } else {
        chunk_cnt += cnt;
        for (int64_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
          if (OB_FAIL(pq_parse_column_chunk(c, depth + 1, type, codec, num_values,
                                            data_page_offset))) {
            LOG_WARN("failed to read a parquet column chunk", K(ret), K(i), K(cnt), K(c));
          }
        }
      }
    } else if (OB_FAIL(pq_skip_value(c, head.type_, depth))) {
      LOG_WARN("failed to skip a parquet row group field", K(ret), K(head), K(c));
    }
  }
  return ret;
}

int pq_parse_file_meta(PqCur &c, ObParquetReader::ColumnSchema &col, bool &has_col,
                       int64_t &num_rows, int32_t &type, int32_t &codec, int64_t &num_values,
                       int64_t &data_page_offset, int64_t &row_group_cnt, int64_t &chunk_cnt)
{
  int ret = OB_SUCCESS;
  int16_t last_id = 0;
  PqFieldHead head;
  bool done = false;
  int64_t leaf_cnt = 0;
  has_col = false;
  num_rows = 0;
  row_group_cnt = 0;
  chunk_cnt = 0;
  while (OB_SUCC(ret) && !done) {
    if (OB_FAIL(pq_read_field_head(c, last_id, head))) {
      LOG_WARN("failed to read the parquet file meta field header", K(ret), K(c));
    } else if (PQ_STOP == head.type_) {
      done = true;
    } else if (2 == head.id_ && PQ_LIST == head.type_) {
      int64_t cnt = 0;
      if (OB_FAIL(pq_read_struct_list_head(c, cnt))) {
        LOG_WARN("failed to read the parquet schema list", K(ret), K(c));
      } else {
        for (int64_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
          bool has_name = false;
          if (OB_FAIL(pq_parse_schema_elem(c, 1, col, has_name))) {
            LOG_WARN("failed to read a parquet schema element", K(ret), K(i), K(cnt), K(c));
          } else if (has_name && ObNullType != col.type_) {
            ++leaf_cnt;
            has_col = true;
          }
        }
      }
    } else if (3 == head.id_ && PQ_I64 == head.type_) {
      if (OB_FAIL(pq_read_i64(c, num_rows))) {
        LOG_WARN("failed to read the parquet row count", K(ret), K(c));
      }
    } else if (4 == head.id_ && PQ_LIST == head.type_) {
      int64_t cnt = 0;
      if (OB_FAIL(pq_read_struct_list_head(c, cnt))) {
        LOG_WARN("failed to read the parquet row group list", K(ret), K(c));
      } else {
        row_group_cnt = cnt;
        for (int64_t i = 0; OB_SUCC(ret) && i < cnt; ++i) {
          if (OB_FAIL(pq_parse_row_group(c, 1, type, codec, num_values, data_page_offset,
                                         chunk_cnt))) {
            LOG_WARN("failed to read a parquet row group", K(ret), K(i), K(cnt), K(c));
          }
        }
      }
    } else if (OB_FAIL(pq_skip_value(c, head.type_, 1))) {
      LOG_WARN("failed to skip a parquet file meta field", K(ret), K(head), K(c));
    }
  }
  if (OB_FAIL(ret)) {
  } else if (1 != leaf_cnt) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("parquet file does not have exactly one column", K(ret), K(leaf_cnt));
  }
  return ret;
}

struct PqPageHeader
{
  PqPageHeader() : page_type_(0), uncompressed_(0), compressed_(0), num_values_(0),
                   def_levels_byte_length_(0), encoding_(0), is_compressed_(true) {}
  int32_t page_type_;
  int32_t uncompressed_;
  int32_t compressed_;
  int32_t num_values_;
  int32_t def_levels_byte_length_;
  int32_t encoding_;
  bool is_compressed_;
  TO_STRING_KV(K_(page_type), K_(uncompressed), K_(compressed), K_(num_values),
               K_(def_levels_byte_length), K_(encoding), K_(is_compressed));
};

int pq_parse_data_page_header_v2(PqCur &c, const int32_t depth, PqPageHeader &ph)
{
  int ret = OB_SUCCESS;
  int16_t last_id = 0;
  PqFieldHead head;
  bool done = false;
  while (OB_SUCC(ret) && !done) {
    if (OB_FAIL(pq_read_field_head(c, last_id, head))) {
      LOG_WARN("failed to read the parquet data page field header", K(ret), K(c));
    } else if (PQ_STOP == head.type_) {
      done = true;
    } else if (1 == head.id_ && PQ_I32 == head.type_) {
      if (OB_FAIL(pq_read_i32(c, ph.num_values_))) {
        LOG_WARN("failed to read the parquet page value count", K(ret), K(c));
      }
    } else if (4 == head.id_ && PQ_I32 == head.type_) {
      if (OB_FAIL(pq_read_i32(c, ph.encoding_))) {
        LOG_WARN("failed to read the parquet page encoding", K(ret), K(c));
      }
    } else if (5 == head.id_ && PQ_I32 == head.type_) {
      if (OB_FAIL(pq_read_i32(c, ph.def_levels_byte_length_))) {
        LOG_WARN("failed to read the parquet definition level length", K(ret), K(c));
      }
    } else if (7 == head.id_) {
      if (PQ_TRUE == head.type_) {
        ph.is_compressed_ = true;
      } else if (PQ_FALSE == head.type_) {
        ph.is_compressed_ = false;
      } else if (OB_FAIL(pq_skip_value(c, head.type_, depth))) {
        LOG_WARN("failed to skip the parquet page compression flag", K(ret), K(head), K(c));
      }
    } else if (OB_FAIL(pq_skip_value(c, head.type_, depth))) {
      LOG_WARN("failed to skip a parquet data page field", K(ret), K(head), K(c));
    }
  }
  return ret;
}

int pq_parse_page_header(PqCur &c, PqPageHeader &ph)
{
  int ret = OB_SUCCESS;
  int16_t last_id = 0;
  PqFieldHead head;
  bool done = false;
  while (OB_SUCC(ret) && !done) {
    if (OB_FAIL(pq_read_field_head(c, last_id, head))) {
      LOG_WARN("failed to read the parquet page header field header", K(ret), K(c));
    } else if (PQ_STOP == head.type_) {
      done = true;
    } else if (1 == head.id_ && PQ_I32 == head.type_) {
      if (OB_FAIL(pq_read_i32(c, ph.page_type_))) {
        LOG_WARN("failed to read the parquet page type", K(ret), K(c));
      }
    } else if (2 == head.id_ && PQ_I32 == head.type_) {
      if (OB_FAIL(pq_read_i32(c, ph.uncompressed_))) {
        LOG_WARN("failed to read the parquet page size", K(ret), K(c));
      }
    } else if (3 == head.id_ && PQ_I32 == head.type_) {
      if (OB_FAIL(pq_read_i32(c, ph.compressed_))) {
        LOG_WARN("failed to read the parquet compressed page size", K(ret), K(c));
      }
    } else if (8 == head.id_ && PQ_STRUCT == head.type_) {
      if (OB_FAIL(pq_parse_data_page_header_v2(c, 1, ph))) {
        LOG_WARN("failed to read the parquet data page v2 header", K(ret), K(c));
      }
    } else if (OB_FAIL(pq_skip_value(c, head.type_, 1))) {
      LOG_WARN("failed to skip a parquet page header field", K(ret), K(head), K(c));
    }
  }
  return ret;
}

// DATA_PAGE_V2 stores the levels outside the compressed block and without the 4 byte length
// prefix a V1 page carries, so the section is exactly def_levels_byte_length long. The hybrid
// run header is a varint whose low bit says bit packed (groups of 8) or a repeated value.
int pq_decode_def_levels(const char *buf, const int64_t len, const int bit_width,
                         const int64_t num_values, uint8_t *defs, const int64_t def_cnt)
{
  int ret = OB_SUCCESS;
  PqCur c;
  c.buf_ = buf;
  c.len_ = len;
  const uint8_t mask = (0 == bit_width) ? 0 : static_cast<uint8_t>((1u << bit_width) - 1u);
  int64_t filled = 0;
  while (OB_SUCC(ret) && filled < num_values && c.pos_ < c.len_) {
    uint64_t hdr = 0;
    if (OB_FAIL(pq_read_varint(c, hdr))) {
      LOG_WARN("failed to read the parquet level run header", K(ret), K(c));
    } else if (0 == (hdr & 1)) {
      const int64_t run_cnt = static_cast<int64_t>(hdr >> 1);
      uint64_t value = 0;
      if (OB_FAIL(pq_read_varint(c, value))) {
        LOG_WARN("failed to read the parquet level run value", K(ret), K(c));
      } else {
        for (int64_t i = 0; i < run_cnt && filled < num_values; ++i) {
          defs[filled++] = static_cast<uint8_t>(value & mask);
        }
      }
    } else {
      const int64_t groups = static_cast<int64_t>(hdr >> 1);
      const int64_t total = groups * 8;
      const int64_t packed_bytes = (total * bit_width + 7) / 8;
      if (!c.left(packed_bytes)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("parquet bit packed levels truncated", K(ret), K(total), K(bit_width), K(c));
      } else {
        int64_t bitpos = 0;
        for (int64_t i = 0; i < total && filled < num_values; ++i) {
          uint64_t value = 0;
          for (int b = 0; b < bit_width; ++b) {
            const int64_t byte_idx = (bitpos + b) / 8;
            const int64_t bit_idx = (bitpos + b) % 8;
            const uint8_t byte = static_cast<uint8_t>(c.buf_[c.pos_ + byte_idx]);
            value |= (static_cast<uint64_t>((byte >> bit_idx) & 1)) << b;
          }
          defs[filled++] = static_cast<uint8_t>(value & mask);
          bitpos += bit_width;
        }
        c.pos_ += packed_bytes;
      }
    }
  }
  if (OB_FAIL(ret)) {
  } else if (filled != num_values) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("parquet definition levels did not cover the page", K(ret), K(filled),
             K(num_values), K(len));
  }
  return ret;
}

// Only the literal form of Snappy is decoded, which is all a PLAIN int32 block ever turns into.
// A copy tag means the block really compressed, and this reader then refuses the file instead of
// guessing at the back reference window.
int pq_snappy_literals(const char *src, const int64_t src_len, const int64_t expect_len,
                       common::ObIAllocator &alloc, char *&out, int64_t &out_len)
{
  int ret = OB_SUCCESS;
  PqCur c;
  c.buf_ = src;
  c.len_ = src_len;
  out = NULL;
  out_len = 0;
  uint64_t total = 0;
  if (OB_FAIL(pq_read_varint(c, total))) {
    LOG_WARN("failed to read the snappy block length", K(ret), K(src_len));
  } else if (total != static_cast<uint64_t>(expect_len)) {
    ret = OB_ERR_UNEXPECTED;
    LOG_WARN("snappy block length does not match the page", K(ret), "snappy_len", total,
             K(expect_len));
  } else if (total > static_cast<uint64_t>(PQ_MAX_FILE_SIZE)) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("snappy block too large", K(ret), "snappy_len", total);
  } else {
    char *buf = NULL;
    int64_t pos = 0;
    if (NULL == (buf = static_cast<char *>(alloc.alloc(static_cast<int64_t>(total))))) {
      ret = OB_ALLOCATE_MEMORY_FAILED;
      LOG_WARN("failed to alloc the snappy output", K(ret), K(total));
    } else {
      while (OB_SUCC(ret) && c.pos_ < c.len_) {
        const uint8_t tag = static_cast<uint8_t>(c.buf_[c.pos_++]);
        if (0 != (tag & 0x03)) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("snappy copy tags are not supported", K(ret), K(tag), K(c));
        } else {
          int64_t lit_len = (tag >> 2) + 1;
          if (lit_len > 60) {
            const int64_t extra = lit_len - 60;
            lit_len = 0;
            for (int64_t i = 0; OB_SUCC(ret) && i < extra; ++i) {
              if (!c.left(1)) {
                ret = OB_ERR_UNEXPECTED;
                LOG_WARN("snappy literal length truncated", K(ret), K(c));
              } else {
                lit_len |= static_cast<int64_t>(static_cast<uint8_t>(c.buf_[c.pos_++])) << (8 * i);
              }
            }
            ++lit_len;
          }
          if (OB_FAIL(ret)) {
          } else if (!c.left(lit_len) || pos + lit_len > static_cast<int64_t>(total)) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("snappy literal runs past the block", K(ret), K(lit_len), K(pos), K(total));
          } else {
            MEMCPY(buf + pos, c.buf_ + c.pos_, lit_len);
            pos += lit_len;
            c.pos_ += lit_len;
          }
        }
      }
      if (OB_FAIL(ret)) {
      } else if (pos != static_cast<int64_t>(total)) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("snappy block did not fill the page", K(ret), K(pos), K(total));
      } else {
        out = buf;
        out_len = pos;
      }
      if (OB_FAIL(ret) && NULL != buf) {
        alloc.free(buf);
      }
    }
  }
  return ret;
}

} // namespace

struct ObParquetReader::Impl
{
  Impl() : alloc_(NULL), file_buf_(NULL), file_len_(0), values_(NULL), is_null_(NULL),
           value_cnt_(0), cursor_(0), col_() {}
  ~Impl() {}
  int load(common::ObIAllocator &alloc, const common::ObString &path);
  void reset();

  common::ObIAllocator *alloc_;
  char *file_buf_;
  int64_t file_len_;
  int64_t *values_;
  uint8_t *is_null_;
  int64_t value_cnt_;
  int64_t cursor_;
  ColumnSchema col_;
  char col_name_buf_[common::OB_MAX_COLUMN_NAME_BUF_LENGTH];
};

void ObParquetReader::Impl::reset()
{
  if (NULL != alloc_) {
    if (NULL != file_buf_) {
      alloc_->free(file_buf_);
    }
    if (NULL != values_) {
      alloc_->free(values_);
    }
    if (NULL != is_null_) {
      alloc_->free(is_null_);
    }
  }
  alloc_ = NULL;
  file_buf_ = NULL;
  file_len_ = 0;
  values_ = NULL;
  is_null_ = NULL;
  value_cnt_ = 0;
  cursor_ = 0;
}

int ObParquetReader::Impl::load(common::ObIAllocator &alloc, const common::ObString &path)
{
  int ret = OB_SUCCESS;
  alloc_ = &alloc;
  int fd = ::open(path.ptr(), O_RDONLY);
  struct stat st;
  if (fd < 0) {
    ret = OB_FILE_NOT_EXIST;
    LOG_USER_ERROR(OB_FILE_NOT_EXIST);
    LOG_WARN("failed to open the parquet file", K(ret), K(path), "sys_err", errno);
  } else if (0 != ::fstat(fd, &st)) {
    ret = OB_ERR_SYS;
    LOG_WARN("failed to stat the parquet file", K(ret), K(path), "sys_err", errno);
  } else if (st.st_size < PQ_FOOTER_TAIL_SIZE || st.st_size > PQ_MAX_FILE_SIZE) {
    ret = OB_NOT_SUPPORTED;
    LOG_WARN("parquet file size not supported", K(ret), "size", st.st_size);
  } else if (FALSE_IT(file_len_ = st.st_size)) {
  } else if (NULL == (file_buf_ = static_cast<char *>(alloc.alloc(file_len_)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc the parquet file image", K(ret), K_(file_len));
  } else {
    int64_t read_len = 0;
    while (OB_SUCC(ret) && read_len < file_len_) {
      const ssize_t n = ::pread(fd, file_buf_ + read_len, file_len_ - read_len, read_len);
      if (n <= 0) {
        ret = OB_ERR_SYS;
        LOG_WARN("failed to read the parquet file", K(ret), K(read_len), K_(file_len),
                 "sys_err", errno);
      } else {
        read_len += n;
      }
    }
  }
  if (fd >= 0) {
    // the whole image is already in the arena, so the descriptor ends here and every check below
    // runs without one
    ::close(fd);
  }
  if (OB_FAIL(ret)) {
  } else {
    // Both magic words are checked before anything is trusted, so a csv handed to read_parquet
    // fails here rather than as a corrupt thrift walk.
    if (0 != MEMCMP(file_buf_, "PAR1", PQ_MAGIC_SIZE)
        || 0 != MEMCMP(file_buf_ + file_len_ - PQ_MAGIC_SIZE, "PAR1", PQ_MAGIC_SIZE)) {
      ret = OB_INVALID_ARGUMENT;
      LOG_USER_ERROR(OB_INVALID_ARGUMENT, "parquet file format");
      LOG_WARN("parquet magic missing", K(ret), K_(file_len));
    } else {
      const int64_t tail_pos = file_len_ - PQ_FOOTER_TAIL_SIZE;
      const uint8_t *tail = reinterpret_cast<const uint8_t *>(file_buf_ + tail_pos);
      int64_t footer_len = 0;
      for (int64_t i = 0; i < 4; ++i) {
        footer_len |= static_cast<int64_t>(tail[i]) << (8 * i);
      }
      const int64_t footer_pos = file_len_ - PQ_FOOTER_TAIL_SIZE - footer_len;
      PqCur cur;
      ColumnSchema col;
      bool has_col = false;
      int64_t num_rows = 0;
      int32_t phys_type = 0;
      int32_t codec = -1;
      int64_t num_values = 0;
      int64_t data_page_offset = 0;
      int64_t row_group_cnt = 0;
      int64_t chunk_cnt = 0;
      if (footer_len <= 0 || footer_pos < 0) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("broken parquet footer length", K(ret), K(footer_len), K_(file_len));
      } else if (FALSE_IT(cur.buf_ = file_buf_ + footer_pos)) {
      } else if (FALSE_IT(cur.len_ = footer_len)) {
      } else if (OB_FAIL(pq_parse_file_meta(cur, col, has_col, num_rows, phys_type, codec,
                                            num_values, data_page_offset, row_group_cnt,
                                            chunk_cnt))) {
        LOG_WARN("failed to read the parquet footer", K(ret), K(footer_len));
      } else if (1 != row_group_cnt || 1 != chunk_cnt) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("parquet with several row groups or columns not supported", K(ret),
                 K(row_group_cnt), K(chunk_cnt));
      } else if (PQ_TYPE_INT32 != phys_type) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("parquet physical type not supported", K(ret), K(phys_type));
      } else if (PQ_CODEC_UNCOMPRESSED != codec && PQ_CODEC_SNAPPY != codec) {
        ret = OB_NOT_SUPPORTED;
        LOG_WARN("parquet codec not supported", K(ret), K(codec));
      } else if (num_values < 0 || data_page_offset <= 0
                 || data_page_offset >= file_len_) {
        ret = OB_ERR_UNEXPECTED;
        LOG_WARN("broken parquet column chunk", K(ret), K(num_values), K(data_page_offset),
                 K_(file_len));
      } else {
        col_ = col;
        if (col.name_.length() > 0
            && col.name_.length() <= static_cast<int32_t>(sizeof(col_name_buf_) - 1)) {
          MEMCPY(col_name_buf_, col.name_.ptr(), col.name_.length());
          col_.name_.assign_ptr(col_name_buf_, col.name_.length());
        }
        col_.nullable_ = col.nullable_;
        col_.type_ = ObIntType;
        PqCur ph_cur;
        ph_cur.buf_ = file_buf_ + data_page_offset;
        ph_cur.len_ = file_len_ - data_page_offset;
        PqPageHeader ph;
        if (OB_FAIL(pq_parse_page_header(ph_cur, ph))) {
          LOG_WARN("failed to read the parquet page header", K(ret), K(data_page_offset));
        } else if (PQ_PAGE_DATA_PAGE_V2 != ph.page_type_) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("parquet page type not supported", K(ret), K(ph));
        } else if (PQ_ENCODING_PLAIN != ph.encoding_) {
          ret = OB_NOT_SUPPORTED;
          LOG_WARN("parquet page encoding not supported", K(ret), K(ph));
        } else if (ph.num_values_ != num_values
                   // The levels sit outside the compressed block, and snappy may make the data it
                   // replaces bigger rather than smaller, so each leg is measured on its own.
                   || ph.compressed_ <= ph.def_levels_byte_length_
                   || ph.uncompressed_ <= ph.def_levels_byte_length_) {
          ret = OB_ERR_UNEXPECTED;
          LOG_WARN("broken parquet page header", K(ret), K(ph), K(num_values));
        } else {
          const int64_t body_pos = data_page_offset + ph_cur.pos_;
          const int64_t levels_len = ph.def_levels_byte_length_;
          const int64_t data_pos = body_pos + levels_len;
          const int64_t data_len = ph.compressed_ - levels_len;
          if (body_pos + ph.compressed_ > file_len_) {
            ret = OB_ERR_UNEXPECTED;
            LOG_WARN("parquet page runs past the file", K(ret), K(body_pos), K(ph.compressed_),
                     K_(file_len));
          } else {
            const int64_t expect_plain = ph.uncompressed_ - levels_len;
            char *plain = NULL;
            int64_t plain_len = 0;
            if (ph.is_compressed_ && PQ_CODEC_SNAPPY == codec) {
              if (OB_FAIL(pq_snappy_literals(file_buf_ + data_pos, data_len, expect_plain, alloc,
                                             plain, plain_len))) {
                LOG_WARN("failed to uncompress the parquet page", K(ret), K(data_len),
                         K(expect_plain));
              }
            } else if (expect_plain != data_len) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("uncompressed parquet page size mismatch", K(ret), K(data_len),
                       K(expect_plain));
            } else if (NULL == (plain = static_cast<char *>(alloc.alloc(data_len)))) {
              ret = OB_ALLOCATE_MEMORY_FAILED;
              LOG_WARN("failed to alloc the parquet page", K(ret), K(data_len));
            } else {
              MEMCPY(plain, file_buf_ + data_pos, data_len);
              plain_len = data_len;
            }
            if (OB_FAIL(ret)) {
            } else if (0 != plain_len % static_cast<int64_t>(sizeof(int32_t))) {
              ret = OB_ERR_UNEXPECTED;
              LOG_WARN("parquet plain int32 block is not a whole number of values", K(ret),
                       K(plain_len));
            } else {
              const int64_t plain_cnt = plain_len / static_cast<int64_t>(sizeof(int32_t));
              uint8_t *defs = NULL;
              if (num_values > 0
                  && NULL == (defs = static_cast<uint8_t *>(alloc.alloc(num_values)))) {
                ret = OB_ALLOCATE_MEMORY_FAILED;
                LOG_WARN("failed to alloc the parquet definition levels", K(ret), K(num_values));
              } else if (col_.nullable_) {
                if (levels_len <= 0) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("parquet optional column has no levels", K(ret), K(ph));
                } else if (OB_FAIL(pq_decode_def_levels(file_buf_ + body_pos, levels_len, 1,
                                                        num_values, defs, num_values))) {
                  LOG_WARN("failed to decode the parquet definition levels", K(ret), K(ph));
                }
              } else {
                for (int64_t i = 0; i < num_values; ++i) {
                  defs[i] = 1;
                }
              }
              if (OB_FAIL(ret)) {
              } else {
                int64_t present = 0;
                for (int64_t i = 0; i < num_values; ++i) {
                  present += (1 == defs[i]) ? 1 : 0;
                }
                if (present != plain_cnt) {
                  ret = OB_ERR_UNEXPECTED;
                  LOG_WARN("parquet value count does not match the levels", K(ret), K(present),
                            K(plain_cnt), K(num_values));
                } else if (NULL == (values_ = static_cast<int64_t *>(
                               alloc.alloc(num_values * sizeof(int64_t))))) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  LOG_WARN("failed to alloc the parquet values", K(ret), K(num_values));
                } else if (NULL == (is_null_ = static_cast<uint8_t *>(
                               alloc.alloc(num_values)))) {
                  ret = OB_ALLOCATE_MEMORY_FAILED;
                  LOG_WARN("failed to alloc the parquet null flags", K(ret), K(num_values));
                } else {
                  const int32_t *plain_values = reinterpret_cast<const int32_t *>(plain);
                  for (int64_t i = 0, j = 0; i < num_values; ++i) {
                    if (1 == defs[i]) {
                      values_[i] = static_cast<int64_t>(plain_values[j++]);
                      is_null_[i] = 0;
                    } else {
                      values_[i] = 0;
                      is_null_[i] = 1;
                    }
                  }
                  value_cnt_ = num_values;
                  cursor_ = 0;
                  alloc.free(plain);
                  alloc.free(defs);
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

ObParquetReader::ObParquetReader(common::ObIAllocator &alloc) : impl_(NULL), alloc_(alloc)
{
}

ObParquetReader::~ObParquetReader()
{
  close();
}

int ObParquetReader::open(const common::ObString &path)
{
  int ret = OB_SUCCESS;
  // a reader is never reused for a second file, so an open always starts from an empty one and a
  // rescan reloads the image instead of stacking a second copy of it on the arena
  close();
  void *buf = NULL;
  if (path.empty()) {
    ret = OB_INVALID_ARGUMENT;
    LOG_WARN("empty parquet path", K(ret));
  } else if (NULL == (buf = alloc_.alloc(sizeof(Impl)))) {
    ret = OB_ALLOCATE_MEMORY_FAILED;
    LOG_WARN("failed to alloc the parquet reader", K(ret));
  } else if (FALSE_IT(impl_ = new (buf) Impl())) {
  } else if (OB_FAIL(impl_->load(alloc_, path))) {
    LOG_WARN("failed to load the parquet file", K(ret), K(path));
    close();
  }
  return ret;
}

int ObParquetReader::get_schema(common::ObIArray<ColumnSchema> &schema) const
{
  int ret = OB_SUCCESS;
  schema.reset();
  if (NULL == impl_ || NULL == impl_->values_) {
    ret = OB_NOT_INIT;
    LOG_WARN("the parquet reader was not opened", K(ret));
  } else if (OB_FAIL(schema.push_back(impl_->col_))) {
    LOG_WARN("failed to add the parquet column", K(ret), K(impl_->col_));
  }
  return ret;
}

int ObParquetReader::get_next_value(common::ObObj &value)
{
  int ret = OB_SUCCESS;
  value.set_null();
  if (NULL == impl_ || NULL == impl_->values_) {
    ret = OB_NOT_INIT;
    LOG_WARN("the parquet reader was not opened", K(ret));
  } else if (impl_->cursor_ >= impl_->value_cnt_) {
    ret = OB_ITER_END;
  } else {
    const int64_t idx = impl_->cursor_++;
    if (1 == impl_->is_null_[idx]) {
      value.set_null();
    } else {
      value.set_int(impl_->values_[idx]);
    }
  }
  return ret;
}

void ObParquetReader::close()
{
  if (NULL != impl_) {
    impl_->reset();
    impl_->~Impl();
    alloc_.free(impl_);
    impl_ = NULL;
  }
}

} // namespace sql
} // namespace oceanbase
