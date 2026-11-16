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

#ifndef OB_PARQUET_READER_H
#define OB_PARQUET_READER_H
#include "lib/ob_define.h"
#include "lib/string/ob_string.h"
#include "lib/allocator/ob_allocator.h"
#include "lib/container/ob_se_array.h"
#include "common/object/ob_object.h"
namespace oceanbase {
namespace sql {
// Minimal Parquet reader for file_sql fixtures.
// Supports: single row group, INT32 PLAIN, DATA_PAGE_V2, SNAPPY literal-only.
// Anything else returns OB_NOT_SUPPORTED.
class ObParquetReader {
public:
  struct ColumnSchema {
    ColumnSchema() : name_(), type_(common::ObNullType), nullable_(false) {}
    common::ObString name_;
    common::ObObjType type_;
    bool nullable_;
    TO_STRING_KV(K_(name), K_(type), K_(nullable));
  };
  explicit ObParquetReader(common::ObIAllocator &alloc);
  ~ObParquetReader();
  int open(const common::ObString &path);
  int get_schema(common::ObIArray<ColumnSchema> &schema) const;
  int get_next_value(common::ObObj &value);
  void close();
private:
  struct Impl;
  Impl *impl_;
  common::ObIAllocator &alloc_;
};
}  // namespace sql
}  // namespace oceanbase
#endif
