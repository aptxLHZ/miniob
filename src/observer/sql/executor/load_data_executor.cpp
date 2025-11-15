/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

//
// Created by Wangyunlai on 2023/7/12.
//

#include "sql/executor/load_data_executor.h"
#include "common/lang/string.h"
#include "event/session_event.h"
#include "event/sql_event.h"
#include "sql/executor/sql_result.h"
#include "sql/stmt/load_data_stmt.h"
#include "storage/common/chunk.h"

using namespace common;

// 解析单行CSV，支持 enclosed_by 字符
static void parse_csv_line(const string& line, char terminated, char enclosed, vector<string>& fields) {
    fields.clear();
    if (line.empty()) {
        return;
    }

    stringstream ss;
    bool in_quotes = false;
    for (size_t i = 0; i < line.length(); ++i) {
        char c = line[i];
        if (c == enclosed) {
            // 处理转义的引号，例如 "abc""def"
            if (in_quotes && i + 1 < line.length() && line[i+1] == enclosed) {
                ss << c;
                i++; // 跳过下一个引号
            } else {
                in_quotes = !in_quotes;
            }
        } else if (c == terminated && !in_quotes) {
            fields.push_back(ss.str());
            ss.str("");
            ss.clear();
        } else {
            ss << c;
        }
    }
    fields.push_back(ss.str());
}


// 将字符串字段序列化到内存缓冲区
static RC serialize_pax_record(const TableMeta& meta, const vector<string>& file_values, char* record_buffer) {
    int current_offset = 0;
    const int sys_field_num = meta.sys_field_num();
    const int user_field_num = meta.field_num() - sys_field_num;

    for (int i = 0; i < user_field_num; ++i) {
        const FieldMeta *field_meta = meta.field(i + sys_field_num);
        string value_str = file_values[i];
        common::strip(value_str); // 去掉首尾空格

        char* dest = record_buffer + current_offset;

        try {
            switch (field_meta->type()) {
                case AttrType::INTS: {
                    int val = value_str.empty() ? 0 : std::stoi(value_str);
                    memcpy(dest, &val, sizeof(val));
                    break;
                }
                case AttrType::FLOATS: {
                    float val = value_str.empty() ? 0.0f : std::stof(value_str);
                    memcpy(dest, &val, sizeof(val));
                    break;
                }
                case AttrType::CHARS: {
                    strncpy(dest, value_str.c_str(), field_meta->len());
                    size_t copied_len = value_str.length();
                    if (copied_len < (size_t)field_meta->len()) {
                        memset(dest + copied_len, 0, field_meta->len() - copied_len);
                    }
                    break;
                }
                default:
                    return RC::UNSUPPORTED;
            }
        } catch (const std::exception& e) {
            LOG_WARN("Failed to convert string '%s' for column %s. Error: %s", value_str.c_str(), field_meta->name(), e.what());
            return RC::INVALID_ARGUMENT;
        }
        current_offset += field_meta->len();
    }
    return RC::SUCCESS;
}

RC LoadDataExecutor::execute(SQLStageEvent *sql_event)
{
  RC            rc         = RC::SUCCESS;
  SqlResult    *sql_result = sql_event->session_event()->sql_result();
  LoadDataStmt *stmt       = static_cast<LoadDataStmt *>(sql_event->stmt());
  Table        *table      = stmt->table();
  const char   *file_name  = stmt->filename();
  load_data(table, file_name, stmt->terminated(), stmt->enclosed(), sql_result);
  return rc;
}

/**
 * 从文件中导入数据时使用。尝试向表中插入解析后的一行数据。
 * @param table  要导入的表
 * @param file_values 从文件中读取到的一行数据，使用分隔符拆分后的几个字段值
 * @param record_values Table::insert_record使用的参数，为了防止频繁的申请内存
 * @param errmsg 如果出现错误，通过这个参数返回错误信息
 * @return 成功返回RC::SUCCESS
 */
RC insert_record_from_file(
    Table *table, vector<string> &file_values, vector<Value> &record_values, stringstream &errmsg)
{

  const int field_num     = record_values.size();
  const int sys_field_num = table->table_meta().sys_field_num();

  if (file_values.size() < record_values.size()) {
    return RC::SCHEMA_FIELD_MISSING;
  }

  RC rc = RC::SUCCESS;

  stringstream deserialize_stream;
  for (int i = 0; i < field_num && RC::SUCCESS == rc; i++) {
    const FieldMeta *field = table->table_meta().field(i + sys_field_num);

    string &file_value = file_values[i];
    if (field->type() != AttrType::CHARS) {
      common::strip(file_value);
    }
    rc = DataType::type_instance(field->type())->set_value_from_str(record_values[i], file_value);
    if (rc != RC::SUCCESS) {
      LOG_WARN("Failed to deserialize value from string: %s, type=%d", file_value.c_str(), field->type());
      return rc;
    }
  }

  if (RC::SUCCESS == rc) {
    Record record;
    rc = table->make_record(field_num, record_values.data(), record);
    if (rc != RC::SUCCESS) {
      errmsg << "insert failed.";
    } else if (RC::SUCCESS != (rc = table->insert_record(record))) {
      errmsg << "insert failed.";
    }
  }
  return rc;
}


// TODO: pax format and row format
void LoadDataExecutor::load_data(Table *table, const char *file_name, char terminated, char enclosed, SqlResult *sql_result)
{
  stringstream result_string;

  fstream fs;
  fs.open(file_name, ios_base::in); // 以文本模式打开文件
  if (!fs.is_open()) {
    result_string << "Failed to open file: " << file_name << ". system error=" << strerror(errno) << endl;
    sql_result->set_return_code(RC::FILE_NOT_EXIST);
    sql_result->set_state_string(result_string.str());
    return;
  }
  
  // 这里定义 meta 变量，后续代码就可以安全使用它
  const TableMeta& meta = table->table_meta(); 
  const int sys_field_num = meta.sys_field_num();
  const int user_field_num = meta.field_num() - sys_field_num;

  string line;
  vector<string> file_values;
  int line_num = 0;
  int insertion_count = 0;
  RC rc = RC::SUCCESS;

  while (getline(fs, line)) {
    line_num++;
    if (common::is_blank(line.c_str())) {
        continue;
    }
    
    // 使用我们自己的解析函数，处理分隔符和包围符
    parse_csv_line(line, terminated, enclosed, file_values);

    if (meta.storage_format() == StorageFormat::ROW_FORMAT) {
      // 保持已有的行存逻辑不变
      vector<Value> record_values(user_field_num);
      stringstream errmsg;
      RC line_rc = insert_record_from_file(table, file_values, record_values, errmsg);
      if (line_rc != RC::SUCCESS) {
          result_string << "Line:" << line_num << " insert record failed:" << errmsg.str() << ". error:" << strrc(line_rc)
                        << endl;
      } else {
          insertion_count++;
      }
    } else if (meta.storage_format() == StorageFormat::PAX_FORMAT) {
      // PAX 格式的导入逻辑
      if (file_values.size() != (size_t)user_field_num) {
          result_string << "Line:" << line_num << " field count mismatch. Expected " << user_field_num << ", but got " << file_values.size() << ". Skipping line." << endl;
          continue; // 字段数量不匹配，跳过此行
      }

      // 计算用户字段占用的总记录大小
      int user_record_size = 0;
      for (int i = 0; i < user_field_num; ++i) {
          user_record_size += meta.field(i + sys_field_num)->len();
      }
      
      // 使用 alloca 在栈上分配缓冲区，避免堆分配的开销和忘记释放的问题
      char* record_buffer = (char*)alloca(user_record_size);

      // 1. 将字符串字段序列化到 record_buffer
      RC line_rc = serialize_pax_record(meta, file_values, record_buffer);
      if (line_rc != RC::SUCCESS) {
          result_string << "Line:" << line_num << " serialize failed. error:" << strrc(line_rc) << ". Skipping line." << endl;
          continue;
      }
      
      // 2. 构造 Record 对象并插入
      Record record;
      record.set_data(record_buffer, user_record_size); 
      line_rc = table->insert_record(record);

      if (line_rc != RC::SUCCESS) {
          result_string << "Line:" << line_num << " insert record failed. error:" << strrc(line_rc) << ". Skipping line." << endl;
          continue;
      } else {
          insertion_count++;
      }
    } else {
      rc = RC::UNSUPPORTED;
      result_string << "Unsupported storage format: " << strrc(rc) << endl;
      break; // 不支持的格式，直接退出循环
    }
  }

  fs.close();
  
  if (rc == RC::SUCCESS) {
    result_string << insertion_count << " rows affected.";
  }
  
  LOG_INFO("load data done. row num: %d, final result: %s", insertion_count, strrc(rc));
  sql_result->set_return_code(rc);
  sql_result->set_state_string(result_string.str());
}
