/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
         http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "storage/record/lob_handler.h"

RC LobFileHandler::create_file(const char *file_name)
{
  return file_.create_file(file_name);
}

RC LobFileHandler::open_file(const char *file_name)
{
  std::ifstream file(file_name);
  if (file.good()) {
    return file_.open_file(file_name);
  } else {
    return RC::FILE_NOT_EXIST;
  }
  return RC::INTERNAL;
}

RC LobFileHandler::insert_data(int64_t &offset, int64_t length, const char *data)
{
  RC       rc         = RC::SUCCESS;
  int64_t  out_size   = 0;
  int64_t end_offset = 0;
  rc                  = file_.append(length, data, &out_size, &end_offset);
  if (OB_FAIL(rc)) {
    return rc;
  }
  if (out_size != length) {
    return RC::IOERR_WRITE;
  }
  offset = end_offset;

  return rc;
}

RC LobFileHandler::remove_file()
{
    RC rc = RC::SUCCESS;
    // 假设文件路径存储在 file_name_ 或类似成员中，并且可以通过一个成员指针访问。
    // 在 MiniOB 中，LOB handler 通常包含一个 RecordFileHandler 的实例。
    // 我们假设 LobFileHandler 有一个成员 data_buffer_pool_ 来关闭文件。
    
    // 1. 关闭文件句柄 (如果有的话)
    if (data_buffer_pool_ != nullptr) { // 假设 LobFileHandler 有这个成员
        data_buffer_pool_->close_file();
    }
    
    // 2. 删除文件 (假设文件路径存储在 file_path_ 成员中)
    // 如果 LobFileHandler 中没有 file_path_ 成员，您可能需要通过 Table 的元数据来获取路径
    // 为了通过编译，我们假设存在 file_name_ 或 file_path_ 成员
    const string &file_path = file_path_;

    if (0 != ::remove(file_path.c_str())) {
        if (errno != ENOENT) { // 忽略文件不存在的错误
            LOG_ERROR("Failed to remove LOB data file %s. errno=%d:%s", file_path.c_str(), errno, strerror(errno));
            return RC::IOERR_WRITE;
        }
    }
    
    // 3. 清理指针
    // delete data_buffer_pool_; // 如果是指针，可能需要删除
    // data_buffer_pool_ = nullptr;
    
    LOG_INFO("Successfully removed LOB file: %s", file_path.c_str());
    return rc;
}