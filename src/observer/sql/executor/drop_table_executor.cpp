/* Copyright (c) 2021 OceanBase and/or its affiliates. All rights reserved.
miniob is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sql/executor/drop_table_executor.h"
#include "event/sql_event.h"
#include "event/session_event.h"
#include "session/session.h"
#include "sql/stmt/drop_table_stmt.h"
#include "storage/db/db.h"
#include "common/log/log.h"
// 🎯 确保包含 stdio.h 或 cstdio 以使用 fprintf
#include <cstdio> 

RC DropTableExecutor::execute(SQLStageEvent *sql_event)
{
    // 🎯 DEBUG LOG: Executor 入口点
    fprintf(stderr, "DEBUG_DROP: Entering DropTableExecutor::execute\n");
    
    RC rc = RC::SUCCESS;
    SessionEvent *session_event = sql_event->session_event();
    Session *session = session_event->session(); // 🎯 获取 Session 对象

    // 获取当前数据库对象
    Db *db = session->get_current_db(); 
    // 转换为 DropTableStmt 
    DropTableStmt *drop_table_stmt = dynamic_cast<DropTableStmt *>(sql_event->stmt());
    
    if (drop_table_stmt == nullptr) {
        LOG_ERROR("stmt type not match DropTableStmt");
        return RC::INTERNAL;
    }
    
    const char* table_name_str = drop_table_stmt->table_name().c_str(); // 简化代码

    // 1. 调用 Db 类的 drop_table 方法执行删除操作
    fprintf(stderr, "DEBUG_DROP: Calling db->drop_table(%s)\n", table_name_str);
    rc = db->drop_table(table_name_str);

    if (RC::SUCCESS == rc) {
        LOG_INFO("drop table success, table name %s", table_name_str);
        
        // 2. 提交事务 (COMMIT) 🎯 关键逻辑
        RC commit_rc = session->commit(); 
        
        if (RC::SUCCESS != commit_rc) {
            // 提交失败是导致客户端返回 FAILURE 的常见原因！
            // 🎯 使用 fprintf 强制打印错误码
            fprintf(stderr, "EXECUTOR_COMMIT_FAILED: table=%s, commit_rc=%d\n", 
                            table_name_str, (int)commit_rc);
            LOG_ERROR("Commit failed after dropping table %s. rc=%d", 
                      table_name_str, commit_rc);
            rc = commit_rc; // 用提交失败的错误码覆盖，返回给客户端
        } else {
            fprintf(stderr, "DEBUG_DROP: Transaction committed successfully.\n");
        }
    } else {
        // 3. 如果 Db::drop_table() 失败
        // 🎯 打印 Db 层的错误码
        fprintf(stderr, "EXECUTOR_DB_FAILED: table=%s, rc=%d\n", table_name_str, (int)rc);
        LOG_WARN("drop table failed, table name %s, rc=%d", table_name_str, rc);
    }

    // 返回 Db::drop_table 的错误码，或者 Commit/Rollback 的错误码
    fprintf(stderr, "DEBUG_DROP: DropTableExecutor returning rc=%d\n", (int)rc);
    return rc;
}