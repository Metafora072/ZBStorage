#pragma once

/**
 * @file error_codes.h
 * @brief 错误码定义
 */

namespace volumemanager {

/**
 * @brief 错误码枚举
 */
enum class ErrorCode {
    SUCCESS = 0,           // 成功
    FILE_NOT_FOUND,        // 文件未找到
    INVALID_PATH,          // 无效路径
    VOLUME_FULL,           // 卷镜像已满
    VOLUME_NOT_FOUND,      // 卷镜像未找到
    VOLUME_ALREADY_EXISTS, // image_dir_ 中已有同名 volume_id 的镜像（MoveFrom 专用）
    INVALID_VOLUME_FORMAT, // 无效的卷镜像格式
    INODE_NOT_FOUND,       // inode未找到
    SERIALIZATION_ERROR,   // 序列化/反序列化错误
    IO_ERROR,              // IO错误
    OUT_OF_MEMORY,         // 内存不足
    INVALID_VOLUME_ID,     // 无效的卷镜像ID
    READ_DELAY,            // 读取延后
    TASK_NOT_FOUND,        //任务未找到
    TASK_NOT_FINISH,       //任务未完成
    TASK_ALREADY_FINISH,   //任务已结束（FINISH 终态，读产物已消费）
    INVALID_PARAMETER,     // 无效参数
    MANAGER_NOT_READY,     // 管理器未处于 RUNNING 状态（WriteObject / ReadFile / ReadObjectByTaskId 调用前置守卫使用）
    VOLUME_FULL_NO_READABLE, // image_dir_ 已满且没有 READ 镜像可换出（MoveFrom 专用）
    // ----- 任务阶段失败错误码（TODO-04）-----
    // 这些错误码用于把"任务在某一阶段失败"显式暴露给上层，避免 ZipTaskProcessor /
    // CDBurnTaskProcessor / OnCDReadComplete / OnCDBurnComplete 静默 continue。
    // 上层可通过 WRTask::last_error_code 直接匹配这些枚举做精确告警。
    READ_FAILED,            // 读阶段失败：MountVolume 异常 / cd_manager 读提交失败 / inode 无法解析
    WRITE_FAILED,           // 写任务前置失败：inode 解析失败 / 任务状态异常
    COMPRESS_FAILED,        // AddFileToCollect 返回非 SUCCESS（写入失败 / 编码失败）
    PACK_FAILED,            // ShouldPackVolume 后 PackVolume 返回非 SUCCESS（镜像封装失败）
    BURN_FAILED             // SubmitBurnTask 返回 false / cd_manager 刻录回调报错
};

/**
 * @brief 将错误码转换为描述字符串
 * @param code 错误码
 * @return 错误描述字符串
 */
const char* GetErrorMessage(ErrorCode code);

} // namespace volumemanager
