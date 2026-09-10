#include "error_codes.h"

namespace volumemanager {

const char* GetErrorMessage(ErrorCode code) {
    switch (code) {
        case ErrorCode::SUCCESS:
            return "操作成功";
        case ErrorCode::FILE_NOT_FOUND:
            return "文件未找到";
        case ErrorCode::INVALID_PATH:
            return "无效路径";
        case ErrorCode::VOLUME_FULL:
            return "卷镜像已满";
        case ErrorCode::VOLUME_NOT_FOUND:
            return "卷镜像未找到";
        case ErrorCode::VOLUME_ALREADY_EXISTS:
            return "image_dir中已有同名卷镜像";
        case ErrorCode::INVALID_VOLUME_FORMAT:
            return "无效的卷镜像格式";
        case ErrorCode::INODE_NOT_FOUND:
            return "inode未找到";
        case ErrorCode::SERIALIZATION_ERROR:
            return "序列化/反序列化错误";
        case ErrorCode::IO_ERROR:
            return "IO错误";
        case ErrorCode::OUT_OF_MEMORY:
            return "内存不足";
        case ErrorCode::INVALID_VOLUME_ID:
            return "无效的卷镜像ID";
        case ErrorCode::READ_DELAY:
            return "异步读取";
        case ErrorCode::TASK_NOT_FOUND:
            return "任务未找到";
        case ErrorCode::TASK_NOT_FINISH:
            return "任务未完成";
        case ErrorCode::TASK_ALREADY_FINISH:
            return "任务已结束";
        case ErrorCode::INVALID_PARAMETER:
            return "无效参数";
        case ErrorCode::MANAGER_NOT_READY:
            return "管理器未处于RUNNING状态";
        case ErrorCode::VOLUME_FULL_NO_READABLE:
            return "image_dir已满且无可换出的读镜像";
        case ErrorCode::READ_FAILED:
            return "读阶段失败";
        case ErrorCode::WRITE_FAILED:
            return "写任务前置失败";
        case ErrorCode::COMPRESS_FAILED:
            return "压缩阶段失败";
        case ErrorCode::PACK_FAILED:
            return "封装阶段失败";
        case ErrorCode::BURN_FAILED:
            return "刻录阶段失败";
        default:
            return "未知错误";
    }
}

} // namespace volumemanager
