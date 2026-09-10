#pragma once

// 集中声明 OpticalNodeManager 用到的状态 / 阶段字符串常量。
// 字符串值需保持稳定，上层可直接做 == 比较或拼入日志 / RPC 返回。
namespace optical_node_manager {

namespace manager_status {

// Run() 尚未调用。
inline constexpr const char* kUninitialized = "UNINITIALIZED";

// Run() 已进入，正在初始化目录与组件。
inline constexpr const char* kInitializing  = "INITIALIZING";

// 启动失败；对外接口应拒绝服务，等待上层销毁或修复后重建。
inline constexpr const char* kStartFailed   = "START_FAILED";

// 所有后台线程与 cd_manager 已就绪，可对外提供服务。
inline constexpr const char* kRunning      = "RUNNING";

// 已请求停止，等待后台线程退出。
inline constexpr const char* kStopping     = "STOPPING";

// 停止流程已结束；可再次 Run() 重新初始化。
inline constexpr const char* kStopped      = "STOPPED";

}  // namespace manager_status

namespace start_failure_phase {

// 启动失败 / 运行期失败对应的 phase 字符串，仅用于 GetStatusDetail / 日志分流。
inline constexpr const char* kRunEntry      = "run_entry";
inline constexpr const char* kInitializeDir = "InitializeDir";
inline constexpr const char* kStartWorkers  = "StartBackgroundWorkers";

// 三个后台线程创建失败的细分 phase。
inline constexpr const char* kThreadRead    = "StartBackgroundWorkers.thread=read";
inline constexpr const char* kThreadZip     = "StartBackgroundWorkers.thread=zip";
inline constexpr const char* kThreadBurn    = "StartBackgroundWorkers.thread=burn";

// 运行期链路 phase：上层按字段做 == 比较时使用这些常量。
inline constexpr const char* kRunGuard          = "RunGuard";
inline constexpr const char* kReadFile          = "ReadFile";
inline constexpr const char* kReadObjectByTaskId = "ReadObjectByTaskId";
inline constexpr const char* kReadObjectByInodeId = "ReadObjectByInodeId";
inline constexpr const char* kWriteObject       = "WriteObject";
inline constexpr const char* kOnCDReadComplete  = "OnCDReadComplete";
inline constexpr const char* kOnCDBurnComplete  = "OnCDBurnComplete";
inline constexpr const char* kZipTaskProcessor  = "ZipTaskProcessor";

}  // namespace start_failure_phase

}  // namespace optical_node_manager
