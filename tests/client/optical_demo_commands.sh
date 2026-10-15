# Sourced by a generated demo-env.sh. Requires Bash; never changes caller shell options/cwd.
# Remove the previous demo's helper when re-sourcing in an existing shell.
unset -f zb_release

zb_stage() { "$ZB_DEMO_CTL" --socket "$ZB_DEMO_SOCKET" stage "$@"; }

_zb_demo_file() {
    if [[ $# -gt 1 ]]; then echo '用法：zb_demo [文件名]，默认 large.bin' >&2; return 2; fi
    local name="${1:-large.bin}"
    case "$name" in
        large.bin|another.bin|hello.txt|empty.bin) printf '%s\n' "$name" ;;
        *) echo '文件名请选择 large.bin、another.bin、hello.txt 或 empty.bin' >&2; return 2 ;;
    esac
}

zb_prepare() {
    local name handle
    name=$(_zb_demo_file "$@") || return
    handle=$(zb_stage submit "$ZB_DEMO_MOUNT/$name") || return
    echo "准备请求已受理：$name"
    echo "任务：$handle"
    echo '本命令立即返回；请在启动终端查看就绪通知，收到通知后再 cat 文件。'
}

zb_read() {
    local name
    name=$(_zb_demo_file "$@") || return
    echo "通过 POSIX 读取 $name（cat），并与原始文件比较（cmp）……"
    cat "$ZB_DEMO_MOUNT/$name" > /dev/null || return
    cmp "$ZB_DEMO_MOUNT/$name" "$ZB_DEMO_EXPECTED/$name" || return
    echo 'PASS：完整读取成功，文件内容完全一致。'
    if [[ $name == hello.txt ]]; then cat "$ZB_DEMO_MOUNT/$name"; fi
}

_zb_demo_probe() {
    # Translate only EAGAIN into an expected demonstration result. Other failures stay failures.
    python3 - "$ZB_DEMO_MOUNT/$1" <<'PY'
import errno
import os
import sys
try:
    fd = os.open(sys.argv[1], os.O_RDONLY)
except OSError as exc:
    if exc.errno != errno.EAGAIN:
        raise
    print('文件尚未准备，POSIX open 返回 EAGAIN——这是正常的准备前状态。', flush=True)
else:
    os.close(fd)
    print('文件已处于准备完成状态。', flush=True)
PY
}

zb_demo() {
    local name
    name=$(_zb_demo_file "$@") || return
    ls -lh "$ZB_DEMO_MOUNT/$name" || return
    _zb_demo_probe "$name" || return
    zb_prepare "$name" || return
    echo "收到就绪通知后执行：cat \"\$ZB_DEMO_MOUNT/$name\""
}
