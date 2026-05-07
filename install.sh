#!/bin/bash
# StreamVault 安装/更新脚本（无需 root）
# 适用于 Ubuntu 20.04 / 22.04 / 24.04

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}"
echo "  ╔══════════════════════════════╗"
echo "  ║   StreamVault 安装/更新      ║"
echo "  ║   M3U8 视频下载管理器        ║"
echo "  ╚══════════════════════════════╝"
echo -e "${NC}"

INSTALL_DIR="$HOME/streamvault"
VIDEO_DIR="$INSTALL_DIR/videos"
DATA_FILE="$INSTALL_DIR/data.json"
PORT=8080
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo -e "${YELLOW}[1/4] 创建目录...${NC}"
mkdir -p "$INSTALL_DIR"
mkdir -p "$VIDEO_DIR"
mkdir -p /tmp/m3u8dl

echo -e "${YELLOW}[2/4] 安装 Python 依赖...${NC}"
python3 -m venv "$INSTALL_DIR/venv"
"$INSTALL_DIR/venv/bin/pip" install -q fastapi "uvicorn[standard]" python-multipart httpx yt-dlp

echo -e "${YELLOW}[3/4] 更新程序文件...${NC}"
cp "$SCRIPT_DIR/backend/main.py" "$INSTALL_DIR/main.py"
cp "$SCRIPT_DIR/frontend/index.html" "$INSTALL_DIR/index.html"
cp "$SCRIPT_DIR/frontend/public.html" "$INSTALL_DIR/public.html"

if [ ! -f "$DATA_FILE" ]; then
  echo '{"api_sources": [], "subscriptions": {}, "settings": {"check_interval": 3600, "password_hash": ""}}' > "$DATA_FILE"
  echo -e "  ${GREEN}首次安装，已创建空配置文件${NC}"
else
  echo -e "  ${GREEN}检测到已有配置，保留 data.json（接口、订阅不丢失）${NC}"
fi

echo -e "${YELLOW}[4/4] 配置并启动服务...${NC}"

USE_SYSTEMD=false
if command -v systemctl &>/dev/null && systemctl --user status &>/dev/null 2>&1; then
  mkdir -p "$HOME/.config/systemd/user"
  cat > "$HOME/.config/systemd/user/streamvault.service" << EOF
[Unit]
Description=StreamVault M3U8 Manager
After=network.target

[Service]
Type=simple
WorkingDirectory=$INSTALL_DIR
ExecStart=$INSTALL_DIR/venv/bin/uvicorn main:app --host 0.0.0.0 --port $PORT
Restart=always
RestartSec=5

[Install]
WantedBy=default.target
EOF
  systemctl --user daemon-reload
  systemctl --user enable streamvault
  systemctl --user restart streamvault
  USE_SYSTEMD=true
else
  # systemd 用户服务不可用时，用 nohup 后台运行
  pkill -f "uvicorn main:app" 2>/dev/null || true
  sleep 1
  nohup "$INSTALL_DIR/venv/bin/uvicorn" main:app \
    --host 0.0.0.0 --port "$PORT" \
    --app-dir "$INSTALL_DIR" \
    > "$INSTALL_DIR/streamvault.log" 2>&1 &
  echo $! > "$INSTALL_DIR/streamvault.pid"
  echo -e "  ${GREEN}已在后台启动（PID: $!）${NC}"
fi

SERVER_IP=$(hostname -I 2>/dev/null | awk '{print $1}')
[ -z "$SERVER_IP" ] && SERVER_IP="localhost"

echo ""
echo -e "${GREEN}╔══════════════════════════════════════════════════╗"
echo -e "║  ✓ StreamVault 安装/更新完成！                   ║"
echo -e "╠══════════════════════════════════════════════════╣"
echo -e "║  访问地址：http://${SERVER_IP}:${PORT}                 ║"
echo -e "║  安装目录：${INSTALL_DIR}       ║"
echo -e "║  视频目录：${VIDEO_DIR}  ║"
echo -e "╠══════════════════════════════════════════════════╣"
if [ "$USE_SYSTEMD" = "true" ]; then
echo -e "║  常用命令（systemd 用户服务）：                  ║"
echo -e "║  systemctl --user status streamvault             ║"
echo -e "║  journalctl --user -u streamvault -f             ║"
echo -e "║  systemctl --user restart streamvault            ║"
else
echo -e "║  常用命令（后台进程）：                          ║"
echo -e "║  tail -f ~/streamvault/streamvault.log           ║"
echo -e "║  kill \$(cat ~/streamvault/streamvault.pid)       ║"
fi
echo -e "╚══════════════════════════════════════════════════╝${NC}"
echo ""
echo -e "${YELLOW}如有防火墙，请开放端口：ufw allow ${PORT}${NC}"
if [ "$USE_SYSTEMD" = "true" ]; then
  echo -e "${YELLOW}开机自启（需 sudo）：sudo loginctl enable-linger ${USER}${NC}"
fi
