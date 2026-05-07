#!/bin/bash
# StreamVault 一键安装脚本（无需 root）
#
# 使用方法：
#   bash <(curl -fsSL https://raw.githubusercontent.com/wang-zewen/myTV/main/deploy.sh)
# 或：
#   bash <(wget -qO- https://raw.githubusercontent.com/wang-zewen/myTV/main/deploy.sh)

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

REPO="wang-zewen/myTV"
BRANCH="main"
DOWNLOAD_URL="https://github.com/${REPO}/archive/refs/heads/${BRANCH}.zip"
TMP_WORK=$(mktemp -d)
trap 'rm -rf "$TMP_WORK"' EXIT

echo -e "${BLUE}"
echo "  ╔══════════════════════════════╗"
echo "  ║   StreamVault 一键安装       ║"
echo "  ║   M3U8 视频下载管理器        ║"
echo "  ╚══════════════════════════════╝"
echo -e "${NC}"

# ── 下载 ──────────────────────────────────────────────
echo -e "${YELLOW}正在下载 StreamVault...${NC}"
if command -v curl &>/dev/null; then
  curl -fsSL "$DOWNLOAD_URL" -o "$TMP_WORK/sv.zip"
elif command -v wget &>/dev/null; then
  wget -q "$DOWNLOAD_URL" -O "$TMP_WORK/sv.zip"
else
  echo -e "${RED}错误：需要 curl 或 wget${NC}"
  exit 1
fi

# ── 解压 ──────────────────────────────────────────────
echo -e "${YELLOW}正在解压...${NC}"
unzip -q "$TMP_WORK/sv.zip" -d "$TMP_WORK"

# GitHub zip 解压后目录名为 <repo>-<branch>（斜杠替换为 -）
EXTRACT_DIR=$(find "$TMP_WORK" -maxdepth 1 -mindepth 1 -type d | head -n 1)
if [ -z "$EXTRACT_DIR" ]; then
  echo -e "${RED}解压失败，未找到目录${NC}"
  exit 1
fi

# ── 运行安装脚本 ───────────────────────────────────────
echo -e "${YELLOW}正在安装...${NC}"
bash "$EXTRACT_DIR/install.sh"
