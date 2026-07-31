#!/bin/bash
# StreamVault 一键安装脚本（无需 root）
#
# 使用方法（HEAD 会自动解析为仓库当前默认分支，无需关心分支名）：
#   bash <(curl -fsSL https://raw.githubusercontent.com/wang-zewen/myTV/HEAD/deploy.sh)
# 或：
#   bash <(wget -qO- https://raw.githubusercontent.com/wang-zewen/myTV/HEAD/deploy.sh)

set -e

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

REPO="wang-zewen/myTV"
TMP_WORK=$(mktemp -d)
trap 'rm -rf "$TMP_WORK"' EXIT

echo -e "${BLUE}"
echo "  ╔══════════════════════════════╗"
echo "  ║   StreamVault 一键安装       ║"
echo "  ║   M3U8 视频下载管理器        ║"
echo "  ╚══════════════════════════════╝"
echo -e "${NC}"

if command -v curl &>/dev/null; then
  HTTP_GET() { curl -fsSL "$1"; }
  HTTP_DOWNLOAD() { curl -fsSL "$1" -o "$2"; }
elif command -v wget &>/dev/null; then
  HTTP_GET() { wget -qO- "$1"; }
  HTTP_DOWNLOAD() { wget -q "$1" -O "$2"; }
else
  echo -e "${RED}错误：需要 curl 或 wget${NC}"
  exit 1
fi

# ── 探测默认分支 ────────────────────────────────────────
# 仓库默认分支不一定叫 main，硬编码分支名会在分支改名/切换时导致下载 404
BRANCH=$(HTTP_GET "https://api.github.com/repos/${REPO}" 2>/dev/null \
  | grep -o '"default_branch"[[:space:]]*:[[:space:]]*"[^"]*"' | head -n 1 \
  | sed -E 's/.*"([^"]*)"$/\1/')
if [ -z "$BRANCH" ]; then
  BRANCH="main"
  echo -e "${YELLOW}未能探测到默认分支，回退使用 ${BRANCH}${NC}"
fi

DOWNLOAD_URL="https://github.com/${REPO}/archive/refs/heads/${BRANCH}.zip"

# ── 下载 ──────────────────────────────────────────────
echo -e "${YELLOW}正在下载 StreamVault（分支：${BRANCH}）...${NC}"
HTTP_DOWNLOAD "$DOWNLOAD_URL" "$TMP_WORK/sv.zip"

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
