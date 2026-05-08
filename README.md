# StreamVault - M3U8 视频下载管理器

## 目录结构

```
streamvault/
├── backend/
│   └── main.py        # FastAPI 后端
├── frontend/
│   └── index.html     # 网页管理界面
├── deploy.sh          # 一键安装脚本（从 GitHub 下载并安装）
├── install.sh         # 本地安装/更新脚本
├── data.example.json  # 默认示例配置（首次安装会复制为 data.json）
└── README.md
```

## 一键安装（推荐，无需 root）

```bash
bash <(curl -fsSL https://raw.githubusercontent.com/wang-zewen/myTV/main/deploy.sh)
```

或使用 wget：

```bash
bash <(wget -qO- https://raw.githubusercontent.com/wang-zewen/myTV/main/deploy.sh)
```

安装完成后访问 `http://你的服务器IP:8080`

> **无需 sudo / root 权限。** 程序安装在 `~/streamvault/`，视频保存在 `~/streamvault/videos/`。

---

## 本地安装/更新

已下载源码时直接运行：

```bash
bash install.sh
```

首次安装时会默认把项目里的 `data.example.json` 复制为 `~/streamvault/data.json`，作为初始配置模板；后续再次执行安装脚本会保留你已经在使用的 `data.json`。

---

## 开机自启（可选，需 sudo）

默认使用 systemd 用户服务，登录后自动运行。  
若需要**开机即启动**（无需登录），执行一次：

```bash
sudo loginctl enable-linger $USER
```

---

## 手动安装

### 1. 安装依赖

```bash
sudo apt update
sudo apt install -y python3 python3-pip python3-venv ffmpeg
```

### 2. 安装 N_m3u8DL-RE（推荐，比 ffmpeg 快很多）

```bash
wget https://github.com/nilaoda/N_m3u8DL-RE/releases/latest/download/N_m3u8DL-RE_Beta_linux-x64_20231216.tar.gz
tar -xzf N_m3u8DL-RE*.tar.gz
sudo mv N_m3u8DL-RE /usr/local/bin/
sudo chmod +x /usr/local/bin/N_m3u8DL-RE
```

### 3. 安装 Python 依赖

```bash
cd ~/streamvault
python3 -m venv venv
venv/bin/pip install fastapi "uvicorn[standard]" python-multipart httpx
```

### 4. 启动

```bash
cd ~/streamvault
venv/bin/uvicorn main:app --host 0.0.0.0 --port 8080
```

---

## 使用方法

1. 打开浏览器访问 `http://服务器IP:8080`
2. 在输入框粘贴 M3U8 链接
3. 填写视频名称（可选）
4. 点击「开始下载」
5. 实时查看下载进度和日志
6. 下载完成后在视频库中播放或获取链接

## 在 TVBox 中使用

### 统一订阅（推荐）

将以下地址填入 TVBox 的「配置地址」或「仓库」：

```
http://你的服务器IP:8080/tvbox/source
```

订阅包含：
- **📼 本地视频库** — 已下载到服务器的本地视频，可直接播放
- **🎬 Emby 媒体库** — 若已在设置页配置 Emby，自动出现；按媒体库分类浏览，支持电影直播和剧集分集播放
- **采集站** — 在设置页添加的第三方采集站，自动加入订阅

每个来源是否出现在 TVBox 订阅中均可在网页管理界面单独控制：「TVBox 订阅」页可开关本地视频库和 Emby 媒体库；「接口管理」页每个采集站右侧有 📺 按钮可独立切换。

### 单独播放本地视频

如只想播放某个视频，复制视频卡片上的链接，格式为：

```
http://你的服务器IP:8080/api/stream/视频文件名.mp4
```

将此链接直接填入 TVBox 的自定义播放地址即可。

---

## 常用命令

```bash
# 查看服务状态（systemd 用户服务）
systemctl --user status streamvault

# 实时查看日志
journalctl --user -u streamvault -f

# 重启服务
systemctl --user restart streamvault

# 查看已下载视频
ls -lh ~/streamvault/videos/

# 开放防火墙端口
ufw allow 8080
```

## 环境变量（高级）

| 变量 | 说明 | 默认值 |
|------|------|--------|
| `STREAMVAULT_HOME` | 安装根目录 | main.py 所在目录 |
| `STREAMVAULT_VIDEO_DIR` | 视频保存目录 | `$STREAMVAULT_HOME/videos` |
| `STREAMVAULT_DATA_FILE` | 配置文件路径 | `$STREAMVAULT_HOME/data.json` |
