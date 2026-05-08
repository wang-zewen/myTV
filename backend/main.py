#!/usr/bin/env python3
"""
StreamVault - 采集站搜索 + M3U8 下载管理器 + 自动追剧订阅
"""

import asyncio
import json
import os
import re
import shutil
import uuid
from datetime import datetime
from pathlib import Path
from typing import Optional, List
from urllib.parse import quote

import hashlib
import secrets
import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, HTMLResponse, JSONResponse, Response, RedirectResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

app = FastAPI(title="StreamVault")
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_methods=["*"], allow_headers=["*"])

_HTTP_LIMITS = httpx.Limits(max_connections=30, max_keepalive_connections=10)
_HTTP_TIMEOUT = httpx.Timeout(12.0, connect=5.0)

def _make_http_client(**kwargs) -> httpx.AsyncClient:
    """Shared async HTTP client with consistent timeouts and connection limits."""
    return httpx.AsyncClient(
        follow_redirects=True,
        timeout=_HTTP_TIMEOUT,
        limits=_HTTP_LIMITS,
        **kwargs,
    )

_BASE_DIR = Path(os.environ.get("STREAMVAULT_HOME", Path(__file__).parent))
VIDEO_DIR = Path(os.environ.get("STREAMVAULT_VIDEO_DIR", str(_BASE_DIR / "videos")))
VIDEO_DIR.mkdir(parents=True, exist_ok=True)

DATA_FILE = Path(os.environ.get("STREAMVAULT_DATA_FILE", str(_BASE_DIR / "data.json")))
DATA_FILE.parent.mkdir(parents=True, exist_ok=True)

CACHE_DIR = Path(os.environ.get("STREAMVAULT_CACHE_DIR", str(_BASE_DIR / "cache")))
EMBY_IMAGE_CACHE_DIR = CACHE_DIR / "emby_images"
EMBY_IMAGE_CACHE_DIR.mkdir(parents=True, exist_ok=True)

CHECK_INTERVAL = 3600   # 每小时检查一次订阅更新
MAX_CONCURRENT_DOWNLOADS = 3  # 最大并发下载数

# ─── 运行时状态 ────────────────────────────────────────
tasks: dict = {}          # 下载任务
api_sources: list = []    # 采集站接口
subscriptions: dict = {}  # 订阅列表 {sub_id: Subscription}
settings: dict = {"check_interval": 3600, "password_hash": "", "tvbox_local_enabled": True, "tvbox_emby_enabled": True}  # 全局设置，password_hash 为空时不需要认证
emby_config: dict = {"url": "", "api_key": "", "user_id": "", "password_hash": ""}
_download_semaphore: Optional[asyncio.Semaphore] = None


# ─── 持久化 ────────────────────────────────────────────

def save_data():
    data = {
        "api_sources": api_sources,
        "settings": settings,
        "emby_config": emby_config,
        "subscriptions": {
            sid: {
                "sub_id": s.sub_id,
                "vod_name": s.vod_name,
                "vod_id": s.vod_id,
                "source_url": s.source_url,
                "source_name": s.source_name,
                "vod_pic": s.vod_pic,
                "added_at": s.added_at,
                "last_checked": s.last_checked,
                "last_episode_count": s.last_episode_count,
                "downloaded_episodes": list(s.downloaded_episodes),
                "status": s.status,
                "last_update": s.last_update,
            }
            for sid, s in subscriptions.items()
        }
    }
    try:
        DATA_FILE.write_text(json.dumps(data, ensure_ascii=False, indent=2))
    except Exception as e:
        print(f"[save_data] 写入失败: {e}")


def load_data():
    global api_sources
    if not DATA_FILE.exists():
        return
    try:
        data = json.loads(DATA_FILE.read_text())
        api_sources.clear()
        api_sources.extend(data.get("api_sources", []))
        for src in api_sources:
            src.setdefault("tvbox_enabled", True)
        settings.update(data.get("settings", {}))
        saved_ec = data.get("emby_config", {})
        for k in emby_config:
            if k in saved_ec:
                emby_config[k] = saved_ec[k]
        for sid, d in data.get("subscriptions", {}).items():
            s = Subscription.__new__(Subscription)
            s.sub_id = d["sub_id"]
            s.vod_name = d["vod_name"]
            s.vod_id = d["vod_id"]
            s.source_url = d["source_url"]
            s.source_name = d.get("source_name", "")
            s.vod_pic = d.get("vod_pic", "")
            s.added_at = d["added_at"]
            s.last_checked = d.get("last_checked", "")
            s.last_episode_count = d.get("last_episode_count", 0)
            s.downloaded_episodes = set(d.get("downloaded_episodes", []))
            s.status = d.get("status", "active")
            s.last_update = d.get("last_update", "")
            subscriptions[sid] = s
    except Exception as e:
        print(f"[load_data] 读取失败: {e}")


# ─── Models ────────────────────────────────────────────

class DownloadRequest(BaseModel):
    url: str
    name: Optional[str] = None

class ApiSource(BaseModel):
    name: str
    url: str

class SearchRequest(BaseModel):
    keyword: str
    source_urls: Optional[List[str]] = None

class SubscribeRequest(BaseModel):
    vod_id: str
    vod_name: str
    source_url: str
    source_name: str
    vod_pic: Optional[str] = ""
    download_existing: bool = False  # True=补全已有集，False=只追新集



# ─── Task ──────────────────────────────────────────────

TMP_DIR = Path("/tmp/m3u8dl")
TMP_DIR.mkdir(parents=True, exist_ok=True)

MAX_RETRIES = 3          # 失败后最多重试次数
MIN_FILE_SIZE = 1024     # 完整性校验：文件至少 1KB，否则视为损坏


class TaskStatus:
    def __init__(self, task_id, name, url, source="manual"):
        self.task_id = task_id
        self.name = name
        self.url = url
        self.status = "downloading"   # downloading | paused | done | error | cancelled
        self.progress = 0.0
        self.log_lines = []
        self.created_at = datetime.now().isoformat()
        self.error = ""
        self.process = None
        self.retries = 0
        self.source = source          # manual | subscribe（来源标记）


_VIDEO_EXTS = {".mp4", ".mkv", ".ts", ".m4v", ".m2ts", ".mpeg"}

def _safe_filename(name: str, max_bytes: int = 200) -> str:
    """按字节截断文件名（保证 UTF-8 多字节字符不被截断到中间）。
    预留 55 字节给扩展名及 N_m3u8DL-RE 附加的语言标签。
    """
    encoded = name.encode("utf-8")
    if len(encoded) <= max_bytes:
        return name
    truncated = encoded[:max_bytes].decode("utf-8", errors="ignore")
    return truncated.rstrip("_") or "video"

def _find_output_file(output_name: str):
    """查找下载完成后的输出文件。
    N_m3u8DL-RE 可能附加语言/分辨率标签（如 name.zh.mp4），
    所以先精确匹配，再做前缀 glob，取最大的视频文件。
    """
    for ext in _VIDEO_EXTS:
        p = VIDEO_DIR / f"{output_name}{ext}"
        if p.exists():
            return p
    candidates = [
        p for p in VIDEO_DIR.glob(f"{output_name}*")
        if p.suffix.lower() in _VIDEO_EXTS
    ]
    return max(candidates, key=lambda p: p.stat().st_size) if candidates else None



def _verify_file(path: Path) -> tuple:
    """
    校验文件完整性。
    返回 (ok: bool, reason: str)
    - 文件存在且大于最小尺寸视为完整
    - m3u8 下载的 mp4 用 ffprobe 做格式校验（如果有的话）
    """
    if not path.exists():
        return False, "文件不存在"
    size = path.stat().st_size
    if size < MIN_FILE_SIZE:
        return False, f"文件过小（{size} 字节），可能已损坏"
    # 尝试用 ffprobe 校验容器完整性
    ffprobe = shutil.which("ffprobe")
    if ffprobe:
        import subprocess
        result = subprocess.run(
            [ffprobe, "-v", "error", "-select_streams", "v:0",
             "-show_entries", "stream=codec_type", "-of", "csv=p=0", str(path)],
            capture_output=True, text=True, timeout=30
        )
        if result.returncode != 0 or not result.stdout.strip():
            return False, "ffprobe 校验失败，视频流可能损坏"
    return True, "ok"


async def run_download(task_id: str, url: str, output_name: str):
    """并发限流入口，实际下载逻辑在 _run_download_core"""
    task = tasks[task_id]
    sem = _download_semaphore
    if sem is not None:
        active = sum(1 for t in tasks.values() if t.status == "downloading")
        if active >= MAX_CONCURRENT_DOWNLOADS:
            task.status = "queued"
            task.log_lines.append(f"[排队] 等待下载槽（当前 {active}/{MAX_CONCURRENT_DOWNLOADS}）...")
        async with sem:
            if task.status == "queued":
                task.status = "downloading"
                task.log_lines.append("[开始] 已获得下载槽")
            await _run_download_core(task_id, url, output_name)
    else:
        await _run_download_core(task_id, url, output_name)


async def _run_download_core(task_id: str, url: str, output_name: str):
    task = tasks[task_id]

    while task.retries <= MAX_RETRIES:
        if task.status == "cancelled":
            return

        if task.retries > 0:
            task.log_lines.append(f"--- 第 {task.retries} 次重试 ---")
            task.progress = 0.0
            await asyncio.sleep(3)  # 重试前稍等

        try:
            nm3u8 = shutil.which("N_m3u8DL-RE")
            ytdlp = shutil.which("yt-dlp") or str(_BASE_DIR / "venv/bin/yt-dlp")
            ffmpeg = shutil.which("ffmpeg")

            if nm3u8:
                tmp_path = TMP_DIR / output_name
                tmp_path.mkdir(parents=True, exist_ok=True)
                cmd = [
                    nm3u8, url,
                    "--save-dir", str(VIDEO_DIR),
                    "--save-name", output_name,
                    "--tmp-dir", str(tmp_path),
                    "--thread-count", "4",
                    "--download-retry-count", "5",
                    "--log-level", "INFO",
                    "--no-date-info",
                    "--auto-select",
                    "--disable-update-check",
                ]
            elif Path(ytdlp).exists():
                out_template = str(VIDEO_DIR / f"{output_name}.%(ext)s")
                cmd = [
                    ytdlp, url,
                    "--hls-prefer-native",
                    "--no-part",
                    "-o", out_template,
                    "--newline",
                ]
            elif ffmpeg:
                out_path = VIDEO_DIR / f"{output_name}.mp4"
                cmd = [
                    ffmpeg, "-i", url,
                    "-c", "copy",
                    "-y",
                    str(out_path),
                ]
            else:
                task.status = "error"
                task.error = "未找到下载工具（yt-dlp/ffmpeg/N_m3u8DL-RE）"
                return

            process = await asyncio.create_subprocess_exec(
                *cmd,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.STDOUT,
            )
            task.process = process

            while True:
                # 暂停时挂起等待，每秒检查一次是否恢复
                while task.status == "paused":
                    await asyncio.sleep(1)
                if task.status == "cancelled":
                    break
                line = await process.stdout.readline()
                if not line:
                    break
                if task.status == "cancelled":
                    break
                decoded = line.decode("utf-8", errors="replace").strip()
                if decoded:
                    task.log_lines.append(decoded)
                    m = re.search(r"(\d+\.?\d*)%", decoded)
                    if m:
                        task.progress = float(m.group(1))
                    elif "time=" in decoded:
                        task.progress = min(task.progress + 1.5, 95)

            await process.wait()

            if task.status == "cancelled":
                # 取消时清理输出文件，但保留 tmp 分片（便于之后续传）
                for f in VIDEO_DIR.glob(f"{output_name}.*"):
                    try: f.unlink()
                    except: pass
                return

            if process.returncode != 0:
                task.retries += 1
                err = f"下载进程退出码 {process.returncode}"
                task.log_lines.append(f"[错误] {err}")
                if task.retries > MAX_RETRIES:
                    task.status = "error"
                    task.error = f"{err}，已重试 {MAX_RETRIES} 次"
                continue  # 进入下一次重试

            # 进程正常退出，做完整性校验
            out_file = _find_output_file(output_name)
            if not out_file:
                task.retries += 1
                task.log_lines.append("[错误] 未找到输出文件")
                if task.retries > MAX_RETRIES:
                    task.status = "error"
                    task.error = "下载完成但未找到输出文件"
                continue

            ok, reason = _verify_file(out_file)
            if not ok:
                task.retries += 1
                task.log_lines.append(f"[校验失败] {reason}，删除文件准备重试")
                try: out_file.unlink()
                except: pass
                if task.retries > MAX_RETRIES:
                    task.status = "error"
                    task.error = f"文件校验失败：{reason}"
                continue

            # 校验通过，清理临时目录
            task.log_lines.append(f"[完成] 文件校验通过：{out_file.name}（{round(out_file.stat().st_size/1024/1024, 1)} MB）")
            tmp_path_cleanup = TMP_DIR / output_name
            if tmp_path_cleanup.exists():
                shutil.rmtree(tmp_path_cleanup, ignore_errors=True)

            task.status = "done"
            task.progress = 100
            return

        except Exception as e:
            if task.status == "cancelled":
                return
            task.retries += 1
            task.log_lines.append(f"[异常] {e}")
            if task.retries > MAX_RETRIES:
                task.status = "error"
                task.error = str(e)


# ─── 采集站搜索 ────────────────────────────────────────

def normalize_api_url(url: str) -> str:
    url = url.strip().rstrip("/")
    if "provide/vod" not in url and url.endswith("api.php"):
        url += "/provide/vod/at/json/"
    return url

def _is_allowed_source_url(url: str) -> bool:
    """Return True only if url (normalized) matches a configured api_source."""
    norm = normalize_api_url(url)
    return any(normalize_api_url(s["url"]) == norm for s in api_sources)

def parse_episodes(play_from: str, play_url_raw: str) -> list:
    if not play_url_raw:
        return []
    froms = play_from.split("$$$") if play_from else []
    groups = play_url_raw.split("$$$")
    best_group, best_from = None, "默认"
    for i, g in enumerate(groups):
        if "http" in g.lower():
            best_group = g
            best_from = froms[i] if i < len(froms) else f"线路{i+1}"
            break
    if not best_group:
        best_group = groups[0] if groups else ""
    episodes = []
    for ep in best_group.split("#"):
        ep = ep.strip()
        if not ep:
            continue
        if "$" in ep:
            name, url = ep.split("$", 1)
        else:
            name, url = "播放", ep
        if url.strip().startswith("http"):
            episodes.append({"name": name.strip(), "url": url.strip(), "from": best_from})
    return episodes

async def search_single_source(client: httpx.AsyncClient, api_url: str, keyword: str) -> tuple:
    """返回 (results: list, error: str)"""
    base = normalize_api_url(api_url)
    try:
        resp = await client.get(base, params={"ac": "detail", "wd": keyword}, timeout=10)
        resp.raise_for_status()
        text = resp.text.strip()
        if not text.startswith("{") and not text.startswith("["):
            return [], text[:100]
        data = resp.json()
        results = []
        for item in data.get("list", []):
            episodes = parse_episodes(item.get("vod_play_from", ""), item.get("vod_play_url", ""))
            results.append({
                "vod_id": str(item.get("vod_id", "")),
                "vod_name": item.get("vod_name", ""),
                "type_name": item.get("type_name", ""),
                "vod_year": item.get("vod_year", ""),
                "vod_area": item.get("vod_area", ""),
                "vod_remarks": item.get("vod_remarks", ""),
                "vod_pic": item.get("vod_pic", ""),
                "vod_content": re.sub(r"<[^>]+>", "", item.get("vod_content", ""))[:200],
                "episodes": episodes,
                "source_url": api_url,
            })
        return results, ""
    except httpx.TimeoutException:
        return [], "请求超时"
    except httpx.HTTPStatusError as e:
        return [], f"HTTP {e.response.status_code}"
    except Exception as e:
        return [], str(e)

async def fetch_vod_detail(client: httpx.AsyncClient, api_url: str, vod_id: str) -> Optional[dict]:
    """按 vod_id 获取影片详情（用于订阅检查）"""
    base = normalize_api_url(api_url)
    try:
        resp = await client.get(base, params={"ac": "detail", "ids": vod_id}, timeout=15)
        resp.raise_for_status()
        data = resp.json()
        items = data.get("list", [])
        if not items:
            return None
        item = items[0]
        episodes = parse_episodes(item.get("vod_play_from", ""), item.get("vod_play_url", ""))
        return {
            "vod_name": item.get("vod_name", ""),
            "vod_remarks": item.get("vod_remarks", ""),
            "episodes": episodes,
        }
    except Exception:
        return None


# ─── 订阅系统 ──────────────────────────────────────────

class Subscription:
    def __init__(self, sub_id, vod_id, vod_name, source_url, source_name, vod_pic=""):
        self.sub_id = sub_id
        self.vod_id = vod_id
        self.vod_name = vod_name
        self.source_url = source_url
        self.source_name = source_name
        self.vod_pic = vod_pic
        self.added_at = datetime.now().isoformat()
        self.last_checked = ""
        self.last_episode_count = 0
        self.downloaded_episodes: set = set()   # 已下载的集名集合
        self.status = "active"   # active | paused | completed
        self.last_update = ""    # 最后发现更新的时间


async def check_subscription(sub: Subscription):
    """检查单个订阅是否有新集，有则触发下载"""
    if sub.status != "active":
        return

    async with httpx.AsyncClient(follow_redirects=True) as client:
        detail = await fetch_vod_detail(client, sub.source_url, sub.vod_id)

    sub.last_checked = datetime.now().isoformat()

    if not detail:
        print(f"[订阅] {sub.vod_name} 获取详情失败")
        save_data()
        return

    episodes = detail["episodes"]
    new_eps = [ep for ep in episodes if ep["name"] not in sub.downloaded_episodes]

    if new_eps:
        sub.last_update = datetime.now().isoformat()
        print(f"[订阅] {sub.vod_name} 发现 {len(new_eps)} 集新内容，开始下载")
        for ep in new_eps:
            safe_name = _safe_filename(re.sub(r'[^\w\u4e00-\u9fff\-]', '_', f"{sub.vod_name}_{ep['name']}"))
            # 先标记已知，防止重复触发
            sub.downloaded_episodes.add(ep["name"])
            # 文件已存在则跳过，不重复下载
            if list(VIDEO_DIR.glob(f"{safe_name}.*")):
                print(f"[订阅] 文件已存在，跳过：{safe_name}")
                continue
            task_id = str(uuid.uuid4())
            task = TaskStatus(task_id, safe_name, ep["url"], source="subscribe")
            task.log_lines.append(f"[自动追剧] 订阅：{sub.vod_name}")
            tasks[task_id] = task
            asyncio.create_task(run_download(task_id, ep["url"], safe_name))

    sub.last_episode_count = len(episodes)
    save_data()


async def subscription_watcher():
    """后台循环，每小时检查所有活跃订阅"""
    await asyncio.sleep(10)   # 启动后稍等10秒再开始
    while True:
        active = [s for s in subscriptions.values() if s.status == "active"]
        if active:
            print(f"[订阅] 开始检查 {len(active)} 个订阅...")
            await asyncio.gather(*[check_subscription(s) for s in active])
            print(f"[订阅] 检查完毕")
        await asyncio.sleep(settings.get("check_interval", 3600))


@app.on_event("startup")
async def startup():
    global _download_semaphore
    load_data()
    _download_semaphore = asyncio.Semaphore(MAX_CONCURRENT_DOWNLOADS)
    asyncio.create_task(subscription_watcher())


# ─── 路由 ──────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def serve_public():
    for p in [Path(__file__).parent / "public.html",
              Path(__file__).parent.parent / "frontend" / "public.html"]:
        if p.exists():
            return p.read_text(encoding="utf-8")
    return "<h1>public.html not found</h1>"


@app.get("/favicon.svg")
async def serve_favicon():
    for p in [Path(__file__).parent / "favicon.svg",
              Path(__file__).parent.parent / "frontend" / "favicon.svg"]:
        if p.exists():
            return FileResponse(p, media_type="image/svg+xml")
    return Response(status_code=404)


@app.get("/admin", response_class=HTMLResponse)
async def serve_admin():
    for p in [Path(__file__).parent / "index.html",
              Path(__file__).parent.parent / "frontend" / "index.html"]:
        if p.exists():
            return p.read_text(encoding="utf-8")
    return "<h1>index.html not found</h1>"


@app.get("/api/catalog")
async def catalog(source_url: str, ac: str = "list", pg: int = 1, t: int = 0, ids: str = ""):
    """代理采集站请求，只允许已配置接口，供公开浏览页调用"""
    if not _is_allowed_source_url(source_url):
        raise HTTPException(403, "该接口未在系统中配置，拒绝访问")
    base = normalize_api_url(source_url)
    params: dict = {"ac": ac, "pg": pg}
    if t:
        params["t"] = t
    if ids:
        params["ids"] = ids
    try:
        async with _make_http_client() as client:
            resp = await client.get(base, params=params, timeout=10)
            resp.raise_for_status()
            return resp.json()
    except httpx.TimeoutException:
        raise HTTPException(504, "上游接口超时")
    except httpx.HTTPStatusError as e:
        raise HTTPException(502, f"上游返回 HTTP {e.response.status_code}")
    except Exception as e:
        raise HTTPException(502, str(e))


# ── 接口管理 ──

@app.get("/api/sources")
async def list_sources():
    return api_sources

@app.post("/api/sources")
async def add_source(src: ApiSource):
    normalized = normalize_api_url(src.url)
    for s in api_sources:
        if normalize_api_url(s["url"]) == normalized:
            raise HTTPException(400, "该接口已存在")
    api_sources.append({"name": src.name.strip(), "url": normalized, "enabled": True, "tvbox_enabled": True})
    save_data()
    return {"ok": True}

@app.patch("/api/sources/{index}")
async def edit_source(index: int, src: ApiSource):
    if index < 0 or index >= len(api_sources):
        raise HTTPException(404, "不存在")
    normalized = normalize_api_url(src.url)
    for i, s in enumerate(api_sources):
        if i != index and normalize_api_url(s["url"]) == normalized:
            raise HTTPException(400, "该接口地址已存在")
    old_url = api_sources[index]["url"]
    api_sources[index]["name"] = src.name.strip()
    api_sources[index]["url"] = normalized
    # 同步更新引用该接口的订阅
    for sub in subscriptions.values():
        if sub.source_url == old_url:
            sub.source_name = src.name.strip()
            sub.source_url = normalized
    save_data()
    return {"ok": True}

@app.patch("/api/sources/{index}/toggle")
async def toggle_source(index: int):
    if index < 0 or index >= len(api_sources):
        raise HTTPException(404, "不存在")
    api_sources[index]["enabled"] = not api_sources[index].get("enabled", True)
    save_data()
    return {"enabled": api_sources[index]["enabled"]}

@app.patch("/api/sources/{index}/tvbox-toggle")
async def tvbox_toggle_source(index: int):
    if index < 0 or index >= len(api_sources):
        raise HTTPException(404, "不存在")
    api_sources[index]["tvbox_enabled"] = not api_sources[index].get("tvbox_enabled", True)
    save_data()
    return {"tvbox_enabled": api_sources[index]["tvbox_enabled"]}

@app.delete("/api/sources/{index}")
async def delete_source(index: int):
    if index < 0 or index >= len(api_sources):
        raise HTTPException(404, "不存在")
    api_sources.pop(index)
    save_data()
    return {"ok": True}


# ── 接口测试 ──

@app.post("/api/sources/test")
async def test_source(src: ApiSource):
    base = normalize_api_url(src.url)
    try:
        async with _make_http_client() as client:
            resp = await client.get(base, params={"ac": "detail", "wd": "test"}, timeout=8)
            resp.raise_for_status()
            text = resp.text.strip()
            if not text.startswith("{") and not text.startswith("["):
                return {"ok": False, "error": text[:100]}
            data = resp.json()
            total = data.get("total", "?")
            return {"ok": True, "msg": f"接口正常，共 {total} 条数据"}
    except httpx.TimeoutException:
        return {"ok": False, "error": "请求超时（8s）"}
    except httpx.HTTPStatusError as e:
        return {"ok": False, "error": f"HTTP {e.response.status_code}"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


# ── 搜索 ──

@app.post("/api/search")
async def search_videos(req: SearchRequest):
    urls = req.source_urls or [s["url"] for s in api_sources if s.get("enabled", True)]
    if not urls:
        raise HTTPException(400, "请先添加采集站接口")
    async with httpx.AsyncClient(follow_redirects=True) as client:
        raw = await asyncio.gather(*[search_single_source(client, u, req.keyword) for u in urls])
    all_results = []
    errors = []
    for url, (results, err) in zip(urls, raw):
        source_name = next((s["name"] for s in api_sources if s["url"] == url), url)
        if err:
            errors.append({"source": source_name, "error": err})
        for r in results:
            r["source_name"] = source_name
            r["subscribed"] = any(
                s.vod_id == r["vod_id"] and s.source_url == r["source_url"]
                for s in subscriptions.values()
            )
            all_results.append(r)
    return {"results": all_results, "total": len(all_results), "errors": errors}


# ── 下载任务 ──

@app.post("/api/download")
async def start_download(req: DownloadRequest):
    task_id = str(uuid.uuid4())
    name = req.name or f"video_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    name = _safe_filename(re.sub(r'[^\w\u4e00-\u9fff\-]', '_', name))
    task = TaskStatus(task_id, name, req.url)
    tasks[task_id] = task
    asyncio.create_task(run_download(task_id, req.url, name))
    return {"task_id": task_id, "name": name}

@app.post("/api/task/{task_id}/cancel")
async def cancel_task(task_id: str):
    if task_id not in tasks:
        raise HTTPException(404, "任务不存在")
    task = tasks[task_id]
    if task.status != "downloading":
        raise HTTPException(400, f"任务状态为 {task.status}，无法取消")
    task.status = "cancelled"
    task.error = "已手动取消"
    if task.process and task.process.returncode is None:
        try: task.process.kill()
        except: pass
    return {"ok": True}

@app.post("/api/task/{task_id}/pause")
async def pause_task(task_id: str):
    if task_id not in tasks:
        raise HTTPException(404, "任务不存在")
    task = tasks[task_id]
    if task.status != "downloading":
        raise HTTPException(400, f"任务状态为 {task.status}，无法暂停")
    task.status = "paused"
    task.log_lines.append("[已暂停]")
    # 用 SIGSTOP 挂起子进程（Linux）
    if task.process and task.process.returncode is None:
        try:
            import os, signal
            os.kill(task.process.pid, signal.SIGSTOP)
        except Exception:
            pass
    return {"ok": True}

@app.post("/api/task/{task_id}/resume")
async def resume_task(task_id: str):
    if task_id not in tasks:
        raise HTTPException(404, "任务不存在")
    task = tasks[task_id]
    if task.status != "paused":
        raise HTTPException(400, f"任务状态为 {task.status}，无法恢复")
    # 先发 SIGCONT 恢复进程，再改状态（让读循环能继续）
    if task.process and task.process.returncode is None:
        try:
            import os, signal
            os.kill(task.process.pid, signal.SIGCONT)
        except Exception:
            pass
    task.status = "downloading"
    task.log_lines.append("[已恢复]")
    return {"ok": True}

@app.get("/api/task/{task_id}")
async def get_task(task_id: str):
    if task_id not in tasks:
        raise HTTPException(404, "任务不存在")
    t = tasks[task_id]
    return {"task_id": t.task_id, "name": t.name, "status": t.status,
            "progress": t.progress, "log": t.log_lines[-20:],
            "error": t.error, "created_at": t.created_at, "retries": t.retries,
            "source": t.source}

@app.get("/api/tasks")
async def list_tasks():
    return [{"task_id": t.task_id, "name": t.name, "status": t.status,
             "progress": t.progress, "error": t.error, "created_at": t.created_at,
             "source": t.source}
            for t in tasks.values()]

@app.delete("/api/tasks/{task_id}")
async def delete_task(task_id: str):
    if task_id not in tasks:
        raise HTTPException(404, "任务不存在")
    if tasks[task_id].status == "downloading":
        raise HTTPException(400, "请先取消下载")
    del tasks[task_id]
    return {"ok": True}


# ── 磁盘空间 ──

@app.get("/api/diskspace")
async def disk_space():
    usage = shutil.disk_usage(str(VIDEO_DIR))
    return {
        "total_gb": round(usage.total / 1024**3, 1),
        "used_gb": round(usage.used / 1024**3, 1),
        "free_gb": round(usage.free / 1024**3, 1),
        "percent": round(usage.used / usage.total * 100, 1),
    }


# ── 视频库 ──

@app.get("/api/videos")
async def list_videos():
    videos = []
    try:
        entries = sorted(VIDEO_DIR.iterdir(), key=lambda x: x.stat().st_ctime, reverse=True)
    except OSError:
        return []
    for f in entries:
        if f.suffix.lower() not in {".mp4", ".mkv", ".ts", ".m4v"}:
            continue
        try:
            stat = f.stat()
        except OSError:
            continue
        videos.append({"name": f.stem, "filename": f.name,
            "size_mb": round(stat.st_size / 1024 / 1024, 2),
            "created_at": datetime.fromtimestamp(stat.st_ctime).isoformat(),
            "url": f"/api/stream/{f.name}"})
    return videos

@app.delete("/api/videos/{filename}")
async def delete_video(filename: str):
    if "/" in filename or ".." in filename:
        raise HTTPException(400, "非法文件名")
    path = VIDEO_DIR / filename
    if not path.exists():
        raise HTTPException(404, "文件不存在")
    path.unlink()
    return {"ok": True}

@app.get("/api/stream/{filename}")
async def stream_video(filename: str):
    if "/" in filename or ".." in filename:
        raise HTTPException(400, "非法文件名")
    path = VIDEO_DIR / filename
    try:
        resolved = path.resolve()
        resolved.relative_to(VIDEO_DIR.resolve())
    except (ValueError, OSError):
        raise HTTPException(403, "拒绝访问")
    if not resolved.exists():
        raise HTTPException(404, "文件不存在")
    suffix = resolved.suffix.lower()
    media_types = {".mp4": "video/mp4", ".mkv": "video/x-matroska",
                   ".ts": "video/mp2t", ".m4v": "video/mp4"}
    return FileResponse(resolved, media_type=media_types.get(suffix, "video/mp4"))


# ── 订阅管理 ──

@app.get("/api/subscriptions")
async def list_subscriptions():
    return [_sub_to_dict(s) for s in subscriptions.values()]

@app.post("/api/subscriptions")
async def add_subscription(req: SubscribeRequest):
    # 检查重复
    for s in subscriptions.values():
        if s.vod_id == req.vod_id and s.source_url == req.source_url:
            raise HTTPException(400, "已在订阅列表中")
    sub_id = str(uuid.uuid4())
    sub = Subscription(sub_id, req.vod_id, req.vod_name,
                       req.source_url, req.source_name, req.vod_pic or "")

    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            detail = await fetch_vod_detail(client, req.source_url, req.vod_id)
        if detail:
            sub.last_episode_count = len(detail["episodes"])
            sub.last_checked = datetime.now().isoformat()
            if req.download_existing:
                # 补全模式：现有集全部加入下载队列
                for ep in detail["episodes"]:
                    safe_name = re.sub(r'[^\w\u4e00-\u9fff\-]', '_', f"{req.vod_name}_{ep['name']}")
                    sub.downloaded_episodes.add(ep["name"])
                    if list(VIDEO_DIR.glob(f"{safe_name}.*")):
                        continue  # 文件已存在，跳过
                    task_id = str(uuid.uuid4())
                    task = TaskStatus(task_id, safe_name, ep["url"], source="subscribe")
                    task.log_lines.append(f"[补全下载] 订阅：{req.vod_name}")
                    tasks[task_id] = task
                    asyncio.create_task(run_download(task_id, ep["url"], safe_name))
            else:
                # 只追新集：把现有集全标记为已知，不下载
                for ep in detail["episodes"]:
                    sub.downloaded_episodes.add(ep["name"])
    except Exception:
        pass

    subscriptions[sub_id] = sub
    save_data()
    return {"ok": True, "sub_id": sub_id}

@app.delete("/api/subscriptions/{sub_id}")
async def delete_subscription(sub_id: str):
    if sub_id not in subscriptions:
        raise HTTPException(404, "订阅不存在")
    del subscriptions[sub_id]
    save_data()
    return {"ok": True}

@app.post("/api/subscriptions/{sub_id}/pause")
async def pause_subscription(sub_id: str):
    if sub_id not in subscriptions:
        raise HTTPException(404, "订阅不存在")
    subscriptions[sub_id].status = "paused"
    save_data()
    return {"ok": True}

@app.post("/api/subscriptions/{sub_id}/resume")
async def resume_subscription(sub_id: str):
    if sub_id not in subscriptions:
        raise HTTPException(404, "订阅不存在")
    subscriptions[sub_id].status = "active"
    save_data()
    return {"ok": True}

@app.post("/api/subscriptions/{sub_id}/check")
async def manual_check(sub_id: str):
    """手动触发一次检查"""
    if sub_id not in subscriptions:
        raise HTTPException(404, "订阅不存在")
    asyncio.create_task(check_subscription(subscriptions[sub_id]))
    return {"ok": True}

def _sub_to_dict(s: Subscription) -> dict:
    return {
        "sub_id": s.sub_id,
        "vod_id": s.vod_id,
        "vod_name": s.vod_name,
        "source_url": s.source_url,
        "source_name": s.source_name,
        "vod_pic": s.vod_pic,
        "added_at": s.added_at,
        "last_checked": s.last_checked,
        "last_episode_count": s.last_episode_count,
        "downloaded_count": len(s.downloaded_episodes),
        "downloaded_episodes": sorted(s.downloaded_episodes),
        "status": s.status,
        "last_update": s.last_update,
    }



# ── 设置 ──

@app.get("/api/settings")
async def get_settings():
    return settings

@app.put("/api/settings")
async def update_settings(data: dict):
    interval = data.get("check_interval")
    if interval is not None:
        if not isinstance(interval, int) or interval < 60:
            raise HTTPException(400, "间隔最少 60 秒")
        settings["check_interval"] = interval
    # 设置密码：传 password 字段，空字符串表示取消密码
    if "password" in data:
        pw = data["password"].strip()
        settings["password_hash"] = hashlib.sha256(pw.encode()).hexdigest() if pw else ""
    save_data()
    return {k: v for k, v in settings.items() if k != "password_hash"}

@app.post("/api/auth")
async def check_auth(data: dict):
    """前端登录验证"""
    pw_hash = settings.get("password_hash", "")
    if not pw_hash:
        return {"ok": True, "need_auth": False}
    input_hash = hashlib.sha256(data.get("password", "").encode()).hexdigest()
    if input_hash != pw_hash:
        raise HTTPException(401, "密码错误")
    token = secrets.token_hex(16)
    return {"ok": True, "need_auth": True, "token": token}

@app.get("/api/auth/status")
async def auth_status():
    """告知前端是否需要登录"""
    return {"need_auth": bool(settings.get("password_hash", ""))}

# ── TVBox 设置 ──

class TvboxSettings(BaseModel):
    local_enabled: Optional[bool] = None
    emby_enabled: Optional[bool] = None

@app.get("/api/tvbox/settings")
async def get_tvbox_settings():
    return {
        "local_enabled": settings.get("tvbox_local_enabled", True),
        "emby_enabled": settings.get("tvbox_emby_enabled", True),
    }

@app.patch("/api/tvbox/settings")
async def update_tvbox_settings(body: TvboxSettings):
    if body.local_enabled is not None:
        settings["tvbox_local_enabled"] = body.local_enabled
    if body.emby_enabled is not None:
        settings["tvbox_emby_enabled"] = body.emby_enabled
    save_data()
    return {
        "local_enabled": settings["tvbox_local_enabled"],
        "emby_enabled": settings["tvbox_emby_enabled"],
    }

# ── TVBox 订阅接口 ──

def _get_video_files():
    if not VIDEO_DIR.exists():
        return []
    return [f for f in sorted(VIDEO_DIR.iterdir(), key=lambda x: x.stat().st_ctime, reverse=True)
            if f.suffix.lower() in [".mp4", ".mkv", ".ts", ".m4v"]]


@app.get("/tvbox/source/{source_index}")
async def tvbox_source_proxy(source_index: int, request: Request):
    """Proxy configured CMS sources through our normalized endpoint for TVBox stability."""
    if source_index < 0 or source_index >= len(api_sources):
        raise HTTPException(404, "接口不存在")
    src = api_sources[source_index]
    if not src.get("enabled", True) or not src.get("tvbox_enabled", True):
        raise HTTPException(403, "接口未启用 TVBox 暴露")
    ac = request.query_params.get("ac", "list")
    pg = request.query_params.get("pg", "1")
    t_param = request.query_params.get("t", "")
    ids = request.query_params.get("ids", "")
    wd = request.query_params.get("wd", "")
    params = {"ac": ac, "pg": pg}
    if t_param:
        params["t"] = t_param
    if ids:
        params["ids"] = ids
    if wd:
        params["wd"] = wd
    try:
        async with _make_http_client() as client:
            r = await client.get(normalize_api_url(src["url"]), params=params, timeout=15)
            r.raise_for_status()
            return JSONResponse(content=r.json())
    except httpx.TimeoutException:
        raise HTTPException(504, "上游接口超时")
    except httpx.HTTPStatusError as e:
        raise HTTPException(502, f"上游接口返回 HTTP {e.response.status_code}")
    except Exception as e:
        raise HTTPException(502, str(e))


@app.get("/tvbox/source")
async def tvbox_source(request: Request):
    """
    TVBox 多仓配置：
    - 本地视频库：type=0 内置JSON，直接显示已下载视频
    - 采集站源：type=1 苹果CMS，直接透传网页里配置的每个接口
    把 http://IP:8080/tvbox/source 填入 TVBox 配置地址即可。
    """
    base_url = str(request.base_url).rstrip("/")
    sites = []

    # 1. 本地视频库（type=0，内置JSON直接渲染）
    if settings.get("tvbox_local_enabled", True):
        sites.append({
            "key": "streamvault_local",
            "name": "📼 本地视频库",
            "type": 1,
            "api": f"{base_url}/tvbox",
            "searchable": 0,
            "quickSearch": 0,
            "filterable": 0,
        })

    # 2. Emby 媒体库（配置后才显示，且 TVBox 曝光已开启）
    if emby_config.get("url") and emby_config.get("api_key") and settings.get("tvbox_emby_enabled", True):
        sites.append({
            "key": "emby_library",
            "name": "🎬 Emby 媒体库",
            "type": 1,
            "api": f"{base_url}/tvbox/emby",
            "searchable": 0,
            "quickSearch": 0,
            "filterable": 0,
        })

    # 3. 网页里配置的每个采集站（type=1，苹果CMS JSON接口）
    for idx, src in enumerate(api_sources):
        if not src.get("enabled", True):
            continue
        if not src.get("tvbox_enabled", True):
            continue
        safe_key = re.sub(r'[^a-zA-Z0-9_]', '_', src["name"]).strip('_') or f"source_{idx}"
        sites.append({
            "key": f"src_{idx}_{safe_key}",
            "name": src["name"],
            "type": 1,
            "api": f"{base_url}/tvbox/source/{idx}",
            "searchable": 1,
            "quickSearch": 1,
            "filterable": 1,
        })

    config = {
        "spider": "",
        "wallpaper": "",
        "sites": sites,
        "doh": [],
        "lives": [],
        "parses": [],
        "flags": ["youku", "qq", "iqiyi", "fun", "le", "sohu", "pptv"],
        "rules": [],
        "ads": [],
    }
    return JSONResponse(content=config)


@app.get("/tvbox")
async def tvbox_config(request: Request):
    """
    标准苹果CMS接口，TVBox 用 type=1 调用。
    - ac=list  → 返回分类列表（首页展示用）
    - ac=detail → 返回影片详情+播放地址
    - 不传 ac  → 同 ac=list
    """
    base_url = str(request.base_url).rstrip("/")
    ac = request.query_params.get("ac", "list")
    video_files = _get_video_files()

    # 固定只有一个分类"本地视频"
    class_list = [{"type_id": 1, "type_name": "本地视频"}]

    if ac == "list":
        # 首页请求：返回分类 + 最近视频列表（不含播放地址）
        vod_list = [{
            "vod_id": i + 1,
            "vod_name": f.stem,
            "type_id": 1,
            "type_name": "本地视频",
            "vod_pic": "",
            "vod_remarks": f"{round(f.stat().st_size/1024/1024):.0f}MB",
            "vod_time": datetime.fromtimestamp(f.stat().st_ctime).strftime("%Y-%m-%d %H:%M:%S"),
        } for i, f in enumerate(video_files)]
        return JSONResponse({
            "code": 1, "msg": "数据列表",
            "page": 1, "pagecount": 1,
            "limit": len(vod_list), "total": len(vod_list),
            "list": vod_list,
            "class": class_list,
        })

    elif ac == "detail":
        # 详情请求：返回带播放地址的完整数据
        # 支持按 ids 筛选（TVBox 点击某个影片时传 ids=vod_id）
        ids_param = request.query_params.get("ids", "")
        id_set = set(ids_param.split(",")) if ids_param else set()

        vod_list = []
        for i, f in enumerate(video_files):
            vod_id = i + 1
            if id_set and str(vod_id) not in id_set:
                continue
            stream_url = f"{base_url}/api/stream/{quote(f.name)}"
            vod_list.append({
                "vod_id": vod_id,
                "vod_name": f.stem,
                "type_id": 1,
                "type_name": "本地视频",
                "vod_pic": "",
                "vod_remarks": f"{round(f.stat().st_size/1024/1024):.0f}MB",
                "vod_time": datetime.fromtimestamp(f.stat().st_ctime).strftime("%Y-%m-%d %H:%M:%S"),
                "vod_play_from": "local",
                "vod_play_url": f"{f.stem}${stream_url}",
            })
        return JSONResponse({
            "code": 1, "msg": "数据列表",
            "page": 1, "pagecount": 1,
            "limit": len(vod_list), "total": len(vod_list),
            "list": vod_list,
            "class": class_list,
        })

    # 其他 ac 值返回空
    return JSONResponse({"code": 1, "msg": "ok", "list": [], "class": class_list})


def _emby_stream_url(item_id: str) -> str:
    """Return a direct Emby stream URL suitable for TVBox playback."""
    return (f"{emby_config['url']}/Videos/{item_id}/stream"
            f"?api_key={emby_config['api_key']}&static=true")


@app.get("/tvbox/emby")
async def tvbox_emby(request: Request):
    """
    TVBox Apple CMS endpoint for Emby content.
    ac=list   → Emby libraries as classes; movies/series as vod list (filter by t=<class idx>)
    ac=detail → item details with direct playback URLs; series exposes all episodes
    """
    url = emby_config.get("url", "")
    api_key = emby_config.get("api_key", "")
    user_id = emby_config.get("user_id", "")
    base_url = str(request.base_url).rstrip("/")

    if not url or not api_key or not user_id:
        return JSONResponse({"code": -1, "msg": "Emby 未完整配置", "list": [], "class": []})

    ac = request.query_params.get("ac", "list")

    async with _make_http_client() as client:
        # Always fetch libraries so we can build the class list
        try:
            lib_r = await client.get(
                f"{url}/Users/{user_id}/Views",
                headers={"X-Emby-Token": api_key}, timeout=10,
            )
            lib_r.raise_for_status()
            libraries = [{"id": i["Id"], "name": i["Name"]}
                         for i in lib_r.json().get("Items", [])]
        except Exception:
            libraries = []

        # type_id is 1-based index into libraries list
        class_list = [{"type_id": idx + 1, "type_name": lib["name"]}
                      for idx, lib in enumerate(libraries)]

        if ac == "list":
            t_param = request.query_params.get("t", "0")
            pg = max(1, int(request.query_params.get("pg", "1")))
            limit = 40
            try:
                t_idx = int(t_param)
            except ValueError:
                t_idx = 0
            parent_id = (libraries[t_idx - 1]["id"]
                         if 0 < t_idx <= len(libraries) else "")

            params = {
                "IncludeItemTypes": "Movie,Series",
                "Recursive": "true",
                "Fields": "PrimaryImageAspectRatio,Overview,ProductionYear",
                "StartIndex": (pg - 1) * limit,
                "Limit": limit,
                "SortBy": "DateCreated",
                "SortOrder": "Descending",
            }
            if parent_id:
                params["ParentId"] = parent_id

            try:
                r = await client.get(f"{url}/Users/{user_id}/Items",
                                     headers={"X-Emby-Token": api_key},
                                     params=params, timeout=15)
                r.raise_for_status()
                data = r.json()
            except Exception as e:
                return JSONResponse({"code": -1, "msg": str(e), "list": [], "class": class_list})

            vod_list = []
            for item in data.get("Items", []):
                has_img = bool((item.get("ImageTags") or {}).get("Primary"))
                vod_list.append({
                    "vod_id": item["Id"],
                    "vod_name": item["Name"],
                    "type_id": t_idx or 1,
                    "type_name": item.get("Type", ""),
                    "vod_pic": f"{base_url}/api/emby/image/{item['Id']}?w=200" if has_img else "",
                    "vod_remarks": str(item.get("ProductionYear", "")),
                    "vod_time": "",
                })

            total = data.get("TotalRecordCount", len(vod_list))
            pagecount = max(1, (total + limit - 1) // limit)
            return JSONResponse({
                "code": 1, "msg": "数据列表",
                "page": pg, "pagecount": pagecount,
                "limit": limit, "total": total,
                "list": vod_list,
                "class": class_list,
            })

        elif ac == "detail":
            ids_param = request.query_params.get("ids", "")
            vod_list = []

            for item_id in ids_param.split(","):
                item_id = item_id.strip()
                if not item_id:
                    continue
                try:
                    r = await client.get(
                        f"{url}/Users/{user_id}/Items/{item_id}",
                        headers={"X-Emby-Token": api_key},
                        params={"Fields": "Overview,ProductionYear,OfficialRating,People,Genres,Studios"},
                        timeout=10,
                    )
                    r.raise_for_status()
                    item = r.json()
                except Exception:
                    continue

                has_img = bool((item.get("ImageTags") or {}).get("Primary"))
                item_type = item.get("Type", "")
                people = item.get("People") or []
                actors = [p.get("Name", "") for p in people if p.get("Name") and p.get("Type") in {"Actor", "GuestStar"}]
                directors = [p.get("Name", "") for p in people if p.get("Name") and p.get("Type") == "Director"]
                genres = item.get("Genres") or []

                if item_type == "Series":
                    try:
                        ep_r = await client.get(
                            f"{url}/Shows/{item_id}/Episodes",
                            headers={"X-Emby-Token": api_key},
                            params={"UserId": user_id, "Fields": "Overview", "Limit": 500},
                            timeout=15,
                        )
                        ep_r.raise_for_status()
                        episodes = ep_r.json().get("Items", [])
                    except Exception:
                        episodes = []
                    parts = []
                    for ep in episodes:
                        s = ep.get("ParentIndexNumber", 1)
                        e = ep.get("IndexNumber", 0)
                        label = f"S{s}E{e:02d} {ep.get('Name', '')}"
                        parts.append(f"{label}${base_url}/api/emby/stream/{ep['Id']}")
                    play_url = "#".join(parts) if parts else f"暂无剧集${url}"
                else:
                    play_url = f"播放${base_url}/api/emby/stream/{item_id}"

                vod_list.append({
                    "vod_id": item_id,
                    "vod_name": item.get("Name", ""),
                    "type_id": 1,
                    "type_name": item_type,
                    "vod_pic": f"{base_url}/api/emby/image/{item_id}?w=320" if has_img else "",
                    "vod_year": item.get("ProductionYear", ""),
                    "vod_remarks": str(item.get("ProductionYear", "")),
                    "vod_actor": ",".join(actors[:12]),
                    "vod_director": ",".join(directors[:4]),
                    "vod_class": ",".join(genres[:4]),
                    "vod_content": (item.get("Overview") or "")[:500],
                    "vod_time": "",
                    "vod_play_from": "Emby",
                    "vod_play_url": play_url,
                })

            return JSONResponse({
                "code": 1, "msg": "数据列表",
                "page": 1, "pagecount": 1,
                "limit": len(vod_list), "total": len(vod_list),
                "list": vod_list,
                "class": class_list,
            })

    return JSONResponse({"code": 1, "msg": "ok", "list": [], "class": class_list})


app.mount("/videos", StaticFiles(directory=str(VIDEO_DIR)), name="videos")


# ── Emby 集成 ──────────────────────────────────────────

@app.get("/api/emby/status")
async def emby_status():
    return {
        "configured": bool(emby_config.get("url") and emby_config.get("api_key")),
        "need_auth": bool(emby_config.get("password_hash", "")),
    }


@app.post("/api/emby/auth")
async def emby_auth(data: dict):
    pw_hash = emby_config.get("password_hash", "")
    if not pw_hash:
        return {"ok": True}
    if hashlib.sha256(data.get("password", "").encode()).hexdigest() != pw_hash:
        raise HTTPException(401, "密码错误")
    return {"ok": True}


@app.get("/api/emby/config")
async def get_emby_config():
    return {
        "url": emby_config.get("url", ""),
        "user_id": emby_config.get("user_id", ""),
        "has_api_key": bool(emby_config.get("api_key", "")),
        "has_password": bool(emby_config.get("password_hash", "")),
    }


@app.put("/api/emby/config")
async def set_emby_config(data: dict):
    if "url" in data:
        emby_config["url"] = data["url"].rstrip("/")
    if data.get("api_key"):
        emby_config["api_key"] = data["api_key"]
    if "user_id" in data:
        emby_config["user_id"] = data["user_id"]
    if "password" in data:
        pw = data["password"].strip()
        emby_config["password_hash"] = hashlib.sha256(pw.encode()).hexdigest() if pw else ""
    save_data()
    return {"ok": True}


@app.post("/api/emby/test")
async def test_emby():
    url = emby_config.get("url", "")
    api_key = emby_config.get("api_key", "")
    if not url or not api_key:
        return {"ok": False, "error": "请先填写服务器地址和 API Key"}
    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            r = await client.get(f"{url}/System/Info/Public",
                                 headers={"X-Emby-Token": api_key}, timeout=8)
            r.raise_for_status()
            info = r.json()
            return {"ok": True, "msg": f"连接成功：{info.get('ServerName', 'Emby')} v{info.get('Version', '?')}"}
    except httpx.TimeoutException:
        return {"ok": False, "error": "连接超时（8s）"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/emby/users")
async def emby_users():
    url = emby_config.get("url", "")
    api_key = emby_config.get("api_key", "")
    if not url or not api_key:
        raise HTTPException(400, "Emby 未配置")
    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            r = await client.get(f"{url}/Users",
                                 headers={"X-Emby-Token": api_key}, timeout=10)
            r.raise_for_status()
            return [{"id": u["Id"], "name": u["Name"]} for u in r.json()]
    except Exception as e:
        raise HTTPException(502, str(e))


@app.get("/api/emby/libraries")
async def emby_libraries():
    url = emby_config.get("url", "")
    api_key = emby_config.get("api_key", "")
    user_id = emby_config.get("user_id", "")
    if not url or not api_key or not user_id:
        raise HTTPException(400, "Emby 未完整配置")
    try:
        async with _make_http_client() as client:
            r = await client.get(f"{url}/Users/{user_id}/Views",
                                 headers={"X-Emby-Token": api_key}, timeout=10)
            r.raise_for_status()
            items = r.json().get("Items", [])
            libs = []
            for i in items:
                ctype = (i.get("CollectionType") or "").lower()
                name = (i.get("Name") or "").strip()
                if ctype in {"movies", "tvshows", "mixed"} or i.get("Type") == "CollectionFolder":
                    libs.append({"id": i["Id"], "name": name or i["Id"]})
            return libs
    except Exception as e:
        raise HTTPException(502, str(e))


def _map_emby_items(raw_items: list) -> list:
    items = []
    for i in raw_items:
        if i.get("Type") not in {"Movie", "Series"}:
            continue
        has_img = bool((i.get("ImageTags") or {}).get("Primary"))
        thumb = f"/api/emby/image/{i['Id']}?w=200" if has_img else ""
        items.append({
            "id": i["Id"],
            "name": i.get("Name", ""),
            "type": i.get("Type", "Movie"),
            "year": i.get("ProductionYear", ""),
            "overview": (i.get("Overview") or "")[:200],
            "thumb": thumb,
            "rating": i.get("OfficialRating", ""),
        })
    return items


@app.get("/api/emby/items")
async def emby_items(parent_id: str = "", pg: int = 1, limit: int = 40):
    url = emby_config.get("url", "")
    api_key = emby_config.get("api_key", "")
    user_id = emby_config.get("user_id", "")
    if not url or not api_key or not user_id:
        raise HTTPException(400, "Emby 未完整配置")

    try:
        async with _make_http_client() as client:
            params = {
                "IncludeItemTypes": "Movie,Series",
                "Recursive": "true",
                "Fields": "PrimaryImageAspectRatio,Overview,ProductionYear,OfficialRating,People,Genres,Studios,Taglines",
                "StartIndex": (pg - 1) * limit,
                "Limit": limit,
                "SortBy": "SortName",
                "SortOrder": "Ascending",
            }
            if parent_id:
                params["ParentId"] = parent_id
                r = await client.get(f"{url}/Users/{user_id}/Items",
                                     headers={"X-Emby-Token": api_key},
                                     params=params, timeout=12)
                r.raise_for_status()
                data = r.json()
                items = _map_emby_items(data.get("Items", []))
                return {"items": items, "total": data.get("TotalRecordCount", len(items)), "page": pg}

            # root/all view: some Emby servers fail on querying everything at once.
            views_r = await client.get(f"{url}/Users/{user_id}/Views",
                                       headers={"X-Emby-Token": api_key}, timeout=10)
            views_r.raise_for_status()
            views = views_r.json().get("Items", [])
            libs = [v for v in views if (v.get("CollectionType") or "").lower() in {"movies", "tvshows", "mixed"} or v.get("Type") == "CollectionFolder"]
            collected = []
            for lib in libs:
                lib_params = dict(params)
                lib_params["ParentId"] = lib["Id"]
                try:
                    lr = await client.get(f"{url}/Users/{user_id}/Items",
                                          headers={"X-Emby-Token": api_key},
                                          params=lib_params, timeout=12)
                    lr.raise_for_status()
                    collected.extend(lr.json().get("Items", []))
                except Exception:
                    continue
            items = _map_emby_items(collected)
            seen = set()
            dedup = []
            for item in items:
                if item["id"] in seen:
                    continue
                seen.add(item["id"])
                dedup.append(item)
            dedup.sort(key=lambda x: ((x.get("name") or "").lower(), x.get("id") or ""))
            total = len(dedup)
            start = max(0, (pg - 1) * limit)
            return {"items": dedup[start:start+limit], "total": total, "page": pg}
    except Exception as e:
        raise HTTPException(502, str(e))


@app.get("/api/emby/detail/{item_id}")
async def emby_detail(item_id: str):
    url = emby_config.get("url", "")
    api_key = emby_config.get("api_key", "")
    user_id = emby_config.get("user_id", "")
    if not url or not api_key or not user_id:
        raise HTTPException(400, "Emby 未完整配置")
    try:
        async with _make_http_client() as client:
            r = await client.get(
                f"{url}/Users/{user_id}/Items/{item_id}",
                headers={"X-Emby-Token": api_key},
                params={"Fields": "Overview,ProductionYear,OfficialRating,People,Genres,Studios,Taglines,CommunityRating,PremiereDate,RunTimeTicks"},
                timeout=12,
            )
            r.raise_for_status()
            item = r.json()
            people = item.get("People") or []
            people_out = [{
                "id": p.get("Id", ""),
                "name": p.get("Name", ""),
                "type": p.get("Type", ""),
                "role": p.get("Role", ""),
            } for p in people if p.get("Name")]
            return {
                "id": item.get("Id", item_id),
                "name": item.get("Name", ""),
                "type": item.get("Type", "Movie"),
                "year": item.get("ProductionYear", ""),
                "overview": item.get("Overview", ""),
                "rating": item.get("OfficialRating", ""),
                "community_rating": item.get("CommunityRating", ""),
                "premiere_date": item.get("PremiereDate", ""),
                "genres": item.get("Genres") or [],
                "studios": [s.get("Name", "") for s in (item.get("Studios") or []) if s.get("Name")],
                "taglines": item.get("Taglines") or [],
                "people": people_out,
            }
    except Exception as e:
        raise HTTPException(502, str(e))


@app.get("/api/emby/person/{person_id}")
async def emby_person_items(person_id: str, pg: int = 1, limit: int = 40):
    url = emby_config.get("url", "")
    api_key = emby_config.get("api_key", "")
    user_id = emby_config.get("user_id", "")
    if not url or not api_key or not user_id:
        raise HTTPException(400, "Emby 未完整配置")
    try:
        async with _make_http_client() as client:
            params = {
                "PersonIds": person_id,
                "IncludeItemTypes": "Movie,Series",
                "Recursive": "true",
                "Fields": "PrimaryImageAspectRatio,Overview,ProductionYear,OfficialRating,People,Genres",
                "StartIndex": (pg - 1) * limit,
                "Limit": limit,
                "SortBy": "SortName",
                "SortOrder": "Ascending",
            }
            r = await client.get(f"{url}/Users/{user_id}/Items",
                                 headers={"X-Emby-Token": api_key},
                                 params=params, timeout=12)
            r.raise_for_status()
            data = r.json()
            items = _map_emby_items(data.get("Items", []))
            return {"items": items, "total": data.get("TotalRecordCount", len(items)), "page": pg}
    except Exception as e:
        raise HTTPException(502, str(e))


@app.get("/api/emby/episodes/{series_id}")
async def emby_episodes(series_id: str):
    url = emby_config.get("url", "")
    api_key = emby_config.get("api_key", "")
    user_id = emby_config.get("user_id", "")
    if not url or not api_key:
        raise HTTPException(400, "Emby 未配置")
    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            r = await client.get(f"{url}/Shows/{series_id}/Episodes",
                                 headers={"X-Emby-Token": api_key},
                                 params={"UserId": user_id, "Fields": "Overview", "Limit": 500},
                                 timeout=10)
            r.raise_for_status()
            return [{"id": i["Id"], "name": i.get("Name", ""),
                     "season": i.get("ParentIndexNumber", 1),
                     "episode": i.get("IndexNumber", 0)}
                    for i in r.json().get("Items", [])]
    except Exception as e:
        raise HTTPException(502, str(e))


@app.get("/api/emby/image/{item_id}")
async def emby_image(item_id: str, w: int = 200):
    """代理 Emby 封面图片并缓存到本地，减少重复回源开销。"""
    from fastapi.responses import Response
    url = emby_config.get("url", "")
    api_key = emby_config.get("api_key", "")
    if not url or not api_key:
        raise HTTPException(400, "Emby 未配置")
    width = max(120, min(int(w or 200), 600))
    cache_file = EMBY_IMAGE_CACHE_DIR / f"{item_id}_{width}.img"
    if cache_file.exists():
        media_type = "image/jpeg"
        if cache_file.suffix.lower() == '.png':
            media_type = 'image/png'
        return FileResponse(cache_file, media_type=media_type, headers={"Cache-Control": "public, max-age=86400"})
    image_url = (f"{url}/Items/{item_id}/Images/Primary"
                 f"?api_key={api_key}&maxWidth={width}&quality=82")
    try:
        async with _make_http_client() as client:
            r = await client.get(image_url, timeout=10)
            r.raise_for_status()
            content_type = r.headers.get("content-type", "image/jpeg").split(';')[0].strip()
            ext = '.jpg'
            if 'png' in content_type:
                ext = '.png'
            target = EMBY_IMAGE_CACHE_DIR / f"{item_id}_{width}{ext}"
            target.write_bytes(r.content)
            if target != cache_file and cache_file.exists():
                cache_file.unlink(missing_ok=True)
            return Response(
                content=r.content,
                media_type=content_type,
                headers={"Cache-Control": "public, max-age=86400"},
            )
    except Exception:
        raise HTTPException(404, "图片不可用")


@app.api_route("/api/emby/stream/{item_id}", methods=["GET", "HEAD"])
async def emby_stream(item_id: str, request: Request):
    """Reverse-proxy Emby stream for TVBox compatibility."""
    url = emby_config.get("url", "")
    api_key = emby_config.get("api_key", "")
    if not url or not api_key:
        raise HTTPException(400, "Emby 未配置")

    upstream_url = f"{url}/Videos/{item_id}/stream?api_key={api_key}&static=true"
    headers = {}
    range_header = request.headers.get("range") or request.headers.get("Range")
    if range_header:
        headers["Range"] = range_header

    try:
        async with _make_http_client() as client:
            if request.method == "HEAD":
                upstream = await client.head(upstream_url, headers=headers, timeout=20)
                passthrough_headers = {}
                for key in ["content-type", "content-length", "accept-ranges", "content-range", "cache-control", "etag", "last-modified", "content-disposition"]:
                    if key in upstream.headers:
                        passthrough_headers[key] = upstream.headers[key]
                return Response(status_code=upstream.status_code, headers=passthrough_headers)

            upstream = await client.stream("GET", upstream_url, headers=headers, timeout=None)
            await upstream.__aenter__()

            passthrough_headers = {}
            for key in ["content-type", "content-length", "accept-ranges", "content-range", "cache-control", "etag", "last-modified", "content-disposition"]:
                if key in upstream.headers:
                    passthrough_headers[key] = upstream.headers[key]

            async def body_iter():
                try:
                    async for chunk in upstream.aiter_bytes():
                        yield chunk
                finally:
                    await upstream.aclose()

            return StreamingResponse(
                body_iter(),
                status_code=upstream.status_code,
                headers=passthrough_headers,
                media_type=upstream.headers.get("content-type", "video/mp4"),
            )
    except httpx.HTTPStatusError as e:
        raise HTTPException(e.response.status_code, f"Emby 上游返回 HTTP {e.response.status_code}")
    except Exception as e:
        raise HTTPException(502, f"Emby 播放代理失败: {e}")


@app.get("/api/emby/direct/{item_id}")
async def emby_direct(item_id: str):
    url = emby_config.get("url", "")
    api_key = emby_config.get("api_key", "")
    if not url or not api_key:
        raise HTTPException(400, "Emby 未配置")
    return {"url": f"{url}/Videos/{item_id}/stream?api_key={api_key}&static=true"}
