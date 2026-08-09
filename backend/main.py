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

_DEFAULT_USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36"

def _make_http_client(**kwargs) -> httpx.AsyncClient:
    """Shared async HTTP client with consistent timeouts and connection limits.
    带一个常见浏览器 UA——部分采集站会针对默认的 python-httpx UA 单独限制搜索等接口。"""
    kwargs.setdefault("follow_redirects", True)
    kwargs.setdefault("headers", {"User-Agent": _DEFAULT_USER_AGENT})
    return httpx.AsyncClient(
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
emby_config: dict = {"url": "", "api_key": "", "user_id": "", "password_hash": ""}  # legacy fallback
emby_servers: list = []
_download_semaphore: Optional[asyncio.Semaphore] = None


def _normalize_emby_server(server: dict, idx: int = 0) -> dict:
    sid = str(server.get("id") or f"emby_{idx + 1}").strip()
    sid = re.sub(r"[^a-zA-Z0-9_-]", "_", sid) or f"emby_{idx + 1}"
    return {
        "id": sid,
        "name": (server.get("name") or f"Emby {idx + 1}").strip() or f"Emby {idx + 1}",
        "url": (server.get("url") or "").rstrip("/"),
        "api_key": server.get("api_key") or "",
        "user_id": server.get("user_id") or "",
        "password_hash": server.get("password_hash") or "",
        "enabled": server.get("enabled", True) is not False,
        "tvbox_enabled": server.get("tvbox_enabled", True) is not False,
        "allowed_library_ids": [str(x) for x in (server.get("allowed_library_ids") or []) if str(x).strip()],
        "hidden_library_ids": [str(x) for x in (server.get("hidden_library_ids") or []) if str(x).strip()],
    }


def _migrate_legacy_emby_servers(raw_servers: list) -> list:
    servers = [_normalize_emby_server(s, i) for i, s in enumerate(raw_servers or []) if isinstance(s, dict)]
    if servers:
        return servers
    if emby_config.get("url") or emby_config.get("api_key") or emby_config.get("user_id") or emby_config.get("password_hash"):
        return [_normalize_emby_server({
            "id": "default",
            "name": "默认 Emby",
            "url": emby_config.get("url", ""),
            "api_key": emby_config.get("api_key", ""),
            "user_id": emby_config.get("user_id", ""),
            "password_hash": emby_config.get("password_hash", ""),
            "enabled": True,
            "tvbox_enabled": settings.get("tvbox_emby_enabled", True),
            "allowed_library_ids": [],
            "hidden_library_ids": [],
        }, 0)]
    return []


def _ensure_emby_servers_loaded():
    global emby_servers
    if not emby_servers:
        emby_servers = _migrate_legacy_emby_servers([])


def _get_emby_server(server_id: str | None = None, require_enabled: bool = True) -> dict:
    _ensure_emby_servers_loaded()
    if not emby_servers:
        raise HTTPException(400, "Emby 未配置")
    if not server_id:
        for s in emby_servers:
            if not require_enabled or s.get("enabled", True):
                return s
        raise HTTPException(400, "没有可用的 Emby 源")
    for s in emby_servers:
        if s.get("id") == server_id:
            if require_enabled and not s.get("enabled", True):
                raise HTTPException(403, "Emby 源已禁用")
            return s
    raise HTTPException(404, "Emby 源不存在")


def _emby_public_server_summary(server: dict) -> dict:
    return {
        "id": server["id"],
        "name": server.get("name", server["id"]),
        "configured": bool(server.get("url") and server.get("api_key") and server.get("user_id")),
        "need_auth": bool(server.get("password_hash", "")),
        "tvbox_enabled": server.get("tvbox_enabled", True),
        "enabled": server.get("enabled", True),
        "has_filters": bool(server.get("allowed_library_ids") or server.get("hidden_library_ids")),
    }


async def _fetch_emby_views(server: dict) -> list:
    url = server.get("url", "")
    api_key = server.get("api_key", "")
    user_id = server.get("user_id", "")
    if not url or not api_key or not user_id:
        raise HTTPException(400, "Emby 未完整配置")
    async with _make_http_client() as client:
        r = await client.get(f"{url}/Users/{user_id}/Views", headers={"X-Emby-Token": api_key}, timeout=10)
        r.raise_for_status()
        items = r.json().get("Items", [])
    libs = []
    allowed = set(server.get("allowed_library_ids") or [])
    hidden = set(server.get("hidden_library_ids") or [])
    for i in items:
        ctype = (i.get("CollectionType") or "").lower()
        if ctype not in {"movies", "tvshows", "mixed"} and i.get("Type") != "CollectionFolder":
            continue
        iid = str(i.get("Id") or "")
        if allowed and iid not in allowed:
            continue
        if iid in hidden:
            continue
        libs.append(i)
    return libs


# ─── 持久化 ────────────────────────────────────────────

def save_data():
    data = {
        "api_sources": api_sources,
        "settings": settings,
        "emby_config": emby_config,
        "emby_servers": emby_servers,
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
    global api_sources, emby_servers
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
        emby_servers = _migrate_legacy_emby_servers(data.get("emby_servers", []))
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
    media_type: Optional[str] = "video"  # video | audio

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
    def __init__(self, task_id, name, url, source="manual", media_type="video"):
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
        self.media_type = media_type  # video | audio


_VIDEO_EXTS = {".mp4", ".mkv", ".ts", ".m4v", ".m2ts", ".mpeg"}
_AUDIO_ONLY_EXTS = {".m4a", ".mp3", ".aac", ".flac", ".wav", ".opus", ".ogg"}
# .webm can be video or audio-only; include in both discovery sets
_AUDIO_EXTS = _AUDIO_ONLY_EXTS | {".webm"}
_ALL_MEDIA_EXTS = _VIDEO_EXTS | _AUDIO_EXTS

def _safe_filename(name: str, max_bytes: int = 200) -> str:
    """按字节截断文件名（保证 UTF-8 多字节字符不被截断到中间）。
    预留 55 字节给扩展名及 N_m3u8DL-RE 附加的语言标签。
    """
    encoded = name.encode("utf-8")
    if len(encoded) <= max_bytes:
        return name
    truncated = encoded[:max_bytes].decode("utf-8", errors="ignore")
    return truncated.rstrip("_") or "video"

def _find_output_file(output_name: str, media_type: str = "video"):
    """查找下载完成后的输出文件。
    N_m3u8DL-RE 可能附加语言/分辨率标签（如 name.zh.mp4）。
    根据任务类型优先匹配对应后缀，避免音频任务误拿到旧视频文件，
    再做前缀 glob，取同类型里最大的媒体文件。
    """
    preferred_exts = (*_AUDIO_EXTS,) if media_type == "audio" else (*_VIDEO_EXTS, *_AUDIO_EXTS)
    fallback_exts = (*_VIDEO_EXTS, *_AUDIO_EXTS) if media_type == "audio" else ()

    for ext in (*preferred_exts, *fallback_exts):
        p = VIDEO_DIR / f"{output_name}{ext}"
        if p.exists():
            return p

    preferred_set = set(preferred_exts)
    fallback_set = set(fallback_exts)
    preferred_candidates = [
        p for p in VIDEO_DIR.glob(f"{output_name}*")
        if p.suffix.lower() in preferred_set
    ]
    if preferred_candidates:
        return max(preferred_candidates, key=lambda p: p.stat().st_size)

    fallback_candidates = [
        p for p in VIDEO_DIR.glob(f"{output_name}*")
        if p.suffix.lower() in fallback_set
    ]
    return max(fallback_candidates, key=lambda p: p.stat().st_size) if fallback_candidates else None



def _verify_file(path: Path) -> tuple:
    """
    校验文件完整性。
    返回 (ok: bool, reason: str)
    - 文件存在且大于最小尺寸视为完整
    - 用 ffprobe 做格式校验：纯音频文件检查 a:0，视频文件检查 v:0，
      .webm 接受 v:0 或 a:0 任一存在。
    """
    if not path.exists():
        return False, "文件不存在"
    size = path.stat().st_size
    if size < MIN_FILE_SIZE:
        return False, f"文件过小（{size} 字节），可能已损坏"
    ffprobe = shutil.which("ffprobe")
    if ffprobe:
        import subprocess
        suffix = path.suffix.lower()

        def _probe(stream_sel: str) -> bool:
            r = subprocess.run(
                [ffprobe, "-v", "error", "-select_streams", stream_sel,
                 "-show_entries", "stream=codec_type", "-of", "csv=p=0", str(path)],
                capture_output=True, text=True, timeout=30,
            )
            return r.returncode == 0 and bool(r.stdout.strip())

        if suffix in _AUDIO_ONLY_EXTS:
            if not _probe("a:0"):
                return False, "ffprobe 校验失败，音频流可能损坏"
        elif suffix == ".webm":
            # webm 可能是视频或纯音频，任一流存在即视为有效
            if not (_probe("v:0") or _probe("a:0")):
                return False, "ffprobe 校验失败，未找到有效音视频流"
        else:
            if not _probe("v:0"):
                return False, "ffprobe 校验失败，视频流可能损坏"
    return True, "ok"


async def run_download(task_id: str, url: str, output_name: str, media_type: str = "video"):
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
            await _run_download_core(task_id, url, output_name, media_type)
    else:
        await _run_download_core(task_id, url, output_name, media_type)


async def _run_download_core(task_id: str, url: str, output_name: str, media_type: str = "video"):
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
            ytdlp_exists = Path(ytdlp).exists()

            if media_type == "audio" and ytdlp_exists:
                out_template = str(VIDEO_DIR / f"{output_name}.%(ext)s")
                cmd = [
                    ytdlp, url,
                    "-f", "bestaudio/best",
                    "--extract-audio",
                    "--audio-format", "m4a",
                    "--audio-quality", "0",
                    "--no-part",
                    "-o", out_template,
                    "--newline",
                ]
            elif nm3u8:
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
            elif ytdlp_exists:
                out_template = str(VIDEO_DIR / f"{output_name}.%(ext)s")
                cmd = [
                    ytdlp, url,
                    "--hls-prefer-native",
                    "--no-part",
                    "-o", out_template,
                    "--newline",
                ]
            elif ffmpeg:
                out_path = VIDEO_DIR / f"{output_name}.{'m4a' if media_type == 'audio' else 'mp4'}"
                cmd = [ffmpeg, "-i", url]
                if media_type == "audio":
                    cmd += ["-vn", "-c:a", "aac", "-b:a", "192k"]
                else:
                    cmd += ["-c", "copy"]
                cmd += ["-y", str(out_path)]
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
            out_file = _find_output_file(output_name, media_type=media_type)
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

def _is_allowed_public_source_url(url: str) -> bool:
    """跟 _is_allowed_source_url 一样，但额外要求这个接口没被禁用、也没设置『公开页隐藏』。"""
    norm = normalize_api_url(url)
    return any(
        normalize_api_url(s["url"]) == norm and s.get("enabled", True) and s.get("public_enabled", True)
        for s in api_sources
    )

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

    async with _make_http_client() as client:
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


async def _proxy_catalog(source_url: str, ac: str, pg: int, t: int, ids: str):
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


@app.get("/api/catalog")
async def catalog(source_url: str, ac: str = "list", pg: int = 1, t: int = 0, ids: str = ""):
    """代理采集站请求，只允许已配置接口，供后台管理页调用（能看到所有接口，不管有没有对公开页隐藏）"""
    if not _is_allowed_source_url(source_url):
        raise HTTPException(403, "该接口未在系统中配置，拒绝访问")
    return await _proxy_catalog(source_url, ac, pg, t, ids)


@app.get("/api/public/catalog")
async def public_catalog(source_url: str, ac: str = "list", pg: int = 1, t: int = 0, ids: str = ""):
    """给公开主页用的代理，比 /api/catalog 多一层限制：接口被禁用或设置了『公开页隐藏』
    时直接拒绝，不能靠猜/拿到 source_url 绕过隐藏设置。"""
    if not _is_allowed_public_source_url(source_url):
        raise HTTPException(403, "该接口不可用或已在公开页隐藏")
    return await _proxy_catalog(source_url, ac, pg, t, ids)


# ── 接口管理 ──

@app.get("/api/sources")
async def list_sources():
    return api_sources

@app.get("/api/public/sources")
async def list_public_sources():
    """给公开主页用的接口列表，只暴露启用且未设为『公开页隐藏』的接口，
    不走 /api/sources（那个是给后台管理用的，会把所有接口原样列出来）。"""
    return [
        {"name": s["name"], "url": s["url"]}
        for s in api_sources
        if s.get("enabled", True) and s.get("public_enabled", True)
    ]

@app.post("/api/sources")
async def add_source(src: ApiSource):
    normalized = normalize_api_url(src.url)
    for s in api_sources:
        if normalize_api_url(s["url"]) == normalized:
            raise HTTPException(400, "该接口已存在")
    api_sources.append({"name": src.name.strip(), "url": normalized, "enabled": True, "tvbox_enabled": True, "public_enabled": True})
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

@app.patch("/api/sources/{index}/public-toggle")
async def public_toggle_source(index: int):
    if index < 0 or index >= len(api_sources):
        raise HTTPException(404, "不存在")
    api_sources[index]["public_enabled"] = not api_sources[index].get("public_enabled", True)
    save_data()
    return {"public_enabled": api_sources[index]["public_enabled"]}

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
    async with _make_http_client() as client:
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
    media_type = (req.media_type or "video").lower()
    if media_type not in {"video", "audio"}:
        media_type = "video"
    task = TaskStatus(task_id, name, req.url, media_type=media_type)
    tasks[task_id] = task
    asyncio.create_task(run_download(task_id, req.url, name, media_type=media_type))
    return {"task_id": task_id, "name": name, "media_type": media_type}

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
             "source": t.source, "media_type": getattr(t, 'media_type', 'video')}
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

@app.get("/api/videos/files")
async def list_all_files():
    files = []
    try:
        entries = sorted(VIDEO_DIR.iterdir(), key=lambda x: x.stat().st_ctime, reverse=True)
    except OSError:
        return []
    for f in entries:
        if not f.is_file() or f.name.startswith('.'):
            continue
        try:
            files.append({"filename": f.name, "size_mb": round(f.stat().st_size / 1024 / 1024, 2)})
        except OSError:
            continue
    return files

@app.delete("/api/videos/{filename}")
async def delete_video(filename: str):
    if "/" in filename or ".." in filename:
        raise HTTPException(400, "非法文件名")
    path = VIDEO_DIR / filename
    if not path.exists():
        raise HTTPException(404, "文件不存在")
    path.unlink()
    return {"ok": True}

class RenameVideoRequest(BaseModel):
    name: str

class MergeMediaRequest(BaseModel):
    video_file: str
    audio_file: str
    output_name: str

@app.patch("/api/videos/{filename}/rename")
async def rename_video(filename: str, data: RenameVideoRequest):
    if "/" in filename or ".." in filename:
        raise HTTPException(400, "非法文件名")
    path = VIDEO_DIR / filename
    if not path.exists():
        raise HTTPException(404, "文件不存在")
    new_name = _safe_filename(re.sub(r'[^\w\u4e00-\u9fff\-]', '_', data.name.strip()))
    if not new_name:
        raise HTTPException(400, "新名称不能为空")
    new_path = path.with_name(new_name + path.suffix)
    if new_path.exists() and new_path != path:
        raise HTTPException(400, "目标文件名已存在")
    path.rename(new_path)
    return {"ok": True, "filename": new_path.name, "name": new_path.stem}

@app.post("/api/videos/merge")
async def merge_video_audio(data: MergeMediaRequest):
    for field in [data.video_file, data.audio_file]:
        if "/" in field or ".." in field:
            raise HTTPException(400, "非法文件名")
    video_path = VIDEO_DIR / data.video_file
    audio_path = VIDEO_DIR / data.audio_file
    if not video_path.exists() or not audio_path.exists():
        raise HTTPException(404, "视频或音频文件不存在")
    output_stem = _safe_filename(re.sub(r'[^\w\u4e00-\u9fff\-]', '_', data.output_name.strip()))
    if not output_stem:
        raise HTTPException(400, "输出名称不能为空")
    output_path = VIDEO_DIR / f"{output_stem}.mp4"
    if output_path.exists():
        raise HTTPException(400, "目标文件已存在")
    ffmpeg = shutil.which("ffmpeg")
    if not ffmpeg:
        raise HTTPException(500, "系统未安装 ffmpeg")
    import subprocess
    cmd = [
        ffmpeg, "-y",
        "-i", str(video_path),
        "-i", str(audio_path),
        "-map", "0:v:0",
        "-map", "1:a:0",
        "-c:v", "copy",
        "-c:a", "aac",
        "-shortest",
        str(output_path),
    ]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=7200)
        if result.returncode != 0:
            output_path.unlink(missing_ok=True)
            raise HTTPException(500, (result.stderr or result.stdout or "ffmpeg 合并失败")[-1200:])
        return {"ok": True, "filename": output_path.name, "name": output_path.stem}
    except subprocess.TimeoutExpired:
        output_path.unlink(missing_ok=True)
        raise HTTPException(500, "合并超时")


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
        async with _make_http_client() as client:
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


def _tvbox_emby_site(server: dict, base_url: str) -> dict:
    return {
        "key": f"emby_{server['id']}",
        "name": f"🎬 {server.get('name', 'Emby')}",
        "type": 1,
        "api": f"{base_url}/tvbox/emby/{server['id']}",
        "searchable": 0,
        "quickSearch": 0,
        "filterable": 0,
    }


@app.get("/tvbox/source")
async def tvbox_source(request: Request):
    """
    TVBox 多仓配置：
    - 本地视频库：type=1 内置JSON直接渲染
    - Emby：每个已启用 TVBox 暴露的 Emby 单独一个站点
    - 采集站源：type=1 苹果CMS，直接透传网页里配置的每个接口
    """
    base_url = str(request.base_url).rstrip("/")
    sites = []

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

    if settings.get("tvbox_emby_enabled", True):
        for server in emby_servers:
            if not server.get("enabled", True):
                continue
            if not server.get("tvbox_enabled", True):
                continue
            if not (server.get("url") and server.get("api_key") and server.get("user_id")):
                continue
            sites.append(_tvbox_emby_site(server, base_url))

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
        "sites": sites,
        "lives": [],
        "parses": [],
        "spider": "",
        "wallpaper": "",
        "logo": "",
    }
    return JSONResponse(config)


def _emby_stream_url(server: dict, item_id: str) -> str:
    return f"{server['url']}/Videos/{item_id}/stream?api_key={server['api_key']}&static=true"


@app.get("/tvbox/emby/{server_id}")
async def tvbox_emby(server_id: str, request: Request):
    server = _get_emby_server(server_id)
    url = server.get("url", "")
    api_key = server.get("api_key", "")
    user_id = server.get("user_id", "")
    base_url = str(request.base_url).rstrip("/")
    if not url or not api_key or not user_id:
        return JSONResponse({"code": -1, "msg": "Emby 未完整配置", "list": [], "class": []})
    ac = request.query_params.get("ac", "list")
    if ac == "detail" and not request.query_params.get("ids", "").strip():
        # 部分 TVBox 客户端浏览分类时用 ac=detail&t=&pg=&f= 而不带 ids，
        # 这种情况应按列表请求处理，否则会因为没有 ids 而返回空列表
        ac = "list"
    async with _make_http_client() as client:
        try:
            raw_libraries = await _fetch_emby_views(server)
            libraries = [{"id": i["Id"], "name": i["Name"]} for i in raw_libraries]
        except Exception:
            libraries = []
        class_list = [{"type_id": idx + 1, "type_name": lib["name"]} for idx, lib in enumerate(libraries)]
        if ac == "list":
            t_param = request.query_params.get("t", "0")
            pg = max(1, int(request.query_params.get("pg", "1")))
            limit = 40
            try:
                t_idx = int(t_param)
            except ValueError:
                t_idx = 0
            parent_id = (libraries[t_idx - 1]["id"] if 0 < t_idx <= len(libraries) else "")
            base_params = {
                "IncludeItemTypes": "Movie,Series",
                "Recursive": "true",
                "Fields": "PrimaryImageAspectRatio,Overview,ProductionYear",
                "SortBy": "DateCreated",
                "SortOrder": "Descending",
            }
            has_filter = bool(server.get("allowed_library_ids") or server.get("hidden_library_ids"))
            if parent_id or not has_filter:
                # 选中了具体分类，或者根本没配过滤规则：单次查询就行，ParentId 为空时
                # 是"全部"（未配过滤时可以放心不带 ParentId 查全库）
                params = {**base_params, "StartIndex": (pg - 1) * limit, "Limit": limit}
                if parent_id:
                    params["ParentId"] = parent_id
                try:
                    r = await client.get(f"{url}/Users/{user_id}/Items", headers={"X-Emby-Token": api_key}, params=params, timeout=15)
                    r.raise_for_status()
                    data = r.json()
                except Exception as e:
                    return JSONResponse({"code": -1, "msg": str(e), "list": [], "class": class_list})
                raw_items = data.get("Items", [])
                total = data.get("TotalRecordCount", len(raw_items))
            else:
                # "全部"这个视图配了库过滤规则时，不能不带 ParentId 直接查（会绕过白名单/
                # 黑名单，隐藏库的内容照样会出现在"全部"里）。改成只对过滤后可见的每个库
                # 分别查、合并再按时间重新排序分页
                async def _fetch_lib_items(lib_id: str):
                    p = {**base_params, "ParentId": lib_id, "StartIndex": 0, "Limit": pg * limit}
                    try:
                        r = await client.get(f"{url}/Users/{user_id}/Items", headers={"X-Emby-Token": api_key}, params=p, timeout=15)
                        r.raise_for_status()
                        d = r.json()
                        return d.get("Items", []), d.get("TotalRecordCount", 0)
                    except Exception:
                        return [], 0
                results = await asyncio.gather(*(_fetch_lib_items(lib["id"]) for lib in libraries))
                merged = []
                total = 0
                for items, cnt in results:
                    merged.extend(items)
                    total += cnt
                merged.sort(key=lambda it: it.get("DateCreated", ""), reverse=True)
                raw_items = merged[(pg - 1) * limit: pg * limit]
            vod_list = []
            for item in raw_items:
                has_img = bool((item.get("ImageTags") or {}).get("Primary"))
                vod_list.append({
                    "vod_id": item["Id"],
                    "vod_name": item["Name"],
                    "type_id": t_idx or 1,
                    "type_name": item.get("Type", ""),
                    "vod_pic": f"{base_url}/api/emby/{server_id}/image/{item['Id']}?w=200" if has_img else "",
                    "vod_remarks": str(item.get("ProductionYear", "")),
                    "vod_time": "",
                })
            pagecount = max(1, (total + limit - 1) // limit)
            return JSONResponse({"code": 1, "msg": "数据列表", "page": pg, "pagecount": pagecount, "limit": limit, "total": total, "list": vod_list, "class": class_list})
        elif ac == "detail":
            ids_param = request.query_params.get("ids", "")
            vod_list = []
            for item_id in ids_param.split(","):
                item_id = item_id.strip()
                if not item_id:
                    continue
                try:
                    r = await client.get(f"{url}/Users/{user_id}/Items/{item_id}", headers={"X-Emby-Token": api_key}, params={"Fields": "Overview,ProductionYear,OfficialRating,People,Genres,Studios"}, timeout=10)
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
                        ep_r = await client.get(f"{url}/Shows/{item_id}/Episodes", headers={"X-Emby-Token": api_key}, params={"UserId": user_id, "Fields": "Overview", "Limit": 500}, timeout=15)
                        ep_r.raise_for_status()
                        episodes = ep_r.json().get("Items", [])
                    except Exception:
                        episodes = []
                    parts = []
                    for ep in episodes:
                        s = ep.get("ParentIndexNumber", 1)
                        e = ep.get("IndexNumber", 0)
                        label = f"S{s}E{e:02d} {ep.get('Name', '')}"
                        parts.append(f"{label}${url}/Videos/{ep['Id']}/stream.mp4?api_key={api_key}&static=true")
                    play_url = "#".join(parts) if parts else f"暂无剧集${url}"
                else:
                    play_url = f"播放${url}/Videos/{item_id}/stream.mp4?api_key={api_key}&static=true"
                vod_list.append({
                    "vod_id": item_id,
                    "vod_name": item.get("Name", ""),
                    "type_id": 1,
                    "type_name": item_type,
                    "vod_pic": f"{base_url}/api/emby/{server_id}/image/{item_id}?w=320" if has_img else "",
                    "vod_year": item.get("ProductionYear", ""),
                    "vod_remarks": str(item.get("ProductionYear", "")),
                    "vod_actor": ",".join(actors[:12]),
                    "vod_director": ",".join(directors[:4]),
                    "vod_class": ",".join(genres[:4]),
                    "vod_content": (item.get("Overview") or "")[:500],
                    "vod_time": "",
                    "vod_play_from": server.get("name", "Emby"),
                    "vod_play_url": play_url,
                })
            return JSONResponse({"code": 1, "msg": "数据列表", "page": 1, "pagecount": 1, "limit": len(vod_list), "total": len(vod_list), "list": vod_list, "class": class_list})
    return JSONResponse({"code": 1, "msg": "ok", "list": [], "class": class_list})


app.mount("/videos", StaticFiles(directory=str(VIDEO_DIR)), name="videos")


# ── Emby 集成 ──────────────────────────────────────────

class EmbyServerIn(BaseModel):
    id: Optional[str] = None
    name: Optional[str] = None
    url: Optional[str] = None
    api_key: Optional[str] = None
    user_id: Optional[str] = None
    password: Optional[str] = None
    enabled: Optional[bool] = None
    tvbox_enabled: Optional[bool] = None
    allowed_library_ids: Optional[List[str]] = None
    hidden_library_ids: Optional[List[str]] = None


def _emby_server_admin_summary(server: dict) -> dict:
    return {
        "id": server["id"],
        "name": server.get("name", server["id"]),
        "url": server.get("url", ""),
        "user_id": server.get("user_id", ""),
        "enabled": server.get("enabled", True),
        "tvbox_enabled": server.get("tvbox_enabled", True),
        "allowed_library_ids": server.get("allowed_library_ids", []),
        "hidden_library_ids": server.get("hidden_library_ids", []),
        "has_api_key": bool(server.get("api_key", "")),
        "has_password": bool(server.get("password_hash", "")),
    }


@app.get("/api/emby/status")
async def emby_status():
    active = [_emby_public_server_summary(s) for s in emby_servers if s.get("enabled", True)]
    first = active[0] if active else {"configured": False, "need_auth": False}
    return {**first, "servers": active}


@app.get("/api/emby/servers")
async def list_emby_servers():
    return [_emby_server_admin_summary(s) for s in emby_servers]


@app.post("/api/emby/servers")
async def create_emby_server(data: EmbyServerIn):
    payload = data.model_dump()
    server = _normalize_emby_server(payload, len(emby_servers))
    if any(s.get("id") == server["id"] for s in emby_servers):
        raise HTTPException(400, "Emby 源 ID 已存在")
    if "password" in payload and payload.get("password") is not None:
        pw = (payload.get("password") or "").strip()
        server["password_hash"] = hashlib.sha256(pw.encode()).hexdigest() if pw else ""
    emby_servers.append(server)
    save_data()
    return _emby_server_admin_summary(server)


@app.put("/api/emby/servers/{server_id}")
async def update_emby_server(server_id: str, data: EmbyServerIn):
    server = _get_emby_server(server_id, require_enabled=False)
    payload = data.model_dump(exclude_unset=True)
    if "name" in payload and payload["name"] is not None:
        server["name"] = payload["name"].strip() or server["name"]
    if "url" in payload and payload["url"] is not None:
        server["url"] = payload["url"].rstrip("/")
    if payload.get("api_key"):
        server["api_key"] = payload["api_key"]
    if "user_id" in payload and payload["user_id"] is not None:
        server["user_id"] = payload["user_id"]
    if "enabled" in payload and payload["enabled"] is not None:
        server["enabled"] = bool(payload["enabled"])
    if "tvbox_enabled" in payload and payload["tvbox_enabled"] is not None:
        server["tvbox_enabled"] = bool(payload["tvbox_enabled"])
    if "allowed_library_ids" in payload and payload["allowed_library_ids"] is not None:
        server["allowed_library_ids"] = [str(x) for x in payload["allowed_library_ids"] if str(x).strip()]
    if "hidden_library_ids" in payload and payload["hidden_library_ids"] is not None:
        server["hidden_library_ids"] = [str(x) for x in payload["hidden_library_ids"] if str(x).strip()]
    if "password" in payload and payload["password"] is not None:
        pw = (payload["password"] or "").strip()
        server["password_hash"] = hashlib.sha256(pw.encode()).hexdigest() if pw else ""
    save_data()
    return _emby_server_admin_summary(server)


@app.delete("/api/emby/servers/{server_id}")
async def delete_emby_server(server_id: str):
    global emby_servers
    before = len(emby_servers)
    emby_servers = [s for s in emby_servers if s.get("id") != server_id]
    if len(emby_servers) == before:
        raise HTTPException(404, "Emby 源不存在")
    save_data()
    return {"ok": True}


@app.post("/api/emby/auth")
async def emby_auth(data: dict):
    server = _get_emby_server(data.get("server_id"), require_enabled=True)
    pw_hash = server.get("password_hash", "")
    if not pw_hash:
        return {"ok": True}
    if hashlib.sha256(data.get("password", "").encode()).hexdigest() != pw_hash:
        raise HTTPException(401, "密码错误")
    return {"ok": True}


@app.get("/api/emby/config")
async def get_emby_config():
    server = _get_emby_server(None, require_enabled=False)
    return _emby_server_admin_summary(server)


@app.put("/api/emby/config")
async def set_emby_config(data: dict):
    if emby_servers:
        server = _get_emby_server(None, require_enabled=False)
        payload = EmbyServerIn(**data)
        return await update_emby_server(server["id"], payload)
    payload = EmbyServerIn(**data)
    if not payload.id:
        payload.id = "default"
    if not payload.name:
        payload.name = "默认 Emby"
    return await create_emby_server(payload)


@app.post("/api/emby/test")
async def test_emby(server_id: str = ""):
    server = _get_emby_server(server_id or None, require_enabled=False)
    url = server.get("url", "")
    api_key = server.get("api_key", "")
    if not url or not api_key:
        return {"ok": False, "error": "请先填写服务器地址和 API Key"}
    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            r = await client.get(f"{url}/System/Info/Public", headers={"X-Emby-Token": api_key}, timeout=8)
            r.raise_for_status()
            info = r.json()
            return {"ok": True, "msg": f"连接成功：{info.get('ServerName', 'Emby')} v{info.get('Version', '?')}"}
    except httpx.TimeoutException:
        return {"ok": False, "error": "连接超时（8s）"}
    except Exception as e:
        return {"ok": False, "error": str(e)}


@app.get("/api/emby/users")
async def emby_users(server_id: str = ""):
    server = _get_emby_server(server_id or None, require_enabled=False)
    url = server.get("url", "")
    api_key = server.get("api_key", "")
    if not url or not api_key:
        raise HTTPException(400, "Emby 未配置")
    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            r = await client.get(f"{url}/Users", headers={"X-Emby-Token": api_key}, timeout=10)
            r.raise_for_status()
            return [{"id": u["Id"], "name": u["Name"]} for u in r.json()]
    except Exception as e:
        raise HTTPException(502, str(e))


@app.get("/api/emby/libraries")
async def emby_libraries(server_id: str = ""):
    server = _get_emby_server(server_id or None)
    try:
        libs = await _fetch_emby_views(server)
        return [{"id": i["Id"], "name": (i.get("Name") or i["Id"])} for i in libs]
    except Exception as e:
        raise HTTPException(502, str(e))


def _map_emby_items(raw_items: list, server_id: str) -> list:
    items = []
    for i in raw_items:
        if i.get("Type") not in {"Movie", "Series"}:
            continue
        has_img = bool((i.get("ImageTags") or {}).get("Primary"))
        thumb = f"/api/emby/{server_id}/image/{i['Id']}?w=200" if has_img else ""
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
async def emby_items(parent_id: str = "", pg: int = 1, limit: int = 40, server_id: str = ""):
    server = _get_emby_server(server_id or None)
    url = server.get("url", "")
    api_key = server.get("api_key", "")
    user_id = server.get("user_id", "")
    if not url or not api_key or not user_id:
        raise HTTPException(400, "Emby 未完整配置")
    allowed_ids = {i["Id"] for i in (await _fetch_emby_views(server))}
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
                if parent_id not in allowed_ids:
                    raise HTTPException(403, "该媒体库未授权")
                params["ParentId"] = parent_id
                r = await client.get(f"{url}/Users/{user_id}/Items", headers={"X-Emby-Token": api_key}, params=params, timeout=12)
                r.raise_for_status()
                data = r.json()
                items = _map_emby_items(data.get("Items", []), server["id"])
                return {"items": items, "total": data.get("TotalRecordCount", len(items)), "page": pg}
            collected = []
            for lib_id in allowed_ids:
                lib_params = dict(params)
                lib_params["ParentId"] = lib_id
                try:
                    lr = await client.get(f"{url}/Users/{user_id}/Items", headers={"X-Emby-Token": api_key}, params=lib_params, timeout=12)
                    lr.raise_for_status()
                    collected.extend(lr.json().get("Items", []))
                except Exception:
                    continue
            items = _map_emby_items(collected, server["id"])
            seen = set(); dedup = []
            for item in items:
                if item["id"] in seen:
                    continue
                seen.add(item["id"])
                dedup.append(item)
            dedup.sort(key=lambda x: ((x.get("name") or "").lower(), x.get("id") or ""))
            total = len(dedup)
            start = max(0, (pg - 1) * limit)
            return {"items": dedup[start:start+limit], "total": total, "page": pg}
    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(502, str(e))


@app.get("/api/emby/detail/{item_id}")
async def emby_detail(item_id: str, server_id: str = ""):
    server = _get_emby_server(server_id or None)
    url = server.get("url", "")
    api_key = server.get("api_key", "")
    user_id = server.get("user_id", "")
    if not url or not api_key or not user_id:
        raise HTTPException(400, "Emby 未完整配置")
    try:
        async with _make_http_client() as client:
            r = await client.get(f"{url}/Users/{user_id}/Items/{item_id}", headers={"X-Emby-Token": api_key}, params={"Fields": "Overview,ProductionYear,OfficialRating,People,Genres,Studios,Taglines,CommunityRating,PremiereDate,RunTimeTicks"}, timeout=12)
            r.raise_for_status()
            item = r.json()
            people = item.get("People") or []
            people_out = [{"id": p.get("Id", ""), "name": p.get("Name", ""), "type": p.get("Type", ""), "role": p.get("Role", "")} for p in people if p.get("Name")]
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
async def emby_person_items(person_id: str, pg: int = 1, limit: int = 40, server_id: str = ""):
    server = _get_emby_server(server_id or None)
    url = server.get("url", "")
    api_key = server.get("api_key", "")
    user_id = server.get("user_id", "")
    if not url or not api_key or not user_id:
        raise HTTPException(400, "Emby 未完整配置")
    try:
        async with _make_http_client() as client:
            params = {"PersonIds": person_id, "IncludeItemTypes": "Movie,Series", "Recursive": "true", "Fields": "PrimaryImageAspectRatio,Overview,ProductionYear,OfficialRating,People,Genres", "StartIndex": (pg - 1) * limit, "Limit": limit, "SortBy": "SortName", "SortOrder": "Ascending"}
            r = await client.get(f"{url}/Users/{user_id}/Items", headers={"X-Emby-Token": api_key}, params=params, timeout=12)
            r.raise_for_status()
            data = r.json()
            items = _map_emby_items(data.get("Items", []), server["id"])
            return {"items": items, "total": data.get("TotalRecordCount", len(items)), "page": pg}
    except Exception as e:
        raise HTTPException(502, str(e))


@app.get("/api/emby/episodes/{series_id}")
async def emby_episodes(series_id: str, server_id: str = ""):
    server = _get_emby_server(server_id or None)
    url = server.get("url", "")
    api_key = server.get("api_key", "")
    user_id = server.get("user_id", "")
    if not url or not api_key:
        raise HTTPException(400, "Emby 未配置")
    try:
        async with httpx.AsyncClient(follow_redirects=True) as client:
            r = await client.get(f"{url}/Shows/{series_id}/Episodes", headers={"X-Emby-Token": api_key}, params={"UserId": user_id, "Fields": "Overview", "Limit": 500}, timeout=10)
            r.raise_for_status()
            return [{"id": i["Id"], "name": i.get("Name", ""), "season": i.get("ParentIndexNumber", 1), "episode": i.get("IndexNumber", 0)} for i in r.json().get("Items", [])]
    except Exception as e:
        raise HTTPException(502, str(e))


@app.get("/api/emby/{server_id}/image/{item_id}")
async def emby_image(server_id: str, item_id: str, w: int = 200):
    server = _get_emby_server(server_id)
    url = server.get("url", "")
    api_key = server.get("api_key", "")
    if not url or not api_key:
        raise HTTPException(400, "Emby 未配置")
    width = max(120, min(int(w or 200), 600))
    cache_file = EMBY_IMAGE_CACHE_DIR / f"{server_id}_{item_id}_{width}.img"
    if cache_file.exists():
        media_type = "image/jpeg"
        if cache_file.suffix.lower() == '.png':
            media_type = 'image/png'
        return FileResponse(cache_file, media_type=media_type, headers={"Cache-Control": "public, max-age=86400"})
    image_url = f"{url}/Items/{item_id}/Images/Primary?api_key={api_key}&maxWidth={width}&quality=82"
    try:
        async with _make_http_client() as client:
            r = await client.get(image_url, timeout=10)
            r.raise_for_status()
            content_type = r.headers.get("content-type", "image/jpeg").split(';')[0].strip()
            ext = '.png' if 'png' in content_type else '.jpg'
            target = EMBY_IMAGE_CACHE_DIR / f"{server_id}_{item_id}_{width}{ext}"
            target.write_bytes(r.content)
            if target != cache_file and cache_file.exists():
                cache_file.unlink(missing_ok=True)
            return Response(content=r.content, media_type=content_type, headers={"Cache-Control": "public, max-age=86400"})
    except Exception:
        raise HTTPException(404, "图片不可用")


async def _resolve_emby_source_url(server: dict, item_id: str) -> Optional[str]:
    """有些 Emby 库条目本身就是指向外部地址的 strm（比如挂 AList 网盘直链），
    这种情况下 Emby 的媒体源 Path 字段就是那个 http(s) 地址。能拿到就直接把这个地址
    交给播放器，绕开 Emby 自己代理数据这一层；拿不到/不是 URL 就返回 None，调用方
    照原来的方式走 Emby 的 /stream 接口代理。"""
    url = server.get("url", "")
    api_key = server.get("api_key", "")
    user_id = server.get("user_id", "")
    if not (url and api_key and user_id):
        return None
    try:
        async with _make_http_client() as client:
            r = await client.get(
                f"{url}/Users/{user_id}/Items/{item_id}",
                headers={"X-Emby-Token": api_key},
                params={"Fields": "Path,MediaSources"},
                timeout=10,
            )
            r.raise_for_status()
            data = r.json()
    except Exception:
        return None
    path = data.get("Path") or ""
    if not path:
        for ms in data.get("MediaSources") or []:
            if ms.get("Path"):
                path = ms["Path"]
                break
    return path if path.startswith("http://") or path.startswith("https://") else None


@app.api_route("/api/emby/{server_id}/stream/{item_id}", methods=["GET", "HEAD"])
async def emby_stream(server_id: str, item_id: str, request: Request):
    # URL 带一个假的 .mp4 后缀是给部分靠文件名后缀嗅探格式的 TVBox 播放器识别用的，这里去掉
    item_id = item_id.rsplit(".", 1)[0] if "." in item_id else item_id
    server = _get_emby_server(server_id)
    url = server.get("url", "")
    api_key = server.get("api_key", "")
    if not url or not api_key:
        raise HTTPException(400, "Emby 未配置")

    source_url = await _resolve_emby_source_url(server, item_id)
    if source_url:
        return RedirectResponse(source_url, status_code=302)

    upstream_url = f"{url}/Videos/{item_id}/stream?api_key={api_key}&static=true"
    headers = {}
    range_header = request.headers.get("range") or request.headers.get("Range")
    if range_header:
        headers["Range"] = range_header
    # 万一 Emby 自己对 /stream 请求做 302 跳转（不是靠上面的 Path 字段而是靠自己代理层跳转），
    # 也不要在服务端跟着跳转把视频吃下来再转发，直接把跳转地址回给播放器，
    # 让它自己去连真实地址，视频数据就不会经过这台服务器。
    # client.stream() 只能作 async with 用，不能 await；这里要把响应体流跨出本函数交给
    # StreamingResponse 消费，所以改用 send(..., stream=True)，client 的关闭挪到用完之后
    client = _make_http_client(follow_redirects=False)
    try:
        req = client.build_request(request.method, upstream_url, headers=headers, timeout=None)
        upstream = await client.send(req, stream=True)
    except Exception as e:
        await client.aclose()
        raise HTTPException(502, f"Emby 播放代理失败: {e}")

    if upstream.is_redirect and "location" in upstream.headers:
        location = upstream.headers["location"]
        await upstream.aclose()
        await client.aclose()
        return RedirectResponse(location, status_code=302)

    passthrough_headers = {k: upstream.headers[k] for k in ["content-type", "content-length", "accept-ranges", "content-range", "cache-control", "etag", "last-modified", "content-disposition"] if k in upstream.headers}

    if request.method == "HEAD":
        status_code = upstream.status_code
        await upstream.aclose()
        await client.aclose()
        return Response(status_code=status_code, headers=passthrough_headers)

    async def body_iter():
        try:
            async for chunk in upstream.aiter_bytes():
                yield chunk
        finally:
            await upstream.aclose()
            await client.aclose()
    return StreamingResponse(body_iter(), status_code=upstream.status_code, headers=passthrough_headers, media_type=upstream.headers.get("content-type", "video/mp4"))


@app.get("/api/emby/{server_id}/direct/{item_id}")
async def emby_direct(server_id: str, item_id: str):
    server = _get_emby_server(server_id)
    url = server.get("url", "")
    api_key = server.get("api_key", "")
    if not url or not api_key:
        raise HTTPException(400, "Emby 未配置")
    return {"url": f"{url}/Videos/{item_id}/stream?api_key={api_key}&static=true"}

