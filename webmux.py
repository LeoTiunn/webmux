#!/usr/bin/env python3
"""webmux — Browser-based tmux terminal client.
Full xterm.js terminal emulator connected to tmux sessions via WebSocket."""

__version__ = "1.19.3"

import asyncio
import fcntl
import json
import os
import pty
import re
import secrets
import signal
import ssl
import struct
import subprocess
import sys
import termios
from pathlib import Path
from typing import Optional, List, Dict, Tuple
from urllib.parse import unquote

from aiohttp import web

CONFIG_DIR = Path.home() / ".config" / "webmux"


def _read_config():
    env_file = CONFIG_DIR / "env"
    cfg = {}
    if env_file.exists():
        for line in env_file.read_text().splitlines():
            if "=" in line and not line.startswith("#"):
                k, v = line.split("=", 1)
                cfg[k.strip()] = v.strip()
    return cfg


for _k, _v in _read_config().items():
    os.environ.setdefault(_k, _v)

PORT = int(os.environ.get("WEBMUX_PORT", 3033))
REMOTE = os.environ.get("WEBMUX_REMOTE", "").lower() in ("1", "true", "yes")
REMOTE_PORT = int(os.environ.get("WEBMUX_REMOTE_PORT", 3034))
HOST = "127.0.0.1"
DEV_ROOT = Path(os.environ.get("WEBMUX_DEV_ROOT", str(Path.home() / "Developer")))
CLAUDE_DIR = Path.home() / ".claude" / "projects"
AUTH_USER = os.environ.get("WEBMUX_USER", "admin") if REMOTE else ""
AUTH_PASS = os.environ.get("WEBMUX_PASS", "") or (secrets.token_urlsafe(16) if REMOTE else "")
CLAUDE_CMD = os.environ.get("WEBMUX_CMD", "claude --continue --dangerously-skip-permissions || claude --resume --dangerously-skip-permissions || claude --dangerously-skip-permissions")


def _get_ssl_context():
    if not REMOTE:
        return None
    cert = Path(os.environ.get("WEBMUX_CERT", str(CONFIG_DIR / "cert.pem")))
    key = Path(os.environ.get("WEBMUX_KEY", str(CONFIG_DIR / "key.pem")))
    if not cert.exists() or not key.exists():
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        cert = CONFIG_DIR / "cert.pem"
        key = CONFIG_DIR / "key.pem"
        try:
            subprocess.run([
                "openssl", "req", "-x509", "-newkey", "rsa:2048",
                "-keyout", str(key), "-out", str(cert),
                "-days", "3650", "-nodes",
                "-subj", "/CN=webmux",
            ], capture_output=True, check=True)
        except (subprocess.CalledProcessError, FileNotFoundError):
            print("Warning: openssl not found, remote HTTPS unavailable")
            return None
    ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    ctx.load_cert_chain(str(cert), str(key))
    return ctx


_branch_cache = {}  # type: Dict[str, Tuple[float, str]]  # cwd -> (expires_at, branch)
_BRANCH_TTL = 30.0  # branches change rarely; cache to keep the session poll cheap


def git_branch_for(cwd, now):
    # type: (str, float) -> str
    """Current git branch for a directory, cached for _BRANCH_TTL seconds so a
    fast session-list poll doesn't shell out to git on every tick."""
    hit = _branch_cache.get(cwd)
    if hit and hit[0] > now:
        return hit[1]
    branch = ""
    try:
        out = subprocess.check_output(
            ["git", "-C", cwd, "rev-parse", "--abbrev-ref", "HEAD"],
            text=True, stderr=subprocess.DEVNULL, timeout=2
        ).strip()
        branch = "" if out == "HEAD" else out  # detached HEAD → no name
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        pass
    _branch_cache[cwd] = (now + _BRANCH_TTL, branch)
    return branch


# Cache: claude pid -> conversation id. A claude process holds the same
# transcript for its whole life, so this only needs resolving once per pid.
# Pruned each sweep to the set of still-living pids.
_CONV_ID_CACHE = {}  # type: Dict[int, Optional[str]]
_UUID_RE = re.compile(r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})")


def _snapshot_processes():
    # type: () -> Dict[int, tuple]
    """One `ps` call → {pid: (ppid, command)} for the whole process table.

    Doing this once per sweep (instead of pgrep+ps per pane) is the difference
    between a ~20s sweep and a sub-second one on a machine with dozens of panes.
    """
    procs = {}
    try:
        out = subprocess.check_output(["ps", "-axo", "pid=,ppid=,command="],
                                      text=True, stderr=subprocess.DEVNULL, timeout=5)
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        return procs
    for line in out.splitlines():
        parts = line.strip().split(None, 2)
        if len(parts) < 2:
            continue
        try:
            pid, ppid = int(parts[0]), int(parts[1])
        except ValueError:
            continue
        procs[pid] = (ppid, parts[2] if len(parts) > 2 else "")
    return procs


def _looks_like_claude(cmd):
    # type: (str) -> bool
    """Heuristic to decide if a pane child is a claude process worth probing.
    Claude renames itself to a bare version string like "2.1.204", runs via
    node, or still shows "claude"/--resume/--continue on the cmdline. A plain
    login shell (zsh/bash/-zsh) is NOT — skip it so we never lsof shells."""
    c = cmd.strip()
    if not c:
        return False
    if "claude" in c or "--resume" in c or "--continue" in c or "node" in c:
        return True
    # bare version string (e.g. "2.1.204") — Claude's renamed process
    if re.match(r"^\d+\.\d+", c):
        return True
    return False


def _resolve_conv_id(cpid, cmd):
    # type: (int, str) -> Optional[str]
    """The REAL conversation id a claude pid is on. Ground truth, every launch
    style (`--resume`, `--continue`, fresh `-n`): Claude writes its live
    conversation to ~/.claude/sessions/<pid>.json as `sessionId`, keyed by pid.
    That's authoritative and covers the cases a `--resume <id>` cmdline scan
    misses (continue/fresh). Falls back to the cmdline id if the file's absent.
    Cached by pid (a claude process keeps the same conversation for its life)."""
    if cpid in _CONV_ID_CACHE:
        return _CONV_ID_CACHE[cpid]
    conv = None
    # Primary: Claude's own per-pid session record.
    try:
        rec = json.loads((SESSIONS_DIR / (str(cpid) + ".json")).read_text())
        sid = rec.get("sessionId")
        if sid and _UUID_RE.fullmatch(sid):
            conv = sid
    except (OSError, ValueError):
        pass
    # Fallback: `--resume <id>` on the command line.
    if not conv:
        m = re.search(r"--resume\s+(" + _UUID_RE.pattern + r")", cmd)
        if m:
            conv = m.group(1)
    _CONV_ID_CACHE[cpid] = conv
    return conv


def _claude_descendant(pane_pid, procs):
    # type: (Optional[int], Dict[int, tuple]) -> Optional[tuple]
    """Find the claude (pid, cmd) under a pane, walking the process tree — the
    claude process is often a GRANDCHILD (pane shell → login shell → claude),
    not a direct child. Returns (pid, cmd) or None."""
    if not pane_pid or not procs:
        return None
    # children index
    kids = {}  # type: Dict[int, list]
    for pid, (ppid, cmd) in procs.items():
        kids.setdefault(ppid, []).append((pid, cmd))
    stack = list(kids.get(pane_pid, []))
    seen = set()
    while stack:
        pid, cmd = stack.pop()
        if pid in seen:
            continue
        seen.add(pid)
        if _looks_like_claude(cmd):
            return (pid, cmd)
        stack.extend(kids.get(pid, []))
    return None


def _conv_id_for_pane(pane_pid, procs):
    # type: (Optional[int], Dict[int, tuple]) -> Optional[str]
    """Ground-truth conversation id for a pane, using a pre-built process map."""
    hit = _claude_descendant(pane_pid, procs)
    if not hit:
        return None
    return _resolve_conv_id(hit[0], hit[1])


def get_tmux_sessions():
    # type: () -> List[Dict]
    import time as _time
    now = _time.monotonic()
    procs = _snapshot_processes()  # one ps call for the whole sweep
    rows = []
    try:
        out = subprocess.check_output(
            ["tmux", "list-sessions", "-F",
             "#{session_name}:#{pane_current_path}:#{pane_current_command}:#{session_activity}:#{pane_pid}"],
            text=True, stderr=subprocess.DEVNULL, timeout=5
        )
        for line in out.strip().splitlines():
            parts = line.split(":", 4)
            if len(parts) < 2:
                continue
            rows.append((parts[0], parts[1],
                         parts[2] if len(parts) > 2 else "",
                         int(parts[3]) if len(parts) > 3 and parts[3].isdigit() else 0,
                         int(parts[4]) if len(parts) > 4 and parts[4].isdigit() else None))
    except (subprocess.CalledProcessError, FileNotFoundError, subprocess.TimeoutExpired):
        pass
    # Claude's own per-session busy/idle flag (from ~/.claude/sessions/<pid>.json).
    # Index it by the Claude session NAME so we can attach it to each tmux row —
    # this is the authoritative "agent still working vs finished" signal, far
    # better than tmux activity (which ticks the whole time output streams).
    claude_busy = {}  # name -> True if busy
    for m in session_meta().values():
        nm = m.get("name")
        if nm:
            claude_busy[nm] = (m.get("status") == "busy")
    # Resolve each pane's claude descendant + its conversation id. conv id comes
    # straight from the process command line now (no lsof), so this is cheap.
    sessions = []
    for (name, cwd, cmd, activity, pane_pid) in rows:
        hit = _claude_descendant(pane_pid, procs)
        conv_id = _resolve_conv_id(hit[0], hit[1]) if hit else None
        status = "active" if (hit or "claude" in cmd.lower() or "node" in cmd.lower()) else "idle"
        sessions.append({"name": name, "cwd": cwd, "status": status, "command": cmd,
                         "activity": activity, "branch": git_branch_for(cwd, now),
                         "conv_id": conv_id, "busy": claude_busy.get(name, False)})
    # Prune conv-id cache entries whose pid is gone (using the same snapshot).
    for dead in [p for p in _CONV_ID_CACHE if p not in procs]:
        del _CONV_ID_CACHE[dead]
    return sessions


def list_projects():
    # type: () -> List[Dict]
    projects = []
    if not DEV_ROOT.is_dir():
        return projects
    for org in sorted(DEV_ROOT.iterdir()):
        if not org.is_dir() or org.name.startswith("."):
            continue
        for proj in sorted(org.iterdir()):
            if not proj.is_dir() or proj.name.startswith("."):
                continue
            projects.append({"org": org.name, "name": proj.name, "path": str(proj)})
    return projects


def clean_name(raw, fallback="session"):
    # type: (str, str) -> str
    """Sanitize a user/derived name into a safe tmux session name.

    tmux treats '.' as a target separator (session.window.pane), so a name with
    a dot makes send-keys / kill / rename fail — the session becomes an unusable
    ghost. Also collapse whitespace and any other unsafe char to '-'. Result
    contains only [A-Za-z0-9_-]. Used for BOTH the tmux name and the `claude -n`
    name so the two stay consistent.
    """
    s = re.sub(r"[^A-Za-z0-9_-]+", "-", (raw or "").strip())
    s = re.sub(r"-+", "-", s).strip("-_")
    return s or fallback


def _valid_conv_id(cid):
    # type: (Optional[str]) -> bool
    return bool(cid) and bool(_UUID_RE.fullmatch(cid or ""))


def create_session(name, directory, resume_id=None, fresh=False, claude_name=None, branch=None):
    # type: (str, str, Optional[str], bool, Optional[str], Optional[str]) -> Dict
    # Harden the tmux session name: a stray '.' (e.g. "v1.2" or a "next.js" dir)
    # would make every later `tmux -t <name>` command fail. Never trust the caller.
    name = clean_name(name)
    try:
        # Optional: switch to (or create) a git branch in the directory BEFORE
        # launching claude. NOTE: this directory is shared by all sessions of the
        # project, so a checkout moves them all — the frontend warns first.
        if branch:
            b = branch.strip()
            if b:
                # Try to checkout an existing branch; if it doesn't exist, create it.
                co = subprocess.run(["git", "-C", directory, "checkout", b],
                                    capture_output=True, text=True, timeout=10)
                if co.returncode != 0:
                    cr = subprocess.run(["git", "-C", directory, "checkout", "-b", b],
                                        capture_output=True, text=True, timeout=10)
                    if cr.returncode != 0:
                        return {"ok": False,
                                "error": "git checkout failed: " + (cr.stderr or co.stderr or "unknown").strip()[:200]}
        # resume_id is interpolated into a shell command sent via send-keys, so
        # it MUST be a real conversation UUID — never trust arbitrary input.
        if resume_id and not _valid_conv_id(resume_id):
            return {"ok": False, "error": "invalid resume_id"}
        subprocess.run(["tmux", "new-session", "-d", "-s", name, "-c", directory],
                       check=True, capture_output=True, text=True, timeout=5)
        if resume_id:
            cmd = "claude --resume " + resume_id + " --dangerously-skip-permissions"
        elif fresh:
            # A genuinely NEW session — never --continue. Optionally name it via
            # `claude -n <name>` so the name shows in Claude's own /resume picker.
            # Use the same sanitizer as the tmux name so the two stay consistent
            # (spaces → '-', not silently deleted).
            cmd = "claude --dangerously-skip-permissions"
            if claude_name:
                safe = clean_name(claude_name, fallback="")
                if safe:
                    cmd = "claude -n " + safe + " --dangerously-skip-permissions"
        else:
            cmd = CLAUDE_CMD
        if cmd:
            subprocess.run(["tmux", "send-keys", "-t", name, cmd, "Enter"],
                           check=True, capture_output=True, text=True, timeout=5)
        return {"ok": True, "name": name}
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        return {"ok": False, "error": str(e)}


def send_to_session(session_name, message):
    # type: (str, str) -> Dict
    try:
        # Exit copy-mode first — otherwise send-keys chars are consumed as
        # copy-mode commands and wedge the pane (ignored if not in copy-mode).
        subprocess.run(["tmux", "send-keys", "-t", session_name, "-X", "cancel"],
                       capture_output=True, text=True, timeout=5)
        subprocess.run(["tmux", "send-keys", "-l", "-t", session_name, message],
                       check=True, capture_output=True, text=True, timeout=5)
        subprocess.run(["tmux", "send-keys", "-t", session_name, "Enter"],
                       check=True, capture_output=True, text=True, timeout=5)
        return {"ok": True}
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        return {"ok": False, "error": str(e)}


async def restart_all_sessions():
    # type: () -> Dict
    sessions = get_tmux_sessions()
    restarted = []
    for s in sessions:
        name = s["name"]
        try:
            subprocess.run(["tmux", "send-keys", "-t", name, "/exit", "Enter"],
                           check=True, capture_output=True, text=True)
            restarted.append(name)
        except subprocess.CalledProcessError:
            pass
    if CLAUDE_CMD:
        await asyncio.sleep(3)
        for name in restarted:
            try:
                subprocess.run(["tmux", "send-keys", "-t", name, CLAUDE_CMD, "Enter"],
                               check=True, capture_output=True, text=True)
            except subprocess.CalledProcessError:
                pass
    return {"ok": True, "restarted": restarted}


def kill_session(name):
    # type: (str) -> Dict
    try:
        subprocess.run(["tmux", "kill-session", "-t", name],
                       check=True, capture_output=True, text=True, timeout=5)
        return {"ok": True}
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        return {"ok": False, "error": str(e)}


def rename_session(old, new):
    # type: (str, str) -> Dict
    try:
        subprocess.run(["tmux", "rename-session", "-t", old, new],
                       check=True, capture_output=True, text=True, timeout=5)
        return {"ok": True, "name": new}
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        return {"ok": False, "error": str(e)}


def rename_claude_session(tmux_name, new_name):
    # type: (str, str) -> Dict
    """Rename a live session, keeping the tmux name and the Claude conversation
    name IN SYNC (1:1). Two steps:
      1. `/rename <name>` — Claude's official rename (updates
         ~/.claude/sessions/<pid>.json + the /resume picker).
      2. `tmux rename-session` — rename the tmux session to the SAME (sanitized)
         name so the tmux handle matches the conversation name. This does not
         detach/reattach; the live connection stays up.
    Returns the new tmux name so the client can re-point its socket to it."""
    safe = clean_name(new_name)
    try:
        subprocess.run(["tmux", "send-keys", "-t", tmux_name, "-X", "cancel"],
                       capture_output=True, text=True, timeout=5)
        # Type the slash command literally, then submit with Enter.
        subprocess.run(["tmux", "send-keys", "-l", "-t", tmux_name, "/rename " + new_name],
                       check=True, capture_output=True, text=True, timeout=5)
        subprocess.run(["tmux", "send-keys", "-t", tmux_name, "Enter"],
                       check=True, capture_output=True, text=True, timeout=5)
        # Keep the tmux session name in sync (rename-session doesn't drop the
        # attach). If the target name is taken, tmux errors — ignore and keep the
        # old tmux name rather than fail the whole rename.
        new_tmux = tmux_name
        if safe and safe != tmux_name:
            r = subprocess.run(["tmux", "rename-session", "-t", tmux_name, safe],
                               capture_output=True, text=True, timeout=5)
            if r.returncode == 0:
                new_tmux = safe
        return {"ok": True, "name": new_name, "tmux": new_tmux}
    except (subprocess.CalledProcessError, subprocess.TimeoutExpired) as e:
        return {"ok": False, "error": str(e)}


def find_project_dir(cwd):
    # type: (str) -> Optional[Path]
    key = cwd.replace("/", "-").replace(".", "-")
    p = CLAUDE_DIR / key
    return p if p.is_dir() else None


def find_latest_conversation(project_dir):
    # type: (Path) -> Optional[Path]
    jsonl_files = sorted(project_dir.glob("*.jsonl"), key=os.path.getmtime, reverse=True)
    return jsonl_files[0] if jsonl_files else None


def parse_conversation(conv_file):
    # type: (Path) -> List[Dict]
    messages = []
    for line in conv_file.read_text().splitlines():
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        msg_type = obj.get("type")
        if msg_type == "user":
            content = obj.get("message", {}).get("content", "")
            if isinstance(content, str) and content.strip():
                messages.append({"role": "user", "text": content})
            elif isinstance(content, list):
                texts = [c.get("text", "") for c in content if c.get("type") == "text" and c.get("text", "").strip()]
                if texts:
                    messages.append({"role": "user", "text": "\n".join(texts)})
        elif msg_type == "assistant":
            parts = obj.get("message", {}).get("content", [])
            texts = [p.get("text", "") for p in parts if p.get("type") == "text" and p.get("text", "").strip()]
            tools = [p.get("name", "") for p in parts if p.get("type") == "tool_use"]
            if texts or tools:
                messages.append({"role": "assistant", "text": "\n\n".join(texts), "tools": tools})
    return messages


# --- WebSocket Terminal Handler ---


async def terminal_ws(request):
    ws = web.WebSocketResponse()
    await ws.prepare(request)

    if not check_auth(request):
        await ws.close(code=4001, message=b"unauthorized")
        return ws

    session_name = request.query.get("session", "")
    if not session_name:
        await ws.close(code=4000, message=b"session required")
        return ws


    # Size the PTY BEFORE forking tmux attach, using the dimensions the client
    # passes in the connect URL. This avoids the race where tmux attaches at the
    # default 80x24 and only gets resized by a later message — the cause of the
    # finicky "sometimes blank/wrong size until I poke it" attaches.
    try:
        init_cols = max(2, min(500, int(request.query.get("cols", "80"))))
        init_rows = max(1, min(300, int(request.query.get("rows", "24"))))
    except (ValueError, TypeError):
        init_cols, init_rows = 80, 24

    master_fd, slave_fd = pty.openpty()
    # Set winsize on the slave so the child inherits the correct size at exec.
    winsize = struct.pack("HHHH", init_rows, init_cols, 0, 0)
    try:
        fcntl.ioctl(slave_fd, termios.TIOCSWINSZ, winsize)
    except OSError:
        pass
    pid = os.fork()
    if pid == 0:
        os.setsid()
        os.dup2(slave_fd, 0)
        os.dup2(slave_fd, 1)
        os.dup2(slave_fd, 2)
        os.close(master_fd)
        os.close(slave_fd)
        env = os.environ.copy()
        env["TERM"] = "xterm-256color"
        os.execvpe("tmux", ["tmux", "attach-session", "-t", session_name], env)
        os._exit(1)

    os.close(slave_fd)

    loop = asyncio.get_event_loop()
    queue = asyncio.Queue()  # type: asyncio.Queue

    def on_pty_read():
        try:
            data = os.read(master_fd, 65536)
            if data:
                queue.put_nowait(data)
            else:
                queue.put_nowait(None)
        except OSError:
            queue.put_nowait(None)

    loop.add_reader(master_fd, on_pty_read)

    async def send_to_ws():
        while True:
            data = await queue.get()
            if data is None:
                break
            try:
                await ws.send_bytes(data)
            except Exception:
                break

    sender = asyncio.create_task(send_to_ws())

    try:
        async for msg in ws:
            if msg.type == web.WSMsgType.BINARY:
                try:
                    os.write(master_fd, msg.data)
                except OSError:
                    break
            elif msg.type == web.WSMsgType.TEXT:
                try:
                    ctrl = json.loads(msg.data)
                    if ctrl.get("type") == "resize":
                        winsize = struct.pack("HHHH", ctrl["rows"], ctrl["cols"], 0, 0)
                        fcntl.ioctl(master_fd, termios.TIOCSWINSZ, winsize)
                        os.kill(pid, signal.SIGWINCH)
                except (json.JSONDecodeError, KeyError, OSError):
                    try:
                        os.write(master_fd, msg.data.encode())
                    except OSError:
                        break
            elif msg.type in (web.WSMsgType.ERROR, web.WSMsgType.CLOSE):
                break
    finally:
        loop.remove_reader(master_fd)
        sender.cancel()
        try:
            os.close(master_fd)
        except OSError:
            pass
        try:
            os.kill(pid, signal.SIGHUP)
            os.waitpid(pid, os.WNOHANG)
        except OSError:
            pass

    return ws


# --- HTTP Handlers ---

async def api_sessions(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    return web.json_response(get_tmux_sessions())

async def api_messages(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    session_name = request.query.get("session", "")
    msgs = []
    for s in get_tmux_sessions():
        if s["name"] == session_name:
            proj = find_project_dir(s["cwd"])
            if proj:
                conv = find_latest_conversation(proj)
                if conv:
                    msgs = parse_conversation(conv)
            break
    total = len(msgs)
    limit = int(request.query.get("limit", "500"))
    if limit > 0:
        msgs = msgs[-limit:]
    return web.json_response({"total": total, "messages": msgs})

async def api_projects(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    return web.json_response(list_projects())

SESSIONS_DIR = Path.home() / ".claude" / "sessions"


def session_meta():
    # type: () -> Dict[str, Dict]
    """Map sessionId -> {name, status, pid} from ~/.claude/sessions/<pid>.json.
    This is Claude Code's own session registry; `name` is what `/rename` sets and
    what `/resume` shows. Newest file wins if a sessionId appears twice."""
    meta = {}
    if not SESSIONS_DIR.is_dir():
        return meta
    files = sorted(SESSIONS_DIR.glob("*.json"), key=os.path.getmtime)
    for f in files:
        try:
            o = json.loads(f.read_text())
        except (OSError, json.JSONDecodeError):
            continue
        sid = o.get("sessionId")
        if not sid:
            continue
        meta[sid] = {
            "name": o.get("name") or "",
            "status": o.get("status") or "",
            "pid": o.get("pid"),
        }
    return meta


def list_conversations(cwd, limit=50):
    # type: (str, int) -> List[Dict]
    proj = find_project_dir(cwd)
    if not proj:
        return []
    meta = session_meta()
    files = sorted(proj.glob("*.jsonl"), key=os.path.getmtime, reverse=True)[:limit]
    out = []
    for f in files:
        sid = f.stem
        summary = ""
        branch = ""
        try:
            with open(f) as fh:
                for line in fh:
                    if not line.strip():
                        continue
                    try:
                        o = json.loads(line)
                    except json.JSONDecodeError:
                        continue
                    if not branch and o.get("gitBranch"):
                        branch = o.get("gitBranch")
                    if o.get("type") != "user":
                        continue
                    c = o.get("message", {}).get("content", "")
                    text = ""
                    if isinstance(c, str):
                        text = c.strip()
                    elif isinstance(c, list):
                        t = [p.get("text", "") for p in c
                             if p.get("type") == "text" and p.get("text", "").strip()]
                        if t:
                            text = t[0].strip()
                    if not text:
                        continue
                    # Skip command/tool/system noise + machine-fed transcript
                    # excerpts (these make many unrelated sessions look identical);
                    # keep scanning for a real human prompt.
                    low = text.lower()
                    if (text.startswith("<") or text.startswith("Caveat:")
                            or low.startswith("below is a claude code session")
                            or low.startswith("you are reading a claude code session")
                            or low.startswith("here is a claude code session")):
                        continue
                    if not summary:
                        summary = text
                    # Got both summary and branch — stop early.
                    if branch:
                        break
        except OSError:
            pass
        try:
            size = os.path.getsize(f)
        except OSError:
            size = 0
        m = meta.get(sid, {})
        out.append({
            "id": sid,
            "mtime": int(os.path.getmtime(f)),
            "summary": summary[:80],
            "branch": branch,
            "size": size,
            "claude_name": m.get("name", ""),   # Claude Code's own session name (/rename)
            "claude_status": m.get("status", ""),
        })
    return out

async def api_conversations(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    cwd = request.query.get("cwd", "")
    if not cwd:
        return web.json_response([])
    return web.json_response(list_conversations(cwd))


def delete_conversation(cwd, conv_id):
    # type: (str, str) -> Dict
    proj = find_project_dir(cwd)
    if not proj:
        return {"ok": False, "error": "project not found"}
    # Guard against path traversal — conv_id must be a bare jsonl stem.
    if "/" in conv_id or "\\" in conv_id or ".." in conv_id:
        return {"ok": False, "error": "invalid id"}
    f = proj / (conv_id + ".jsonl")
    try:
        f.resolve().relative_to(proj.resolve())
    except ValueError:
        return {"ok": False, "error": "path escapes project"}
    if not f.exists():
        return {"ok": False, "error": "not found"}
    try:
        f.unlink()
    except OSError as e:
        return {"ok": False, "error": str(e)}
    return {"ok": True}


async def api_delete_conversation(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    body = await request.json()
    cwd = body.get("cwd", "")
    conv_id = body.get("id", "")
    if not cwd or not conv_id:
        return web.json_response({"ok": False, "error": "cwd and id required"}, status=400)
    result = delete_conversation(cwd, conv_id)
    return web.json_response(result, status=200 if result.get("ok") else 400)

async def api_create_project(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    body = await request.json()
    raw = (body.get("name") or "").strip().strip("/")
    if not raw or ".." in raw:
        return web.json_response({"ok": False, "error": "invalid name"}, status=400)
    parts = [p for p in raw.split("/") if p]
    # "org/name" → DEV_ROOT/org/name ; bare "name" → DEV_ROOT/leo-chang/name
    if len(parts) == 1:
        parts = ["leo-chang", parts[0]]
    target = DEV_ROOT.joinpath(*parts)
    try:
        target.resolve().relative_to(DEV_ROOT.resolve())
    except ValueError:
        return web.json_response({"ok": False, "error": "path escapes Developer"}, status=400)
    try:
        target.mkdir(parents=True, exist_ok=True)
    except OSError as e:
        return web.json_response({"ok": False, "error": str(e)}, status=500)
    return web.json_response({"ok": True, "path": str(target), "session": parts[-1]})

async def api_create_session(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    body = await request.json()
    name = body.get("name", "")
    directory = body.get("directory", "")
    resume_id = body.get("resume_id") or None
    fresh = bool(body.get("fresh"))
    claude_name = body.get("claude_name") or None
    branch = body.get("branch") or None
    if not name or not directory:
        return web.json_response({"ok": False, "error": "name and directory required"}, status=400)
    # Duplicate-session guard. Opening a conversation that ALREADY has a live
    # session would spawn a SECOND tmux session on the same transcript — two
    # rows for one conversation (and two `claude` appending the same JSONL).
    # Figure out which conversation this request would land on and, if a live
    # session is already on it, just return that session for the client to
    # attach instead of creating a duplicate.
    #   - resume_id: the explicit target.
    #   - not fresh (default CLAUDE_CMD uses `claude --continue`): lands on the
    #     directory's most-recently-modified conversation.
    #   - fresh: a genuinely new conversation → never a duplicate, skip.
    target_conv = resume_id
    if not target_conv and not fresh:
        convs = list_conversations(directory, limit=1)  # newest first
        if convs:
            target_conv = convs[0]["id"]
    if target_conv:
        for s in get_tmux_sessions():
            if s.get("conv_id") == target_conv:
                return web.json_response({"ok": True, "name": s["name"], "existing": True})
    result = create_session(name, directory, resume_id=resume_id, fresh=fresh,
                            claude_name=claude_name, branch=branch)
    return web.json_response(result, status=200 if result.get("ok") else 500)

async def api_kill_session(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    name = unquote(request.match_info["name"])
    if not name:
        return web.json_response({"ok": False, "error": "name required"}, status=400)
    result = kill_session(name)
    return web.json_response(result, status=200 if result["ok"] else 500)

async def api_rename_session(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    data = await request.json()
    old = data.get("old", "")
    new = data.get("new", "")
    if not old or not new:
        return web.json_response({"ok": False, "error": "old and new required"}, status=400)
    result = rename_session(old, new)
    return web.json_response(result, status=200 if result["ok"] else 500)

async def api_rename_claude(request):
    # Rename the Claude Code session in a live tmux session via `/rename`.
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    data = await request.json()
    tmux_name = data.get("tmux", "")
    new_name = (data.get("name", "") or "").strip()
    if not tmux_name or not new_name:
        return web.json_response({"ok": False, "error": "tmux and name required"}, status=400)
    # Claude session names: no spaces in the resume key, keep it sane.
    new_name = new_name.replace("\n", " ").strip()
    result = await asyncio.get_event_loop().run_in_executor(
        None, rename_claude_session, tmux_name, new_name)
    return web.json_response(result, status=200 if result.get("ok") else 500)

async def api_restart_all(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    result = await restart_all_sessions()
    return web.json_response(result)

UPLOAD_DIR = Path.home() / "Downloads" / "webmux-uploads"

async def api_upload(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
    data = await request.post()
    session_name = data.get("session", "")
    saved_paths = []
    for key, val in data.items():
        if key == "files" and hasattr(val, "filename"):
            safe_name = Path(val.filename).name
            dest = UPLOAD_DIR / safe_name
            dest.write_bytes(val.file.read())
            saved_paths.append(str(dest))
    if not saved_paths:
        return web.json_response({"ok": False, "error": "no files uploaded"}, status=400)
    if session_name:
        paths_str = " ".join(saved_paths)
        # Run in executor so a wedged tmux send-keys can't block the event loop.
        loop = asyncio.get_event_loop()
        await loop.run_in_executor(None, send_to_session, session_name, paths_str)
    return web.json_response({"ok": True, "paths": saved_paths})


def check_auth(request):
    if not REMOTE:
        return True
    if request.remote in ("127.0.0.1", "::1"):
        return True
    return request.cookies.get("webmux_auth", "") == "1"

_login_failures = {}  # ip -> (count, last_failure_time)
_MAX_ATTEMPTS = 5
_LOCKOUT_SECS = 300

async def handle_login(request):
    if not REMOTE:
        return web.HTTPFound("/")
    ip = request.remote
    if request.method == "POST":
        import time as _time
        now = _time.time()
        fails = _login_failures.get(ip, (0, 0))
        if fails[0] >= _MAX_ATTEMPTS and (now - fails[1]) < _LOCKOUT_SECS:
            remaining = int(_LOCKOUT_SECS - (now - fails[1]))
            return web.Response(
                text=LOGIN_HTML.replace("<!--ERROR-->", f'<div class="login-error">Too many attempts. Try again in {remaining}s</div>'),
                content_type="text/html", status=429)
        data = await request.post()
        user = data.get("username", "")
        pwd = data.get("password", "")
        if user == AUTH_USER and pwd == AUTH_PASS:
            _login_failures.pop(ip, None)
            resp = web.HTTPFound("/")
            session_days = int(os.environ.get("WEBMUX_SESSION_DAYS", "7"))
            resp.set_cookie("webmux_auth", "1", httponly=True, samesite="Strict",
                            secure=True, max_age=session_days * 86400)
            return resp
        _login_failures[ip] = (fails[0] + 1, now)
        return web.Response(
            text=LOGIN_HTML.replace("<!--ERROR-->", '<div class="login-error">Invalid username or password</div>'),
            content_type="text/html")
    return web.Response(text=LOGIN_HTML, content_type="text/html")

async def api_browse(request):
    path = request.query.get("path", str(Path.home()))
    try:
        p = Path(path)
        dirs = sorted([d.name for d in p.iterdir() if d.is_dir() and not d.name.startswith(".")])
        return web.json_response({"path": str(p), "parent": str(p.parent), "dirs": dirs})
    except (OSError, PermissionError):
        return web.json_response({"path": path, "parent": str(Path(path).parent), "dirs": []})

GROUPS_FILE = CONFIG_DIR / "groups.json"

async def api_groups(request):
    if not check_auth(request):
        return web.json_response({"error": "unauthorized"}, status=401)
    if request.method == "POST":
        try:
            data = await request.json()
        except json.JSONDecodeError:
            return web.json_response({"ok": False, "error": "bad json"}, status=400)
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        GROUPS_FILE.write_text(json.dumps(data, indent=2))
        return web.json_response({"ok": True})
    # GET
    if GROUPS_FILE.exists():
        try:
            return web.json_response(json.loads(GROUPS_FILE.read_text()))
        except (json.JSONDecodeError, OSError):
            pass
    return web.json_response({"categories": [], "assign": {}})

async def handle_settings(request):
    if request.method == "POST":
        data = await request.post()
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        env_file = CONFIG_DIR / "env"
        lines = env_file.read_text().splitlines() if env_file.exists() else []
        for key in ("WEBMUX_PORT", "WEBMUX_DEV_ROOT", "WEBMUX_REMOTE", "WEBMUX_REMOTE_PORT",
                     "WEBMUX_USER", "WEBMUX_PASS", "WEBMUX_SESSION_DAYS", "WEBMUX_CMD"):
            val = data.get(key, "")
            val = val.strip() if hasattr(val, "strip") else ""
            if key == "WEBMUX_PASS" and not val:
                continue
            if key in ("WEBMUX_PORT", "WEBMUX_REMOTE_PORT", "WEBMUX_SESSION_DAYS"):
                try:
                    int(val or "0")
                except ValueError:
                    continue
            lines = [l for l in lines if not l.startswith(f"{key}=")]
            if val:
                lines.append(f"{key}={val}")
        for file_key, env_key in [("cert_file", "WEBMUX_CERT"), ("key_file", "WEBMUX_KEY")]:
            upload = data.get(file_key)
            if upload and hasattr(upload, "file") and upload.filename:
                dest = CONFIG_DIR / upload.filename
                dest.write_bytes(upload.file.read())
                lines = [l for l in lines if not l.startswith(f"{env_key}=")]
                lines.append(f"{env_key}={dest}")
        env_file.write_text("\n".join(lines) + "\n")
        import threading
        def _delayed_restart():
            import time; time.sleep(1)
            subprocess.Popen(["open", "/Applications/Webmux.app"])
            os._exit(0)
        threading.Thread(target=_delayed_restart, daemon=True).start()
        return web.Response(text=_settings_html('<div class="settings-msg">Saved. Restarting...</div>'),
                            content_type="text/html")
    return web.Response(text=_settings_html(), content_type="text/html")

async def serve_html(request):
    if not check_auth(request):
        return web.HTTPFound("/login")
    return web.Response(text=HTML, content_type="text/html")


# --- HTML/CSS/JS ---

def _settings_html(msg=""):
    cfg = _read_config()
    esc = lambda s: s.replace("&", "&amp;").replace("<", "&lt;").replace('"', "&quot;")
    c_remote = cfg.get("WEBMUX_REMOTE", "0").lower() in ("1", "true", "yes")
    toggle_cls = "settings-toggle on" if c_remote else "settings-toggle"
    toggle_val = "1" if c_remote else "0"
    c_port = cfg.get("WEBMUX_PORT", str(PORT))
    c_cmd = cfg.get("WEBMUX_CMD", CLAUDE_CMD)
    c_remote_port = cfg.get("WEBMUX_REMOTE_PORT", str(REMOTE_PORT))
    c_user = cfg.get("WEBMUX_USER", AUTH_USER or "admin")
    c_days = cfg.get("WEBMUX_SESSION_DAYS", "7")
    c_dev_root = cfg.get("WEBMUX_DEV_ROOT", str(DEV_ROOT))
    c_cert = cfg.get("WEBMUX_CERT", "")
    c_key = cfg.get("WEBMUX_KEY", "")
    return """<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>webmux — settings</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'><rect width='32' height='32' rx='7' fill='%%230f0f13'/><text x='16' y='22' text-anchor='middle' font-family='Helvetica,sans-serif' font-weight='700' font-size='18' fill='%%23e8a849'>W</text></svg>">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Outfit:wght@400;500;600&family=JetBrains+Mono:wght@400&display=swap" rel="stylesheet">
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body { font-family: 'Outfit', sans-serif; background: #09090b; color: #e4e4ed;
    min-height: 100vh; display: flex; align-items: center; justify-content: center;
    -webkit-font-smoothing: antialiased; }
  .settings-card { width: 480px; max-width: 95vw; background: #0f0f13;
    border: 1px solid #1e1e28; border-radius: 16px; padding: 32px; }
  .settings-header { display: flex; align-items: center; gap: 12px; margin-bottom: 24px; }
  .settings-header a { color: #686880; text-decoration: none; font-size: 20px; }
  .settings-header a:hover { color: #e8a849; }
  .settings-title { font-size: 18px; font-weight: 600; }
  .settings-section { font-size: 11px; text-transform: uppercase; letter-spacing: 0.08em;
    color: #686880; margin: 20px 0 8px; font-weight: 500; }
  .settings-row { display: flex; align-items: center; gap: 12px; margin-bottom: 12px; }
  .settings-label { width: 120px; font-size: 13px; color: #9999b0; flex-shrink: 0; }
  .settings-input { flex: 1; padding: 8px 12px; background: #14141a; border: 1px solid #1e1e28;
    border-radius: 8px; color: #e4e4ed; font-family: 'JetBrains Mono', monospace;
    font-size: 13px; outline: none; transition: border-color 0.15s; }
  .settings-input:focus { border-color: #e8a849; }
  .settings-input::placeholder { color: #55556a; }
  .settings-input.short { width: 80px; flex: none; }
  .settings-suffix { font-size: 13px; color: #686880; }
  .settings-toggle { width: 44px; height: 24px; border-radius: 12px;
    background: #2a2a38; border: none; cursor: pointer;
    position: relative; transition: background 0.2s; }
  .settings-toggle.on { background: #e8a849; }
  .settings-toggle::after { content: ''; position: absolute; top: 2px; left: 2px;
    width: 20px; height: 20px; border-radius: 50%%; background: #fff; transition: left 0.2s; }
  .settings-toggle.on::after { left: 22px; }
  .settings-btn { width: 100%%; padding: 12px; margin-top: 20px; background: #e8a849;
    color: #09090b; border: none; border-radius: 10px; font-family: 'Outfit', sans-serif;
    font-size: 14px; font-weight: 600; cursor: pointer; transition: opacity 0.15s; }
  .settings-btn:hover { opacity: 0.85; }
  .settings-msg { background: rgba(94,194,105,0.12); color: #5ec269;
    padding: 10px; border-radius: 8px; font-size: 13px; margin-bottom: 16px; text-align: center; }
  .settings-hint { font-size: 11px; color: #55556a; margin-top: 2px; }
</style>
</head>
<body>
<div class="settings-card">
  <div class="settings-header">
    <a href="/" title="Back">&#8592;</a>
    <div class="settings-title">Settings</div>
  </div>
  %s
  <form method="POST" action="/settings" enctype="multipart/form-data">
  <div class="settings-section">Server</div>
  <div class="settings-row">
    <span class="settings-label">Port</span>
    <input class="settings-input short" name="WEBMUX_PORT" value="%s" placeholder="3033">
  </div>
  <div class="settings-row">
    <span class="settings-label">Projects Root</span>
    <input class="settings-input" id="dev-root" name="WEBMUX_DEV_ROOT" value="%s" placeholder="~/Developer">
    <button type="button" onclick="browseDir()" style="padding:4px 10px;border-radius:6px;cursor:pointer;background:#14141a;border:1px solid #1e1e28;color:#9999b0;font-size:12px;flex-shrink:0">Browse</button>
  </div>
  <div id="dir-picker" style="display:none;margin-bottom:12px;max-height:200px;overflow-y:auto;background:#14141a;border:1px solid #1e1e28;border-radius:8px;padding:8px">
  </div>
  <div class="settings-row">
    <span class="settings-label">Start Command</span>
    <input class="settings-input" name="WEBMUX_CMD" value="%s" placeholder="claude --continue ...">
  </div>
  <div class="settings-section">Remote Access</div>
  <div class="settings-row">
    <span class="settings-label">Enable</span>
    <input type="hidden" name="WEBMUX_REMOTE" value="%s">
    <button type="button" class="%s"
      onclick="var v=this.classList.toggle('on'); this.previousElementSibling.value=v?'1':'0'"></button>
  </div>
  <div class="settings-row">
    <span class="settings-label">Remote Port</span>
    <input class="settings-input short" name="WEBMUX_REMOTE_PORT" value="%s" placeholder="3034">
  </div>
  <div class="settings-row">
    <span class="settings-label">Username</span>
    <input class="settings-input" name="WEBMUX_USER" value="%s" placeholder="admin">
  </div>
  <div class="settings-row">
    <span class="settings-label">Password</span>
    <input class="settings-input" type="password" name="WEBMUX_PASS" placeholder="leave empty to keep current">
  </div>
  <div class="settings-row">
    <span class="settings-label">Login Expires</span>
    <input class="settings-input short" name="WEBMUX_SESSION_DAYS" value="%s" placeholder="7">
    <span class="settings-suffix">days</span>
  </div>
  <div class="settings-section">SSL Certificate</div>
  <div class="settings-row">
    <span class="settings-label">Certificate</span>
    <span style="flex:1;font-size:13px;color:#9999b0">%s</span>
    <label style="padding:4px 10px;border-radius:6px;cursor:pointer;background:#14141a;border:1px solid #1e1e28;color:#9999b0;font-size:12px">
      Replace <input type="file" name="cert_file" accept=".pem,.crt" style="display:none"
        onchange="this.parentElement.textContent=this.files[0].name">
    </label>
  </div>
  <div class="settings-row">
    <span class="settings-label">Private Key</span>
    <span style="flex:1;font-size:13px;color:#9999b0">%s</span>
    <label style="padding:4px 10px;border-radius:6px;cursor:pointer;background:#14141a;border:1px solid #1e1e28;color:#9999b0;font-size:12px">
      Replace <input type="file" name="key_file" accept=".pem,.key" style="display:none"
        onchange="this.parentElement.textContent=this.files[0].name">
    </label>
  </div>
  <div class="settings-hint">Leave as-is unless replacing certificates.</div>
  <button class="settings-btn" type="submit">Save</button>
  </form>
</div>
<script>
function browseDir() {
  var current = document.getElementById('dev-root').value || '/';
  loadDir(current);
}
function loadDir(path) {
  fetch('/api/browse?path=' + encodeURIComponent(path))
    .then(function(r) { return r.json(); })
    .then(function(data) {
      var picker = document.getElementById('dir-picker');
      picker.style.display = 'block';
      picker.innerHTML = '';
      var up = document.createElement('div');
      up.textContent = '.. (up)';
      up.style.cssText = 'padding:4px 8px;cursor:pointer;color:#e8a849;font-size:13px';
      up.onclick = function() { loadDir(data.parent); };
      picker.appendChild(up);
      var cur = document.createElement('div');
      cur.textContent = '[ Select: ' + data.path + ' ]';
      cur.style.cssText = 'padding:4px 8px;cursor:pointer;color:#5ec269;font-size:13px;font-weight:600';
      cur.onclick = function() {
        document.getElementById('dev-root').value = data.path;
        picker.style.display = 'none';
      };
      picker.appendChild(cur);
      data.dirs.forEach(function(d) {
        var el = document.createElement('div');
        el.textContent = d;
        el.style.cssText = 'padding:4px 8px;cursor:pointer;color:#9999b0;font-size:13px';
        el.onmouseover = function() { el.style.color = '#e4e4ed'; };
        el.onmouseout = function() { el.style.color = '#9999b0'; };
        el.onclick = function() { loadDir(data.path + '/' + d); };
        picker.appendChild(el);
      });
    });
}
</script>
</body></html>""" % (
        msg, c_port, esc(c_dev_root), esc(c_cmd), toggle_val, toggle_cls,
        c_remote_port, esc(c_user), c_days,
        esc(os.path.basename(c_cert) or "self-signed"),
        esc(os.path.basename(c_key) or "self-signed"),
    )

LOGIN_HTML = r"""<!DOCTYPE html>
<html><head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>webmux — login</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'><rect width='32' height='32' rx='7' fill='%230f0f13'/><text x='16' y='22' text-anchor='middle' font-family='Helvetica,sans-serif' font-weight='700' font-size='18' fill='%23e8a849'>W</text></svg>">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Outfit:wght@400;600&display=swap" rel="stylesheet">
<style>
  * { margin: 0; padding: 0; box-sizing: border-box; }
  body {
    font-family: 'Outfit', sans-serif;
    background: #09090b;
    color: #e4e4ed;
    height: 100vh;
    display: flex;
    align-items: center;
    justify-content: center;
    -webkit-font-smoothing: antialiased;
  }
  .login-card {
    width: 320px;
    background: #0f0f13;
    border: 1px solid #1e1e28;
    border-radius: 16px;
    padding: 40px 32px;
    text-align: center;
  }
  .login-logo {
    width: 56px; height: 56px; border-radius: 14px;
    background: #0f0f13;
    border: 1px solid #2a2a38;
    display: inline-flex; align-items: center; justify-content: center;
    font-weight: 700; font-size: 28px; color: #e8a849;
    font-family: Helvetica, sans-serif;
    margin-bottom: 20px;
  }
  .login-title { font-size: 20px; font-weight: 600; margin-bottom: 6px; }
  .login-sub { font-size: 13px; color: #686880; margin-bottom: 24px; }
  .login-input {
    width: 100%; padding: 12px 16px;
    background: #14141a; border: 1px solid #1e1e28;
    border-radius: 10px; color: #e4e4ed;
    font-family: 'Outfit', sans-serif; font-size: 14px;
    outline: none; margin-bottom: 16px;
    transition: border-color 0.15s;
  }
  .login-input:focus { border-color: #e8a849; }
  .login-input::placeholder { color: #55556a; }
  .login-btn {
    width: 100%; padding: 12px;
    background: #e8a849; color: #09090b;
    border: none; border-radius: 10px;
    font-family: 'Outfit', sans-serif;
    font-size: 14px; font-weight: 600;
    cursor: pointer; transition: opacity 0.15s;
  }
  .login-btn:hover { opacity: 0.85; }
  .login-error {
    color: #e05555; font-size: 13px;
    margin-bottom: 12px;
  }
</style>
</head>
<body>
<div class="login-card">
  <div class="login-logo">W</div>
  <div class="login-title">webmux</div>
  <div class="login-sub">Sign in to continue</div>
  <!--ERROR-->
  <form method="POST" action="/login">
    <input class="login-input" type="text" name="username" placeholder="Username" autofocus autocomplete="username">
    <input class="login-input" type="password" name="password" placeholder="Password" autocomplete="current-password">
    <button class="login-btn" type="submit">Sign in</button>
  </form>
</div>
</body></html>"""

HTML = r"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>webmux</title>
<link rel="icon" href="data:image/svg+xml,<svg xmlns='http://www.w3.org/2000/svg' viewBox='0 0 32 32'><rect width='32' height='32' rx='7' fill='%230f0f13'/><text x='16' y='22' text-anchor='middle' font-family='Helvetica,sans-serif' font-weight='700' font-size='18' fill='%23e8a849'>W</text></svg>">
<link rel="preconnect" href="https://fonts.googleapis.com">
<link href="https://fonts.googleapis.com/css2?family=Outfit:wght@300;400;500;600&family=JetBrains+Mono:wght@300;400;500&display=swap" rel="stylesheet">
<link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/@xterm/xterm@5.5.0/css/xterm.css">
<script src="https://cdn.jsdelivr.net/npm/@xterm/xterm@5.5.0/lib/xterm.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/@xterm/addon-fit@0.10.0/lib/addon-fit.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/@xterm/addon-web-links@0.11.0/lib/addon-web-links.min.js"></script>
<script src="https://cdn.jsdelivr.net/npm/@xterm/addon-canvas@0.7.0/lib/addon-canvas.min.js"></script>
<script>if(localStorage.getItem('webmux-theme')==='light')document.documentElement.classList.add('light');</script>
<style>
  :root {
    --bg-base: #09090b;
    --bg-sidebar: #0f0f13;
    --bg-sidebar-hover: #18181f;
    --bg-sidebar-active: #1c1c25;
    --bg-input: #14141a;
    --border: #1e1e28;
    --border-light: #2a2a38;
    --text: #e4e4ed;
    --text-dim: #9999b0;
    --text-muted: #686880;
    --accent: #e8a849;
    --accent-dim: rgba(232,168,73,0.12);
    --accent-glow: rgba(232,168,73,0.25);
    --green: #5ec269;
    --green-dim: rgba(94,194,105,0.15);
    --red: #e05555;
    --red-dim: rgba(224,85,85,0.12);
    --font: 'Outfit', sans-serif;
    --mono: 'JetBrains Mono', monospace;
    --sidebar-w: 260px;
    --radius: 10px;
    /* Sidebar tree — VS Code-style even indent staircase. Row left edge is
       fixed; each level's TEXT is pushed one --tree-step deeper via padding.
       Carets are absolutely positioned in the 16px slot just left of the text,
       so caret width never shifts text alignment. One vertical guide line runs
       down each expanded category, aligned to the project caret column. */
    --tree-step: 16px;
    --tree-cat: 14px;    /* category label text x */
    --tree-proj: 30px;   /* project name text x   (cat + step) */
    --tree-sess: 46px;   /* session name text x   (proj + step) */
    --tree-caret: 16px;  /* caret slot width */
    --tree-font-base: 14px;           /* one font size for the whole tree… */
    --tree-font-scale: 1;             /* …scaled by the sidebar A-/A+ control */
    --tree-font: calc(var(--tree-font-base) * var(--tree-font-scale));
  }

  body.light, html.light {
    --bg-base: #f5f4f1;
    --bg-sidebar: #eae8e4;
    --bg-sidebar-hover: #dedad4;
    --bg-sidebar-active: #d5d1ca;
    --bg-input: #e5e3df;
    --border: #d0cdc6;
    --border-light: #c5c2bb;
    --text: #2a2826;
    --text-dim: #5a5752;
    --text-muted: #8a8680;
    --accent: #c48a2a;
    --accent-dim: rgba(196,138,42,0.12);
    --accent-glow: rgba(196,138,42,0.25);
    --green: #3a8a45;
    --green-dim: rgba(58,138,69,0.15);
    --red: #c93c3c;
    --red-dim: rgba(201,60,60,0.12);
  }
  body.light .session-item.active { background: rgba(196,138,42,0.08); }
  body.light #sidebar { box-shadow: 1px 0 12px rgba(0,0,0,0.08); }
  body.light .modal-card { box-shadow: 0 24px 80px rgba(0,0,0,0.15); }
  body.light .confirm-popup { box-shadow: 0 8px 32px rgba(0,0,0,0.12); }

  * { margin: 0; padding: 0; box-sizing: border-box; }

  body {
    font-family: var(--font);
    background: var(--bg-base);
    color: var(--text);
    height: 100vh;
    display: flex;
    overflow: hidden;
    -webkit-font-smoothing: antialiased;
  }

  /* --- Sidebar --- */
  #sidebar {
    width: var(--sidebar-w);
    min-width: var(--sidebar-w);
    background: var(--bg-sidebar);
    border-right: none;
    box-shadow: 1px 0 12px rgba(0,0,0,0.3);
    display: flex;
    flex-direction: column;
    transition: margin-left 0.25s cubic-bezier(0.4, 0, 0.2, 1);
    will-change: margin-left;
    z-index: 10;
    position: relative;
  }
  #sidebar.collapsed { margin-left: calc(-1 * var(--sidebar-w)); }

  .sidebar-header {
    padding: 20px 20px 16px 20px;
    display: flex;
    align-items: center;
    gap: 12px;
    border-bottom: 1px solid var(--border);
  }
  .sidebar-logo {
    width: 36px; height: 36px; border-radius: 9px;
    background: #0f0f13;
    color: var(--accent);
    font-family: 'Helvetica Neue', Helvetica, sans-serif;
    font-weight: 700; font-size: 20px;
    border: 1px solid var(--border);
    display: grid; place-items: center;
  }
  body.light .sidebar-logo { background: #2a2826; border-color: #3a3835; }
  .sidebar-title { font-size: 18px; font-weight: 600; letter-spacing: -0.02em; }
  .sidebar-subtitle { font-size: 12px; color: var(--text-muted); margin-top: 1px; }

  .sidebar-section {
    padding: 8px 12px 4px 12px;
    font-size: 13px;
    font-weight: 500;
    color: var(--text-muted);
    text-transform: uppercase;
    letter-spacing: 0.08em;
    display: flex; align-items: center; justify-content: space-between;
  }
  /* Font controls stay hidden until you hover the Projects header — font size
     is a rare tweak, so it shouldn't sit there as permanent visual noise. */
  .sb-font-controls { display: flex; gap: 2px; opacity: 0; transition: opacity 0.12s; }
  .sidebar-section:hover .sb-font-controls { opacity: 1; }
  .sb-font-btn {
    width: 20px; height: 18px; border-radius: 4px;
    display: flex; align-items: center; justify-content: center;
    font-size: 11px; letter-spacing: 0; color: var(--text-muted);
    cursor: pointer; transition: background 0.12s, color 0.12s;
  }
  .sb-font-btn:hover { background: var(--bg-sidebar-hover); color: var(--accent); }

  #session-list {
    flex: 1;
    overflow-y: auto;
    padding: 4px 8px;
  }
  #session-list::-webkit-scrollbar { width: 5px; }
  #session-list::-webkit-scrollbar-thumb { background: var(--border-light); border-radius: 2px; }

  .session-item {
    display: flex;
    align-items: center;
    gap: 10px;
    padding: 6px 14px;
    border-radius: var(--radius);
    cursor: pointer;
    transition: background 0.12s ease;
    margin-bottom: 1px;
    position: relative;
  }
  .session-item.dragging { opacity: 0.4; }
  .session-item.drag-over { border-top: 2px solid var(--accent); margin-top: -2px; }
  .session-item:hover { background: var(--bg-sidebar-hover); }
  .session-item.active { background: rgba(232,168,73,0.06); }
  .session-item.active::after {
    content: '';
    position: absolute;
    left: 0; top: 50%;
    transform: translateY(-50%);
    width: 3px; height: 18px;
    background: var(--accent);
    border-radius: 0 3px 3px 0;
  }

  /* Status dot — retired from the sidebar (a wall of identical green dots
     carried no signal). Kept for the Cmd+K palette + history list only. */
  .session-dot { width: 8px; height: 8px; border-radius: 50%; flex-shrink: 0; }
  .session-dot.active { background: var(--green); box-shadow: 0 0 6px var(--green-dim); }
  .session-dot.hist-dot {
    width: 6px; height: 6px;
    border: 1px solid var(--text-muted); background: transparent; opacity: 0.6;
  }

  .session-info { flex: 1; overflow: hidden; }
  .session-name {
    font-size: var(--tree-font); font-weight: 400; white-space: nowrap;
    overflow: hidden; text-overflow: ellipsis; color: var(--text-dim);
  }
  .session-item.active .session-name { color: var(--text); }
  .session-item:hover .session-name { color: var(--text); }
  /* Ended (not-live) sessions read dimmed. */
  .session-item.dead .session-name { color: var(--text-muted); }
  /* Unread (inbox model): new output since you last opened it → a LEADING dot
     before the name, like an unread email. On the left so it never collides
     with the rename/kill actions that slide in from the right on hover.
     Cleared the moment you view the session. */
  .session-item.unread .session-name::before,
  .repo-group.unread .repo-name::before {
    content: ''; display: inline-block;
    width: 7px; height: 7px; border-radius: 50%; margin-right: 7px;
    vertical-align: middle;
    background: var(--accent); box-shadow: 0 0 6px var(--accent-glow);
  }
  .session-path {
    font-size: 11px; color: var(--text-muted); white-space: nowrap;
    overflow: hidden; text-overflow: ellipsis; margin-top: 1px;
    font-family: var(--mono);
  }

  /* Row actions overlay the right edge ONLY on hover → zero layout width, so the
     session name always gets the full row. Stacked VERTICALLY so the overlay is
     ~half the width (covers less of the name on hover). Gradient backdrop keeps
     text under it readable. */
  .row-actions {
    position: absolute; top: 0; right: 3px; bottom: 0;
    display: flex; flex-direction: column; align-items: center; justify-content: center; gap: 1px;
    padding-left: 18px; opacity: 0; pointer-events: none;
    background: linear-gradient(to right, transparent, var(--bg-sidebar-hover) 55%);
    transition: opacity 0.12s;
  }
  .session-item:hover .row-actions,
  .repo-group:hover .row-actions { opacity: 1; pointer-events: auto; }
  .session-kill {
    width: 22px; height: 18px; border-radius: 4px;
    display: flex; align-items: center; justify-content: center;
    color: var(--text-muted); font-size: 14px; cursor: pointer;
    transition: background 0.15s, color 0.15s; flex-shrink: 0;
  }
  .session-kill:hover { background: var(--red-dim); color: var(--red); }
  .session-rename {
    width: 22px; height: 18px; border-radius: 4px;
    display: flex; align-items: center; justify-content: center;
    color: var(--text-muted); font-size: 11px; cursor: pointer;
    transition: background 0.15s, color 0.15s; flex-shrink: 0;
  }
  .session-rename:hover { background: var(--accent-dim); color: var(--accent); }

  /* ===== Sidebar tree (VS Code-style): CATEGORY > PROJECT > SESSION =====
     Even indent staircase, carets absolutely positioned, one font size, one
     guide line per expanded category. Level = indentation + colour, not size. */

  /* PROJECT (level 2) */
  .repo-group {
    display: flex; align-items: center; position: relative;
    padding: 6px 12px 6px var(--tree-proj); cursor: pointer; user-select: none;
    color: var(--text); font-size: var(--tree-font); font-weight: 500;
    border-radius: var(--radius); margin-bottom: 1px;
    transition: background 0.12s, color 0.12s;
  }
  .repo-group:hover { background: var(--bg-sidebar-hover); }
  /* Project containing the session you're attached to — accent bar + faint tint
     is enough to show which project is on the terminal (even when collapsed).
     No name recolouring: the bar+tint IS the signal, and recolouring the name
     would just collide with the accent `unread` name. */
  .repo-group.has-active { background: var(--bg-sidebar-active); }
  .repo-group.has-active::after {
    content: ''; position: absolute;
    left: 0; top: 50%; transform: translateY(-50%);
    width: 3px; height: 18px;
    background: var(--accent); border-radius: 0 3px 3px 0;
  }
  .repo-caret {
    position: absolute; left: calc(var(--tree-proj) - var(--tree-caret));
    top: 50%; transform: translateY(-50%);
    font-size: 10px; width: var(--tree-caret); height: 18px;
    display: flex; align-items: center; justify-content: center;
    color: var(--text-muted); transition: color 0.12s;
  }
  .repo-caret:hover { color: var(--accent); }
  .repo-name { flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .repo-actions { padding-left: 14px; }
  .repo-new {
    width: 22px; height: 22px;
    display: flex; align-items: center; justify-content: center;
    border-radius: 4px; font-size: 16px; color: var(--text-muted);
    transition: background 0.12s, color 0.12s;
  }
  .repo-new:hover { background: var(--accent-dim); color: var(--accent); }

  /* SESSION (level 3) — same font size, dimmer, one step deeper, no caret. */
  .session-item.nested { padding-left: var(--tree-sess); }

  /* One guide line down each expanded category, aligned to the PROJECT caret
     column (a single, consistent vertical rule — not per-row lines). */
  .category-body { position: relative; }
  .category-body::before {
    content: ''; position: absolute;
    left: calc(var(--tree-proj) - var(--tree-caret) / 2 - 1px);
    top: 0; bottom: 4px; width: 1px; background: var(--border);
  }

  /* CATEGORY (level 1) — section header: smallest indent, uppercase label. */
  .category-header {
    display: flex; align-items: center; position: relative;
    padding: 12px 12px 5px var(--tree-cat); cursor: pointer; user-select: none;
    color: var(--text-muted); font-size: 11px; font-weight: 600;
    text-transform: uppercase; letter-spacing: 0.06em;
    transition: color 0.12s; border-radius: 4px;
  }
  .category-header:hover { color: var(--text-dim); }
  .category-header .repo-caret {
    left: calc(var(--tree-cat) - var(--tree-caret));
    font-size: 9px;
  }
  .category-header.drag-over { background: var(--accent-dim); outline: 1px dashed var(--accent); }
  .category-header.uncat { color: var(--text-muted); }
  .cat-name { flex: 1; overflow: hidden; text-overflow: ellipsis; white-space: nowrap; }
  .cat-del { opacity: 0; color: var(--text-muted); font-size: 14px; transition: all 0.15s; }
  .category-header:hover .cat-del { opacity: 0.5; }
  .cat-del:hover { opacity: 1 !important; color: var(--red); }
  .cat-rename { opacity: 0; color: var(--text-muted); font-size: 11px; transition: all 0.15s; margin-left: 4px; }
  .category-header:hover .cat-rename { opacity: 0.5; }
  .cat-rename:hover { opacity: 1 !important; color: var(--accent); }
  .repo-group.dragging { opacity: 0.4; }
  .repo-group.drag-over { background: var(--accent-dim); outline: 1px dashed var(--accent); outline-offset: -1px; }

  /* Layer 2: session-history rows (a conversation; live or resumable) */
  .session-item.hist { padding-top: 6px; padding-bottom: 6px; }
  .session-item.hist .session-name { font-weight: 400; font-size: var(--tree-font); }
  .session-item.hist:not(.live) .session-name { color: var(--text-muted); }
  .session-item.hist:not(.live):hover .session-name { color: var(--text-dim); }
  .session-item.hist .session-path { font-size: 11px; opacity: 0.75; }
  .session-dot.hist-dot {
    width: 6px; height: 6px;
    border: 1px solid var(--text-muted); background: transparent; opacity: 0.6;
    box-shadow: none;
  }
  .conv-loading.nested { padding: 7px 12px 7px var(--tree-sess); font-size: 11px; color: var(--text-muted); font-style: italic; }

  .history-section { color: var(--text-muted); font-size: 11px; }
  .history-item {
    display: flex; align-items: center; gap: 10px;
    padding: 8px 12px 8px 10px; cursor: pointer; opacity: 0.5;
    border-left: 2px dashed var(--border-light); margin-left: 12px;
    transition: all 0.15s; position: relative;
  }
  .history-item:hover { background: var(--bg-sidebar-hover); opacity: 0.8; }
  .history-item .session-name { color: var(--text-muted); }
  .history-item .session-dot { background: var(--text-muted); opacity: 0.3; }
  .history-item .history-remove {
    opacity: 0; width: 22px; height: 22px; border-radius: 5px;
    display: flex; align-items: center; justify-content: center;
    color: var(--text-muted); font-size: 14px; cursor: pointer;
    transition: all 0.15s; flex-shrink: 0;
  }
  .history-item:hover .history-remove { opacity: 0.6; }
  .history-item .history-remove:hover { opacity: 1 !important; color: var(--red); }

  /* Single row of equal-weight icon buttons */
  .sidebar-bottom {
    padding: 10px;
    border-top: 1px solid var(--border);
    display: flex; align-items: stretch; gap: 6px;
  }
  .sb-icon {
    flex: 1; height: 38px;
    background: transparent;
    border: none;
    border-radius: 8px;
    color: var(--text-muted);
    font-size: 17px; line-height: 1;
    cursor: pointer;
    transition: all 0.15s;
    display: flex; align-items: center; justify-content: center;
  }
  .sb-icon:hover {
    color: var(--accent);
    background: var(--bg-sidebar-hover);
  }
  .sb-icon.busy { color: var(--accent); }

  /* Unified icon system: every toolbar/status glyph is an inline Lucide-style
     line SVG (currentColor, one stroke weight + cap), replacing the old mix of
     emoji + dingbats + ad-hoc SVGs. */
  svg { display: inline-block; vertical-align: middle; }
  .sb-icon svg { width: 18px; height: 18px; }
  .upload-btn svg, #sidebar-toggle svg { width: 15px; height: 15px; }
  .sb-icon svg, .upload-btn svg, #sidebar-toggle svg {
    fill: none; stroke: currentColor; stroke-width: 2;
    stroke-linecap: round; stroke-linejoin: round;
  }
  .upload-btn { display: inline-flex; align-items: center; gap: 6px; }
  .upload-btn.icon-only { gap: 0; }

  /* --- Toggle --- */
  #sidebar-toggle {
    position: fixed;
    bottom: 14px; left: 14px;
    width: 38px; height: 38px;
    background: rgba(15,15,19,0.55);
    backdrop-filter: blur(8px);
    -webkit-backdrop-filter: blur(8px);
    border: 1px solid rgba(255,255,255,0.06);
    border-radius: 8px;
    cursor: pointer;
    display: none;
    align-items: center; justify-content: center;
    color: var(--text-muted);
    font-size: 17px;
    opacity: 0.5;
    z-index: 20;
    transition: all 0.15s;
  }
  #sidebar-toggle:hover {
    color: var(--accent);
    background: rgba(15,15,19,0.85);
    opacity: 1;
  }
  #sidebar.collapsed ~ #main-area #sidebar-toggle { display: flex; }

  /* --- Main Terminal Area --- */
  #main-area {
    flex: 1;
    display: flex;
    flex-direction: column;
    position: relative;
    overflow: hidden;
    background: var(--bg-base);
  }
  #terminal-container {
    flex: 1;
    padding: 4px;
    overflow: hidden;
  }
  #terminal-container .xterm { height: 100%; }

  .terminal-status {
    padding: 6px 16px;
    background: var(--bg-sidebar);
    border-top: 1px solid var(--border);
    font-size: 12px;
    color: var(--text-muted);
    font-family: var(--mono);
    display: flex;
    align-items: center;
    gap: 12px;
  }
  .terminal-status .connected { color: var(--green); }
  .terminal-status .disconnected { color: var(--red); }
  .font-controls {
    display: flex; align-items: center; gap: 6px; margin-right: 12px;
  }
  .font-btn {
    padding: 2px 7px; border-radius: 4px; cursor: pointer;
    background: var(--bg-sidebar-hover); color: var(--text-dim);
    transition: all 0.1s; user-select: none; font-size: 12px;
  }
  .font-btn:hover { background: var(--border-light); color: var(--text); }
  #font-size-display { font-size: 12px; min-width: 18px; text-align: center; }

  /* No session selected */
  #no-session {
    flex: 1;
    display: flex;
    flex-direction: column;
    align-items: center;
    justify-content: center;
    gap: 16px;
    color: var(--text-muted);
  }
  #no-session .icon { font-size: 48px; opacity: 0.3; }
  #no-session .hint { font-size: 14px; }

  /* --- Modal --- */
  .modal-overlay {
    position: fixed; inset: 0;
    background: radial-gradient(ellipse at center, rgba(0,0,0,0.55) 0%, rgba(0,0,0,0.75) 100%);
    backdrop-filter: blur(6px);
    display: flex; align-items: center; justify-content: center;
    z-index: 100;
    opacity: 0; pointer-events: none;
    transition: opacity 0.2s ease;
  }
  .modal-overlay.open { opacity: 1; pointer-events: auto; }

  .modal-card {
    background: var(--bg-sidebar);
    border: 1px solid var(--border-light);
    border-radius: 14px;
    width: 480px; max-width: 92vw;
    max-height: 65vh;
    display: flex; flex-direction: column;
    box-shadow: 0 24px 80px rgba(0,0,0,0.6);
    transform: translateY(10px) scale(0.98);
    transition: transform 0.25s cubic-bezier(0.4, 0, 0.2, 1);
  }
  .modal-overlay.open .modal-card { transform: translateY(0) scale(1); }

  .modal-header {
    padding: 18px 20px 14px 20px;
    display: flex; justify-content: space-between; align-items: center;
    border-bottom: 1px solid var(--border);
  }
  .modal-title { font-size: 15px; font-weight: 600; }
  .modal-close {
    width: 28px; height: 28px; border-radius: 6px;
    display: flex; align-items: center; justify-content: center;
    cursor: pointer; color: var(--text-muted); font-size: 18px;
    transition: all 0.1s;
  }
  .modal-close:hover { background: var(--bg-sidebar-hover); color: var(--text); }

  .modal-search {
    margin: 12px 16px 4px 16px;
    padding: 10px 14px;
    background: var(--bg-input);
    border: 1px solid var(--border);
    border-radius: 8px;
    color: var(--text);
    font-family: var(--font);
    font-size: 13px;
    outline: none;
    width: calc(100% - 32px);
    transition: border-color 0.15s;
  }
  .modal-search:focus { border-color: var(--accent); }
  .modal-search::placeholder { color: var(--text-muted); }

  .modal-list {
    flex: 1; overflow-y: auto;
    padding: 8px;
  }
  .modal-list::-webkit-scrollbar { width: 4px; }
  .modal-list::-webkit-scrollbar-thumb { background: var(--border-light); border-radius: 2px; }

  /* New Project modal */
  .np-card { width: 420px; }
  .np-body { padding: 20px 20px 8px 20px; }
  .np-label {
    display: block; font-size: 11px; font-weight: 600;
    text-transform: uppercase; letter-spacing: 0.05em;
    color: var(--text-muted); margin-bottom: 8px;
  }
  .np-input {
    width: 100%; box-sizing: border-box;
    padding: 11px 14px;
    background: var(--bg-input);
    border: 1px solid var(--border-light);
    border-radius: 8px;
    color: var(--text);
    font-family: var(--font);
    font-size: 14px;
    outline: none;
    transition: border-color 0.15s, box-shadow 0.15s;
  }
  .np-input:focus { border-color: var(--accent); box-shadow: 0 0 0 3px var(--accent-dim); }
  .np-input::placeholder { color: var(--text-muted); }
  .np-hint {
    margin-top: 10px; font-size: 12px; color: var(--text-muted);
    line-height: 1.5;
  }
  .np-path {
    font-family: var(--font-mono, ui-monospace, monospace);
    color: var(--text-dim);
  }
  .np-name { color: var(--accent); }
  .np-actions {
    display: flex; justify-content: flex-end; gap: 8px;
    padding: 14px 20px 18px 20px;
  }
  .np-btn {
    padding: 8px 16px; border-radius: 8px;
    font-family: var(--font); font-size: 13px; font-weight: 500;
    cursor: pointer; transition: all 0.15s; border: 1px solid transparent;
  }
  .np-cancel {
    background: transparent; border-color: var(--border-light); color: var(--text-dim);
  }
  .np-cancel:hover { border-color: var(--text-muted); color: var(--text); }
  .np-create {
    background: var(--accent); color: #1a1206; border-color: var(--accent);
  }
  .np-create:hover { filter: brightness(1.08); }

  .project-item {
    padding: 10px 14px;
    border-radius: 8px;
    cursor: pointer;
    transition: all 0.1s;
    display: flex;
    align-items: center;
    gap: 10px;
  }
  .project-item:hover { background: var(--bg-sidebar-hover); }
  .project-name { font-size: 13px; font-weight: 500; }
  .project-org { font-size: 10px; color: var(--text-muted); font-family: var(--mono); }

  /* Cmd+K palette rows */
  .palette-item {
    display: flex; align-items: center; gap: 10px;
    padding: 9px 14px; border-radius: 8px; cursor: pointer;
    transition: background 0.1s;
  }
  .palette-item:hover { background: var(--bg-sidebar-hover); }
  .palette-item.sel { background: var(--accent-dim); outline: 1px solid rgba(232,168,73,0.3); outline-offset: -1px; }
  .palette-tag {
    font-size: 10px; font-weight: 500; color: var(--accent);
    background: var(--accent-dim); border-radius: 4px; padding: 1px 6px; margin-left: 6px;
  }

  /* --- Confirm Popup --- */
  .confirm-popup {
    position: fixed;
    background: var(--bg-sidebar);
    border: 1px solid var(--border-light);
    border-radius: 10px;
    padding: 14px 16px;
    box-shadow: 0 8px 32px rgba(0,0,0,0.5);
    z-index: 60;
    font-size: 13px;
    display: flex; align-items: center; gap: 10px;
  }
  .confirm-popup button {
    padding: 5px 14px;
    border-radius: 6px;
    border: none;
    cursor: pointer;
    font-family: var(--font);
    font-size: 12px;
    font-weight: 500;
    transition: all 0.1s;
  }
  .confirm-popup .btn-kill { background: var(--red-dim); color: var(--red); }
  .confirm-popup .btn-kill:hover { background: var(--red); color: #fff; }
  .confirm-popup .btn-cancel { background: var(--bg-sidebar-hover); color: var(--text-dim); }
  .confirm-popup .btn-cancel:hover { color: var(--text); }

  /* Floating hover tooltip — escapes the fixed-width sidebar so long session
     names + full meta are readable without widening the sidebar. */
  #wm-tip {
    position: fixed;
    z-index: 200;
    max-width: 460px;
    padding: 9px 12px;
    background: var(--bg-sidebar);
    border: 1px solid var(--border-light);
    border-radius: 8px;
    box-shadow: 0 8px 32px rgba(0,0,0,0.5);
    pointer-events: none;
    opacity: 0;
    transition: opacity 0.12s;
    font-size: 13px; line-height: 1.45;
  }
  #wm-tip.show { opacity: 1; }
  #wm-tip .wm-tip-name { color: var(--text); font-weight: 500; word-break: break-word; }
  #wm-tip .wm-tip-meta { color: var(--text-muted); font-size: 11px; margin-top: 3px; font-family: var(--mono); }
  body.light #wm-tip { box-shadow: 0 8px 32px rgba(0,0,0,0.12); }

  /* Drag-drop overlay */
  #drop-overlay {
    position: absolute; inset: 0; z-index: 40;
    background: rgba(232,168,73,0.08);
    border: 2px dashed var(--accent);
    display: none;
    align-items: center; justify-content: center;
    pointer-events: none;
  }
  #drop-overlay.visible { display: flex; }
  #drop-overlay .drop-text {
    font-size: 18px; font-weight: 600; color: var(--accent);
    background: var(--bg-sidebar); padding: 16px 32px; border-radius: 12px;
  }
  .upload-btn {
    padding: 2px 8px; border-radius: 4px; cursor: pointer;
    background: var(--bg-sidebar-hover); color: var(--text-dim);
    transition: all 0.1s; user-select: none; font-size: 12px;
    margin-right: 6px;
  }
  .upload-btn:hover { background: var(--border-light); color: var(--text); }

  .icon-btn {
    cursor: pointer; opacity: 0.7; padding: 4px;
    color: var(--text-dim); transition: all 0.15s;
  }
  .icon-btn:hover { opacity: 1; color: var(--accent); }
  #file-input { display: none; }

  #mobile-keys {
    display: none; gap: 4px;
  }
  .mkey {
    padding: 2px 6px; border-radius: 4px; cursor: pointer;
    background: var(--bg-sidebar-hover); color: var(--text-dim);
    font-family: var(--mono); font-size: 11px;
    user-select: none; -webkit-tap-highlight-color: transparent;
  }
  .mkey:active { background: var(--accent-dim); color: var(--accent); }

  @media (max-width: 768px) {
    #sidebar { position: fixed; height: 100%; z-index: 30; }
    #sidebar.collapsed { margin-left: calc(-1 * var(--sidebar-w)); }
    #sidebar-toggle { display: flex !important; }
  }
  @media (pointer: coarse) {
    #mobile-keys { display: inline-flex; }
  }
</style>
</head>
<body>

<div id="sidebar">
  <div class="sidebar-header">
    <div class="sidebar-logo">W</div>
    <div style="flex:1">
      <div class="sidebar-title">webmux</div>
      <div class="sidebar-subtitle">tmux in your browser</div>
    </div>
  </div>
  <div class="sidebar-section">
    <span>Projects</span>
    <span class="sb-font-controls">
      <span class="sb-font-btn" onclick="changeSidebarFont(-1)" title="Smaller sidebar text">A-</span>
      <span class="sb-font-btn" onclick="changeSidebarFont(1)" title="Larger sidebar text">A+</span>
    </span>
  </div>
  <div id="session-list"></div>
  <div class="sidebar-section history-section" id="history-header" style="display:none">History</div>
  <div id="history-list"></div>
  <div class="sidebar-bottom">
    <button class="sb-icon" onclick="openModal()" title="Open project"><svg viewBox="0 0 24 24"><path d="M3 7a2 2 0 0 1 2-2h4l2 2h8a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/></svg></button>
    <button class="sb-icon" onclick="newProject()" title="New project"><svg viewBox="0 0 24 24"><path d="M12 5v14M5 12h14"/></svg></button>
    <button class="sb-icon" onclick="addCategory()" title="New category"><svg viewBox="0 0 24 24"><path d="M3 7a2 2 0 0 1 2-2h4l2 2h8a2 2 0 0 1 2 2v8a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2z"/><path d="M12 11v4M10 13h4"/></svg></button>
    <button class="sb-icon" id="restart-all-btn" onclick="restartAll()" title="Restart all Claude sessions"><svg viewBox="0 0 24 24"><path d="M21 12a9 9 0 1 1-2.64-6.36"/><path d="M21 3v6h-6"/></svg></button>
    <button class="sb-icon" onclick="toggleSidebar()" title="Hide sidebar"><svg viewBox="0 0 24 24"><path d="M15 6l-6 6 6 6"/></svg></button>
  </div>
</div>

<div id="main-area">
  <div id="sidebar-toggle" onclick="toggleSidebar()" title="Show sidebar"><svg viewBox="0 0 24 24"><path d="M9 6l6 6-6 6"/></svg></div>
  <div id="terminal-container"></div>
  <div id="drop-overlay"><span class="drop-text">Drop files to attach</span></div>
  <input type="file" id="file-input" multiple />
  <div id="no-session">
    <div class="icon">&#9002;</div>
    <div class="hint">Select a session to connect</div>
  </div>
  <div class="terminal-status">
    <span id="conn-status" class="connected">online</span>
    <span id="conn-session">—</span>
    <span id="mobile-keys">
      <span class="mkey" onclick="sendKey('\x03')">^C</span>
      <span class="mkey" onclick="sendKey('\x04')">^D</span>
      <span class="mkey" onclick="sendKey('\t')">Tab</span>
      <span class="mkey" onclick="sendKey('\x1b')">Esc</span>
      <span class="mkey" onclick="sendKey('\x1b[A')">↑</span>
      <span class="mkey" onclick="sendKey('\x1b[B')">↓</span>
    </span>
    <span style="flex:1"></span>
    <span class="upload-btn" onclick="document.getElementById('file-input').click()" title="Attach files"><svg viewBox="0 0 24 24"><path d="M21.44 11.05l-9.19 9.19a6 6 0 0 1-8.49-8.49l9.19-9.19a4 4 0 0 1 5.66 5.66l-9.2 9.19a2 2 0 0 1-2.83-2.83l8.49-8.48"/></svg><span class="btn-label">attach</span></span>
    <span class="font-controls">
      <span class="font-btn" onclick="changeFontSize(-1)" title="Smaller (Ctrl+-)">A-</span>
      <span id="font-size-display">18</span>
      <span class="font-btn" onclick="changeFontSize(1)" title="Larger (Ctrl+=)">A+</span>
    </span>
    <span id="term-size">—</span>
    <span id="theme-toggle" class="upload-btn icon-only" onclick="toggleTheme()" title="Toggle theme"><svg viewBox="0 0 24 24"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"></path></svg></span>
    <a href="/settings" class="upload-btn icon-only" title="Settings" style="text-decoration:none"><svg viewBox="0 0 24 24"><circle cx="12" cy="12" r="3"/><path d="M19.4 15a1.65 1.65 0 0 0 .33 1.82l.06.06a2 2 0 1 1-2.83 2.83l-.06-.06a1.65 1.65 0 0 0-1.82-.33 1.65 1.65 0 0 0-1 1.51V21a2 2 0 0 1-4 0v-.09A1.65 1.65 0 0 0 9 19.4a1.65 1.65 0 0 0-1.82.33l-.06.06a2 2 0 1 1-2.83-2.83l.06-.06a1.65 1.65 0 0 0 .33-1.82 1.65 1.65 0 0 0-1.51-1H3a2 2 0 0 1 0-4h.09A1.65 1.65 0 0 0 4.6 9a1.65 1.65 0 0 0-.33-1.82l-.06-.06a2 2 0 1 1 2.83-2.83l.06.06a1.65 1.65 0 0 0 1.82.33H9a1.65 1.65 0 0 0 1-1.51V3a2 2 0 0 1 4 0v.09a1.65 1.65 0 0 0 1 1.51 1.65 1.65 0 0 0 1.82-.33l.06-.06a2 2 0 1 1 2.83 2.83l-.06.06a1.65 1.65 0 0 0-.33 1.82V9a1.65 1.65 0 0 0 1.51 1H21a2 2 0 0 1 0 4h-.09a1.65 1.65 0 0 0-1.51 1z"/></svg></a>
  </div>
</div>

<div class="modal-overlay" id="modal">
  <div class="modal-card">
    <div class="modal-header">
      <span class="modal-title">Open Project</span>
      <span class="modal-close" onclick="closeModal()">&times;</span>
    </div>
    <input class="modal-search" id="modal-search" placeholder="Search projects..." oninput="filterProjects()" />
    <div class="modal-list" id="modal-list"></div>
  </div>
</div>

<div class="modal-overlay" id="np-modal">
  <div class="modal-card np-card">
    <div class="modal-header">
      <span class="modal-title">New Project</span>
      <span class="modal-close" onclick="closeNewProject()">&times;</span>
    </div>
    <div class="np-body">
      <label class="np-label" for="np-input">Project name</label>
      <input class="np-input" id="np-input" placeholder="org/name  or  name" autocomplete="off" spellcheck="false"
             onkeydown="if(event.key==='Enter')submitNewProject();if(event.key==='Escape')closeNewProject()"
             oninput="updateNewProjectHint()" />
      <div class="np-hint" id="np-hint">Creates <span class="np-path">~/Developer/leo-chang/<span class="np-name">name</span></span></div>
    </div>
    <div class="np-actions">
      <button class="np-btn np-cancel" onclick="closeNewProject()">Cancel</button>
      <button class="np-btn np-create" onclick="submitNewProject()">Create</button>
    </div>
  </div>
</div>

<div class="modal-overlay" id="ns-modal">
  <div class="modal-card np-card">
    <div class="modal-header">
      <span class="modal-title">New Session</span>
      <span class="modal-close" onclick="closeNewSession()">&times;</span>
    </div>
    <div class="np-body">
      <label class="np-label" for="ns-name">Session name <span style="opacity:.6;text-transform:none;font-weight:400">(optional)</span></label>
      <input class="np-input" id="ns-name" placeholder="blank = auto" autocomplete="off" spellcheck="false"
             onkeydown="if(event.key==='Enter')submitNewSession();if(event.key==='Escape')closeNewSession()" />
      <label class="np-label" for="ns-branch" style="margin-top:14px">Git branch <span style="opacity:.6;text-transform:none;font-weight:400">(optional — checkout / create)</span></label>
      <input class="np-input" id="ns-branch" placeholder="blank = current branch" autocomplete="off" spellcheck="false"
             onkeydown="if(event.key==='Enter')submitNewSession();if(event.key==='Escape')closeNewSession()" />
      <div class="np-hint" id="ns-warn" style="display:none;color:var(--red)"></div>
    </div>
    <div class="np-actions">
      <button class="np-btn np-cancel" onclick="closeNewSession()">Cancel</button>
      <button class="np-btn np-create" onclick="submitNewSession()">Create</button>
    </div>
  </div>
</div>

<div class="modal-overlay" id="palette-modal">
  <div class="modal-card">
    <div class="modal-header">
      <span class="modal-title">Switch session</span>
      <span class="modal-close" onclick="closePalette()">&times;</span>
    </div>
    <input class="modal-search" id="palette-search" placeholder="Type to filter sessions…" autocomplete="off" spellcheck="false" />
    <div class="modal-list" id="palette-list"></div>
  </div>
</div>

<script>
var activeSession = null;
var lastSeenActivity = {};
var sessions = [];
var allProjects = [];
var ws = null;
var groupCfg = {categories: [], assign: {}, names: {}};  // categories:[names], assign:{cwd:category}, names:{convId:customName}
var convCache = {};  // cwd -> [past conversations] (lazy-loaded on project expand)
var term = null;
var fitAddon = null;
var wsConnected = false;

// --- Terminal ---

var darkTermTheme = {
  background: '#09090b', foreground: '#d4d4e0', cursor: '#e8a849', cursorAccent: '#09090b',
  selectionBackground: 'rgba(232,168,73,0.2)', selectionForeground: '#ffffff',
  black: '#1a1a2e', red: '#e05555', green: '#5ec269', yellow: '#e8a849',
  blue: '#6e8efb', magenta: '#c578dd', cyan: '#56b6c2', white: '#d4d4e0',
  brightBlack: '#555570', brightRed: '#ef7070', brightGreen: '#7dd88a', brightYellow: '#e8b96a',
  brightBlue: '#8ba4fc', brightMagenta: '#d494ea', brightCyan: '#6fcad4', brightWhite: '#eeeefc'
};
var lightTermTheme = {
  // Warm off-white bg with a strong-contrast dark foreground. "white"/"brightWhite"
  // map to DARK so programs that print white text expecting a dark terminal stay
  // readable on the light bg (they'd otherwise vanish).
  background: '#faf9f6', foreground: '#1f1d1b', cursor: '#b07d20', cursorAccent: '#faf9f6',
  selectionBackground: 'rgba(196,138,42,0.28)', selectionForeground: '#1f1d1b',
  black: '#3a3835', red: '#c0392b', green: '#2f8a3e', yellow: '#a9760f',
  blue: '#2d57c4', magenta: '#8a32a8', cyan: '#0f7d80', white: '#3a3835',
  brightBlack: '#6e6a64', brightRed: '#c5392b', brightGreen: '#2f8a3e', brightYellow: '#a9760f',
  brightBlue: '#2d57c4', brightMagenta: '#8a32a8', brightCyan: '#0f7d80', brightWhite: '#1f1d1b'
};

function initTerminal() {
  if (term) return;
  document.getElementById('font-size-display').textContent = termFontSize;
  term = new Terminal({
    fontSize: termFontSize,
    fontFamily: "'JetBrains Mono', Menlo, 'PingFang TC', 'PingFang SC', 'Hiragino Sans GB', 'Microsoft YaHei', monospace",
    fontWeight: 400,
    fontWeightBold: 600,
    letterSpacing: 0,
    lineHeight: 1.2,
    cursorBlink: true,
    cursorStyle: 'bar',
    scrollback: 50000,
    rightClickSelectsWord: true,
    theme: darkTermTheme
  });
  fitAddon = new FitAddon.FitAddon();
  term.loadAddon(fitAddon);
  try { term.loadAddon(new WebLinksAddon.WebLinksAddon()); } catch(e) {}

  if (document.body.classList.contains('light')) updateTerminalTheme(true);

  var container = document.getElementById('terminal-container');
  term.open(container);
  // Canvas renderer: grid-positions every cell so wide CJK glyphs don't drift /
  // crop at larger font sizes (the DOM renderer's inline layout does). Load it
  // AFTER the webfont is ready — otherwise it bakes a glyph atlas from the bold
  // fallback font and everything looks heavy. fontWeight 300 also offsets the
  // canvas renderer's tendency to render heavier (no -webkit-font-smoothing).
  function attachRenderer() {
    try { term.loadAddon(new CanvasAddon.CanvasAddon()); } catch(e) {}
    fitAddon.fit();
  }
  if (document.fonts && document.fonts.ready) {
    document.fonts.ready.then(attachRenderer);
  } else {
    attachRenderer();
  }
  fitAddon.fit();



  term.attachCustomKeyEventHandler(function(e) {
    // Cmd/Ctrl+K opens the session switcher — don't send it to the shell.
    if ((e.metaKey || e.ctrlKey) && (e.key === 'k' || e.key === 'K')) {
      return false;
    }
    return true;
  });

  term.onData(function(data) {
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(new TextEncoder().encode(data));
    }
  });

  term.onResize(function(size) {
    document.getElementById('term-size').textContent = size.cols + 'x' + size.rows;
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify({type: 'resize', cols: size.cols, rows: size.rows}));
    }
  });

  window.addEventListener('resize', function() { fitAddon.fit(); });
  new ResizeObserver(function() { fitAddon.fit(); }).observe(container);
}

var convLoading = {};  // cwd -> true while a fetch is in flight

// Lazily fetch the session history for a project (cwd), then re-render.
function loadConversations(cwd) {
  if (convCache[cwd] !== undefined || convLoading[cwd]) return;
  convLoading[cwd] = true;
  fetch('/api/conversations?cwd=' + encodeURIComponent(cwd))
    .then(function(r) { return r.json(); })
    .then(function(convs) {
      convCache[cwd] = convs || [];
      delete convLoading[cwd];
      renderSessions();
    })
    .catch(function() { delete convLoading[cwd]; convCache[cwd] = []; renderSessions(); });
}

// Resume a past Claude session: spawn a tmux session for it under the same repo,
// then attach. It then becomes a running session within that project.
function resumeConversation(cwd, convId) {
  var base = cwd.replace(/\/+$/, '').split('/').pop() || 'session';
  // Strip '.' too — a dotted tmux name (e.g. dir "next.js") breaks every later
  // `tmux -t <name>` command. Backend re-sanitizes, but keep the client honest.
  var name = (base + '-' + convId.slice(0, 6)).replace(/[^A-Za-z0-9_-]+/g, '-').replace(/-+/g, '-');
  fetch('/api/sessions/create', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({name: name, directory: cwd, resume_id: convId})
  }).then(function(r) { return r.json(); }).then(function(d) {
    if (d.ok) {
      setTimeout(function() { loadSessions(); connectSession(d.name); }, 400);
    } else {
      alert('Resume failed: ' + (d.error || 'unknown'));
    }
  });
}

function connectSession(name) {
  // Already attached to this session with a live socket — nothing to do.
  if (name === activeSession && ws && ws.readyState === WebSocket.OPEN) {
    if (term) term.focus();
    return;
  }
  if (ws) {
    ws.close();
    ws = null;
    wsConnected = false;
  }

  activeSession = name;
  localStorage.setItem('webmux-last-session', name);
  var cur = sessions.find(function(s) { return s.name === name; });
  if (cur) lastSeenActivity[name] = cur.activity || 0;
  historyLimit = 500;
  historyTotal = 0;
  location.hash = encodeURIComponent(name);
  renderSessions();

  document.getElementById('no-session').style.display = 'none';
  document.getElementById('terminal-container').style.display = 'block';
  document.getElementById('conn-session').textContent = name;
  document.getElementById('conn-status').textContent = 'connecting...';
  document.getElementById('conn-status').className = 'disconnected';

  initTerminal();
  term.clear();
  term.focus();
  // Fit FIRST so we know the real cols/rows, then pass them in the connect URL.
  // The backend sizes the PTY before forking tmux attach, so tmux paints at the
  // correct size on the first frame — no resize race, no blank/stale pane.
  fitAddon.fit();
  var cols = term.cols || 80, rows = term.rows || 24;

  var proto = location.protocol === 'https:' ? 'wss:' : 'ws:';
  ws = new WebSocket(proto + '//' + location.host + '/ws/terminal?session=' +
    encodeURIComponent(name) + '&cols=' + cols + '&rows=' + rows);
  ws.binaryType = 'arraybuffer';

  ws.onopen = function() {
    wsConnected = true;
    renderSessions();
    document.getElementById('conn-status').textContent = 'connected';
    document.getElementById('conn-status').className = 'connected';
    // Re-fit in case layout settled between connect and open, then send the
    // authoritative size once. tmux already attached at the URL size, so this
    // is a correction, not the primary sizing — and a SIGWINCH forces a redraw.
    fitAddon.fit();
    ws.send(JSON.stringify({type: 'resize', cols: term.cols, rows: term.rows}));
  };

  ws.onmessage = function(e) {
    if (e.data instanceof ArrayBuffer) {
      term.write(new Uint8Array(e.data));
    } else {
      term.write(e.data);
    }
  };

  ws.onclose = function() {
    wsConnected = false;
    renderSessions();
    document.getElementById('conn-status').textContent = 'disconnected';
    document.getElementById('conn-status').className = 'disconnected';
  };

  ws.onerror = function() {
    document.getElementById('conn-status').textContent = 'error';
    document.getElementById('conn-status').className = 'disconnected';
  };
}

// --- Sessions ---

function getSessionHistory() {
  try { return JSON.parse(localStorage.getItem('webmux-session-history') || '[]'); }
  catch(e) { return []; }
}

function saveSessionHistory(history) {
  localStorage.setItem('webmux-session-history', JSON.stringify(history));
}

function removeFromHistory(name) {
  var history = getSessionHistory().filter(function(h) { return h.name !== name; });
  saveSessionHistory(history);
  renderHistory(history);
}

function restoreFromHistory(name, cwd) {
  fetch('/api/sessions/create', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({name: name, directory: cwd})
  })
  .then(function(r) { return r.json(); })
  .then(function(data) {
    if (data.ok) {
      removeFromHistory(name);
      setTimeout(function() {
        loadSessions();
        setTimeout(function() { connectSession(data.name); }, 500);
      }, 1000);
    }
  });
}

function renderHistory(history) {
  var header = document.getElementById('history-header');
  var list = document.getElementById('history-list');
  list.innerHTML = '';
  if (!history.length) {
    header.style.display = 'none';
    return;
  }
  header.style.display = '';
  history.forEach(function(h) {
    var el = document.createElement('div');
    el.className = 'history-item';
    var sp = shortPath(h.cwd);
    el.innerHTML =
      '<span class="session-dot"></span>' +
      '<div class="session-info">' +
        '<div class="session-name">' + esc(h.name) + '</div>' +
        '<div class="session-path">' + esc(sp) + '</div>' +
      '</div>' +
      '<span class="history-remove">&times;</span>';
    el.querySelector('.session-info').onclick = function() { restoreFromHistory(h.name, h.cwd); };
    el.querySelector('.history-remove').onclick = function(e) {
      e.stopPropagation();
      removeFromHistory(h.name);
    };
    list.appendChild(el);
  });
}

function loadSessions() {
  fetch('/api/sessions')
    .then(function(r) { return r.json(); })
    .then(function(data) {
      sessions = data;
      // Inbox baseline: the first time we ever see a session, record its current
      // activity as "seen" — so it starts read, and only NEW output after that
      // marks it unread. (Without this, lastSeenActivity stays undefined and the
      // unread dot never fires.) The attached session is always kept read.
      data.forEach(function(s) {
        if (lastSeenActivity[s.name] === undefined) lastSeenActivity[s.name] = s.activity || 0;
      });
      if (activeSession) {
        var cur = data.find(function(s) { return s.name === activeSession; });
        if (cur) lastSeenActivity[activeSession] = cur.activity || 0;
      }
      var activeNames = data.map(function(s) { return s.name; });
      var raw = getSessionHistory();
      var history = raw.filter(function(h) {
        return activeNames.indexOf(h.name) === -1;
      });
      if (history.length !== raw.length) saveSessionHistory(history);
      renderSessions();
      renderHistory(history);
      if (!activeSession && sessions.length > 0) {
        var hash = decodeURIComponent(location.hash.slice(1));
        var last = localStorage.getItem('webmux-last-session') || '';
        var target = hash || last;
        var found = sessions.some(function(s) { return s.name === target; });
        if (found) connectSession(target);
      }
    });
}

// Sanitize a name the same way the backend clean_name() does (spaces + unsafe
// chars → '-', collapse, trim) so a conversation name and its tmux session name
// compare equal even though one may keep spaces.
function treeCleanName(s) {
  return String(s || '').trim().replace(/[^A-Za-z0-9_-]+/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '').toLowerCase();
}

// --- Project order (by cwd) persistence ---
function getProjectOrder() {
  try { return JSON.parse(localStorage.getItem('webmux-project-order') || '[]'); }
  catch(e) { return []; }
}
function saveProjectOrder(order) {
  localStorage.setItem('webmux-project-order', JSON.stringify(order));
}

// Label a project by the last path segment (e.g. /Users/x/Developer/leo-chang/webmux → webmux).
function projName(cwd) { return cwd.replace(/\/+$/, '').split('/').pop() || cwd; }

// Group live tmux sessions into projects keyed by cwd, ordered by saved order.
function buildProjects() {
  var byCwd = {};
  var list = [];
  sessions.forEach(function(s) {
    if (!byCwd[s.cwd]) { byCwd[s.cwd] = {cwd: s.cwd, name: projName(s.cwd), live: []}; list.push(byCwd[s.cwd]); }
    byCwd[s.cwd].live.push(s);
  });
  var order = getProjectOrder();
  list.sort(function(a, b) {
    var ai = order.indexOf(a.cwd), bi = order.indexOf(b.cwd);
    if (ai === -1 && bi === -1) return a.name.localeCompare(b.name);
    if (ai === -1) return 1;
    if (bi === -1) return -1;
    return ai - bi;
  });
  return list;
}

// Relative time + human size for session-history rows.
function timeAgo(sec) {
  var d = Math.floor(Date.now() / 1000) - sec;
  if (d < 60) return 'just now';
  if (d < 3600) return Math.floor(d / 60) + 'm ago';
  if (d < 86400) return Math.floor(d / 3600) + 'h ago';
  if (d < 604800) return Math.floor(d / 86400) + 'd ago';
  return Math.floor(d / 604800) + 'w ago';
}
function humanSize(b) {
  if (!b) return '';
  if (b < 1024) return b + 'B';
  if (b < 1048576) return (b / 1024).toFixed(0) + 'KB';
  return (b / 1048576).toFixed(1) + 'MB';
}
// Absolute local date+time for tooltips (vs timeAgo's relative form).
function fmtDate(sec) {
  if (!sec) return '';
  var d = new Date(sec * 1000);
  return d.toLocaleString([], {month: 'short', day: 'numeric',
    hour: '2-digit', minute: '2-digit'});
}

// --- Floating hover tooltip (escapes the fixed-width sidebar) ---
var wmTipEl = null, wmTipTimer = null;
function wmTip() {
  if (!wmTipEl) {
    wmTipEl = document.createElement('div');
    wmTipEl.id = 'wm-tip';
    document.body.appendChild(wmTipEl);
  }
  return wmTipEl;
}
// Attach hover-tooltip behavior to a row. nameHtml/metaHtml are pre-escaped.
function attachTip(el, nameHtml, metaHtml) {
  el.addEventListener('mouseenter', function() {
    var t = wmTip();
    t.innerHTML = '<div class="wm-tip-name">' + nameHtml + '</div>' +
      (metaHtml ? '<div class="wm-tip-meta">' + metaHtml + '</div>' : '');
    var r = el.getBoundingClientRect();
    // Anchor to the SIDEBAR's right edge (not the row's), so the tip always
    // clears in-row hover controls like the "+" new-session button.
    var sb = document.getElementById('sidebar');
    var sbRight = sb ? sb.getBoundingClientRect().right : r.right;
    var x = sbRight + 10, y = r.top;
    t.style.left = '0px'; t.style.top = '0px'; t.classList.add('show');
    var tw = t.offsetWidth, th = t.offsetHeight;
    if (x + tw > window.innerWidth - 8) x = Math.max(8, (sb ? sb.getBoundingClientRect().left : r.left) - tw - 10);
    if (y + th > window.innerHeight - 8) y = Math.max(8, window.innerHeight - th - 8);
    t.style.left = x + 'px'; t.style.top = y + 'px';
  });
  el.addEventListener('mouseleave', function() {
    if (wmTipEl) wmTipEl.classList.remove('show');
  });
}

// Map live tmux sessions in a project to conversation ids. TWO passes only —
// both EXACT. No mtime guessing: an unmatched session is shown as itself (its
// tmux name, via leftover), never fused onto some other conversation.
//   Pass 0: session.conv_id — the REAL conversation the pane's claude process
//     is on (backend reads it from `claude --resume <id>` on the cmdline). Ground
//     truth; immune to renames and mtime churn.
//   Pass 1: session named "<name>-<6hex>" → conversation whose id starts with
//     that hex (how webmux names sessions it resumes).
// Anything still unmatched — a fresh `claude -n` / `--continue` session that
// has no id on its cmdline and no matching conversation yet — goes to leftover
// and renders under its own tmux name. Previously a mtime "newest unclaimed"
// Pass 2 fused these onto an unrelated old conversation (showing the wrong name
// + size), which is the mismatch bug this removes.
function mapLiveToConv(live, convs) {
  var map = {}, claimed = {}, leftover = [], mapped = {};
  // Pass 0 — authoritative conv_id from the backend.
  live.forEach(function(s) {
    if (!s.conv_id) return;
    var hit = convs.find(function(c) { return c.id === s.conv_id && !claimed[c.id]; });
    if (hit) { map[hit.id] = s.name; claimed[hit.id] = true; mapped[s.name] = true; }
  });
  // Pass 1 — exact "-<6hex>" suffix match (only for sessions still unmapped).
  live.forEach(function(s) {
    if (mapped[s.name]) return;
    var m = s.name.match(/-([0-9a-f]{6})$/i);
    if (!m) return;
    var hit = convs.find(function(c) { return c.id.slice(0, 6) === m[1].toLowerCase() && !claimed[c.id]; });
    if (hit) { map[hit.id] = s.name; claimed[hit.id] = true; mapped[s.name] = true; }
  });
  // Unmatched → leftover (rendered as its own tmux name). No mtime guessing.
  live.forEach(function(s) {
    if (!mapped[s.name]) leftover.push(s);
  });
  return {byConv: map, leftover: leftover};
}

var dragSrcCwd = null;

// A session-history row (Layer 2). `liveName` is the tmux session name if this
// conversation is currently running (else null → resume on click).
function makeHistoryRow(cwd, c, liveName) {
  var el = document.createElement('div');
  var isAttached = liveName && liveName === activeSession;
  var isLive = !!liveName;
  // Unread = inbox model: this live session produced new output since you last
  // viewed it, you're not viewing it now, AND the agent has FINISHED (Claude's
  // own status is idle, not busy) — so the dot means "a completed answer is
  // waiting", not "still typing". Cleared when you open it.
  var isUnread = isLive && !isAttached && (function() {
    var s = sessions.find(function(x) { return x.name === liveName; });
    return s && !s.busy && s.activity && lastSeenActivity[liveName] !== undefined && s.activity > lastSeenActivity[liveName];
  })();
  el.className = 'session-item nested hist' + (isAttached ? ' active' : '') + (isLive ? ' live' : '') + (isUnread ? ' unread' : '') + (isLive ? '' : ' dead');
  // Title priority: Claude Code's own session name (set via /rename) → webmux
  // display name (for ended sessions Claude can't rename) → summary → the live
  // tmux session name (the name the user typed when creating it, before Claude
  // has a summary/rename) → hashcode. The tmux-name fallback is why a freshly
  // created session shows the name you typed instead of a random id.
  var customName = groupCfg.names && groupCfg.names[c.id];
  var title = c.claude_name || customName || c.summary || (isLive ? liveName : '') || c.id.slice(0, 8);
  // For a live session, the tmux pane's CURRENT branch beats the conversation's
  // historical branch (you may have checked out a different branch since).
  var branch = c.branch;
  if (isLive) {
    var ls = sessions.find(function(x) { return x.name === liveName; });
    if (ls && ls.branch) branch = ls.branch;
  }
  var meta = timeAgo(c.mtime) + (branch ? ' · ' + esc(branch) : '') + (c.size ? ' · ' + humanSize(c.size) : '');
  // Full-text tooltip meta: absolute date + branch + size (size = how much work).
  var tipMeta = [fmtDate(c.mtime), branch ? esc(branch) : '', c.size ? humanSize(c.size) : '']
    .filter(Boolean).join(' · ');
  el.innerHTML =
    '<div class="session-info">' +
      '<div class="session-name">' + esc(title) + '</div>' +
      '<div class="session-path">' + meta + '</div>' +
    '</div>' +
    '<span class="row-actions">' +
      '<span class="session-rename" title="Rename session">&#9998;</span>' +
      '<span class="session-kill" title="' + (isLive ? 'Kill session' : 'Delete conversation') + '">&times;</span>' +
    '</span>';
  var info = el.querySelector('.session-info');
  info.onclick = function() {
    if (liveName) connectSession(liveName);
    else resumeConversation(cwd, c.id);
  };
  attachTip(info, esc(title), tipMeta);
  var doRename = function(e) { e.stopPropagation(); renameSessionRow(cwd, c, liveName); };
  el.querySelector('.session-name').ondblclick = doRename;
  el.querySelector('.session-rename').onclick = doRename;
  if (isLive) {
    el.querySelector('.session-kill').onclick = function(e) { e.stopPropagation(); showConfirm(liveName, e.target); };
  } else {
    el.querySelector('.session-kill').onclick = function(e) { e.stopPropagation(); showDeleteConv(cwd, c, e.target); };
  }
  return el;
}

// Rename a session. LIVE → send Claude's official `/rename` (updates Claude Code
// itself + /resume picker). ENDED (no live tmux) → store a webmux display name,
// since Claude can't rename a session that isn't running.
function renameSessionRow(cwd, c, liveName) {
  var cur = c.claude_name || (groupCfg.names && groupCfg.names[c.id]) || c.summary || c.id.slice(0, 8);
  var name = prompt('Session name:', cur);
  if (name === null) return;
  name = name.trim();
  if (liveName) {
    if (!name) return;
    fetch('/api/sessions/rename-claude', {
      method: 'POST', headers: {'Content-Type': 'application/json'},
      body: JSON.stringify({tmux: liveName, name: name})
    }).then(function(r) { return r.json(); }).then(function(d) {
      if (!d.ok) { alert('Rename failed: ' + (d.error || 'unknown')); return; }
      // The tmux session was renamed too (kept in sync). If we were attached to
      // it under the old name, re-point our socket to the new tmux name.
      if (d.tmux && d.tmux !== liveName && activeSession === liveName) {
        activeSession = d.tmux;
        localStorage.setItem('webmux-last-session', d.tmux);
        location.hash = encodeURIComponent(d.tmux);
      }
      // Refresh immediately so the row reflects the new name right away, then
      // again after Claude finishes its async write (so the conversation's
      // claude_name catches up). The name-based PAST-row suppression prevents a
      // duplicate row in the gap between the two.
      delete convCache[cwd]; loadSessions();
      setTimeout(function() { delete convCache[cwd]; loadSessions(); }, 900);
    });
  } else {
    if (!groupCfg.names) groupCfg.names = {};
    if (name) groupCfg.names[c.id] = name;
    else delete groupCfg.names[c.id];
    saveGroups();
    renderSessions();
  }
}

function getCollapsed() {
  try { return JSON.parse(localStorage.getItem('webmux-collapsed') || '{}'); }
  catch(e) { return {}; }
}
function setCollapsed(map) { localStorage.setItem('webmux-collapsed', JSON.stringify(map)); }

function saveGroups() {
  fetch('/api/groups', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify(groupCfg)
  });
}

function addCategory() {
  var name = prompt('New category name:');
  if (!name) return;
  name = name.trim();
  if (!name || groupCfg.categories.indexOf(name) !== -1) return;
  groupCfg.categories.push(name);
  saveGroups();
  renderSessions();
}

// Rename a category in place: update the categories list, re-point every project
// assigned to it, and migrate its collapsed-state key. Keeps all members.
function renameCategory(oldName) {
  var newName = prompt('Rename category:', oldName);
  if (newName === null) return;
  newName = newName.trim();
  if (!newName || newName === oldName) return;
  if (groupCfg.categories.indexOf(newName) !== -1) {
    alert('A category named "' + newName + '" already exists.');
    return;
  }
  groupCfg.categories = groupCfg.categories.map(function(c) { return c === oldName ? newName : c; });
  Object.keys(groupCfg.assign).forEach(function(cwd) {
    if (groupCfg.assign[cwd] === oldName) groupCfg.assign[cwd] = newName;
  });
  // Migrate collapsed state so the renamed category keeps its expand/collapse.
  var collapsed = getCollapsed();
  if ('cat:' + oldName in collapsed) {
    collapsed['cat:' + newName] = collapsed['cat:' + oldName];
    delete collapsed['cat:' + oldName];
    setCollapsed(collapsed);
  }
  saveGroups();
  renderSessions();
}

// Assign a PROJECT (keyed by cwd) to a category (or null to un-assign).
function assignToCategory(cwd, category) {
  if (category) groupCfg.assign[cwd] = category;
  else delete groupCfg.assign[cwd];
  saveGroups();
  renderSessions();
}

// Render a list of projects (Layer 1) into container. Each project is a
// draggable, collapsible header; expanded body = its session history (Layer 2).
// `category` (or null for Uncategorized) is where dropped projects get assigned.
function renderProjectList(projects, container, category) {
  var collapsed = getCollapsed();
  projects.forEach(function(p) {
    var hasActive = p.live.some(function(s) { return s.name === activeSession; });
    // Project shows the unread dot when any of its live sessions has new output
    // since you last viewed it — so a COLLAPSED project still tells you there's
    // something to read inside (you'd never expand it otherwise).
    var anyUnread = p.live.some(function(s) {
      return s.name !== activeSession && !s.busy && s.activity && lastSeenActivity[s.name] !== undefined && s.activity > lastSeenActivity[s.name];
    });
    var isCollapsed = (p.cwd in collapsed) ? collapsed[p.cwd] : !hasActive;

    var header = document.createElement('div');
    header.className = 'repo-group' + (isCollapsed ? ' collapsed' : '') + (hasActive ? ' has-active' : '') + (anyUnread ? ' unread' : '');
    header.draggable = true;
    header.dataset.cwd = p.cwd;
    header.innerHTML =
      '<span class="repo-caret" title="Expand session history">' + (isCollapsed ? '&#9656;' : '&#9662;') + '</span>' +
      '<span class="repo-name" title="' + esc(p.cwd) + '">' + esc(p.name) + '</span>' +
      '<span class="row-actions repo-actions"><span class="repo-new" title="New session in this project">&#43;</span></span>';
    // Caret toggles the session-history list; the rest of the row auto-attaches.
    header.querySelector('.repo-caret').onclick = function(e) {
      e.stopPropagation();
      var c = getCollapsed();
      var nowCollapsed = !isCollapsed;
      c[p.cwd] = nowCollapsed;
      setCollapsed(c);
      // Re-render IMMEDIATELY so the caret flips and the row expands/collapses
      // on click — don't wait for the /api/conversations fetch. Expanding also
      // kicks off loadConversations(), which re-renders again once the history
      // arrives. (Previously expand only re-rendered inside the fetch callback,
      // so a cold expand felt laggy and a cached one didn't visibly respond.)
      if (!nowCollapsed) loadConversations(p.cwd);
      renderSessions();
    };
    // + → spawn a fresh claude session in this project's cwd (no resume).
    header.querySelector('.repo-new').onclick = function(e) {
      e.stopPropagation();
      newSessionInProject(p.cwd);
    };
    header.onclick = function() { openLatestSession(p); };
    attachTip(header.querySelector('.repo-name'), esc(p.name),
      esc(p.cwd) + (p.live.length ? ' · ' + p.live.length + ' live' : ''));
    // Drag project → reorder within / move between categories.
    header.addEventListener('dragstart', function(e) {
      dragSrcCwd = p.cwd;
      e.dataTransfer.setData('text/plain', p.cwd);
      e.dataTransfer.effectAllowed = 'move';
      header.classList.add('dragging');
    });
    header.addEventListener('dragend', function() { header.classList.remove('dragging'); dragSrcCwd = null; });
    header.addEventListener('dragover', function(e) { e.preventDefault(); header.classList.add('drag-over'); });
    header.addEventListener('dragleave', function() { header.classList.remove('drag-over'); });
    header.addEventListener('drop', function(e) {
      e.preventDefault(); e.stopPropagation();
      header.classList.remove('drag-over');
      var src = e.dataTransfer.getData('text/plain');
      if (src && src !== p.cwd) reorderProject(src, p.cwd, category);
    });
    container.appendChild(header);
    if (isCollapsed) return;

    // Layer 2: session history for this project.
    var convs = convCache[p.cwd];
    if (convs === undefined) {
      loadConversations(p.cwd);
      var loading = document.createElement('div');
      loading.className = 'conv-loading nested';
      loading.textContent = 'Loading…';
      container.appendChild(loading);
      return;
    }
    // LIVE rows = the project's live tmux sessions, shown faithfully as-is
    // (tmux session name, click → attach). No conversation matching, no guessing,
    // no de-duping: what tmux has is what you see. A session carries its resolved
    // conversation (conv_id) only to enrich the row with that convo's summary/
    // size/branch when available.
    var convById = {};
    convs.forEach(function(c) { convById[c.id] = c; });
    // Sanitized names of live sessions — used to suppress a duplicate PAST row
    // during the brief window after a rename where conv_id hasn't re-resolved
    // yet (tmux name and conversation name are kept in sync, so the sanitized
    // forms match even before conv_id catches up).
    var liveNameKeys = {};
    p.live.forEach(function(s) { liveNameKeys[treeCleanName(s.name)] = true; });
    var liveConvIds = {}, seenConv = {}, seenSess = {};
    p.live.forEach(function(s) {
      if (seenSess[s.name]) return;          // one row per tmux session, always
      seenSess[s.name] = true;
      var c = s.conv_id && convById[s.conv_id];
      // Enforce 1:1 — a conversation shows ONE row even if two tmux sessions
      // happen to be on it (attach the first/most-relevant; the dup is hidden).
      if (c) {
        if (seenConv[c.id]) return;
        seenConv[c.id] = true;
        liveConvIds[c.id] = true;
      }
      // Row is the SESSION. If we know its conversation, borrow that convo's
      // metadata; otherwise show the session's own name/activity.
      var row = c ? {id: c.id, claude_name: c.claude_name, summary: c.summary,
                     branch: c.branch, size: c.size, mtime: c.mtime}
                  : {id: s.name, summary: s.name, mtime: s.activity || 0,
                     branch: s.branch || '', size: 0};
      container.appendChild(makeHistoryRow(p.cwd, row, s.name));
    });
    // PAST rows = conversations with NO live session on them → click to resume.
    // Exclude by conv_id AND by sanitized-name (covers the post-rename window
    // before conv_id re-resolves, so the just-renamed session never doubles).
    convs.forEach(function(c) {
      if (liveConvIds[c.id]) return;
      var nm = c.claude_name || (groupCfg.names && groupCfg.names[c.id]) || c.summary || '';
      if (nm && liveNameKeys[treeCleanName(nm)]) return;
      container.appendChild(makeHistoryRow(p.cwd, c, null));
    });
  });
}

// Click a project → attach to its latest session. If it has live tmux sessions,
// attach to the most-recently-active one. Otherwise resume the newest conversation.
function openLatestSession(p) {
  if (p.live && p.live.length) {
    var latest = p.live.slice().sort(function(a, b) {
      return (b.activity || 0) - (a.activity || 0);
    })[0];
    connectSession(latest.name);
    return;
  }
  // No live session: resume the newest conversation (needs history loaded).
  var convs = convCache[p.cwd];
  if (convs === undefined) {
    fetch('/api/conversations?cwd=' + encodeURIComponent(p.cwd))
      .then(function(r) { return r.json(); })
      .then(function(cs) {
        convCache[p.cwd] = cs || [];
        if (cs && cs.length) resumeConversation(p.cwd, cs[0].id);
      });
    return;
  }
  if (convs.length) resumeConversation(p.cwd, convs[0].id);
}

// Move a project before `targetCwd` (or to end if null) and assign it to `category`.
function reorderProject(srcCwd, targetCwd, category) {
  // Update category assignment (keyed by cwd).
  if (category) groupCfg.assign[srcCwd] = category;
  else delete groupCfg.assign[srcCwd];
  saveGroups();
  // Update order: place src right before target.
  var order = getProjectOrder();
  // Seed order from current full project list if empty.
  if (order.length === 0) order = buildProjects().map(function(p) { return p.cwd; });
  order = order.filter(function(c) { return c !== srcCwd; });
  if (targetCwd) {
    var ti = order.indexOf(targetCwd);
    if (ti === -1) order.push(srcCwd); else order.splice(ti, 0, srcCwd);
  } else {
    order.push(srcCwd);
  }
  saveProjectOrder(order);
  renderSessions();
}

function makeCategoryHeader(name) {
  var collapsed = getCollapsed();
  var key = 'cat:' + name;
  var isCollapsed = collapsed[key];
  var h = document.createElement('div');
  h.className = 'category-header' + (isCollapsed ? ' collapsed' : '');
  h.innerHTML =
    '<span class="repo-caret">' + (isCollapsed ? '&#9656;' : '&#9662;') + '</span>' +
    '<span class="cat-name" title="Double-click to rename">' + esc(name) + '</span>' +
    '<span class="cat-rename" title="Rename category">&#9998;</span>' +
    '<span class="cat-del" title="Delete category">&times;</span>';
  h.onclick = function(e) {
    if (e.target.classList.contains('cat-del') || e.target.classList.contains('cat-rename')) return;
    var c = getCollapsed();
    if (c[key]) delete c[key]; else c[key] = true;
    setCollapsed(c);
    renderSessions();
  };
  h.querySelector('.cat-name').ondblclick = function(e) { e.stopPropagation(); renameCategory(name); };
  h.querySelector('.cat-rename').onclick = function(e) { e.stopPropagation(); renameCategory(name); };
  h.querySelector('.cat-del').onclick = function(e) {
    e.stopPropagation();
    // Remove category; its sessions fall back to Uncategorized.
    groupCfg.categories = groupCfg.categories.filter(function(c) { return c !== name; });
    Object.keys(groupCfg.assign).forEach(function(sn) {
      if (groupCfg.assign[sn] === name) delete groupCfg.assign[sn];
    });
    saveGroups();
    renderSessions();
  };
  // Drag-to-assign
  h.addEventListener('dragover', function(e) { e.preventDefault(); h.classList.add('drag-over'); });
  h.addEventListener('dragleave', function() { h.classList.remove('drag-over'); });
  h.addEventListener('drop', function(e) {
    e.preventDefault();
    h.classList.remove('drag-over');
    var sn = e.dataTransfer.getData('text/plain');
    if (sn) assignToCategory(sn, name);
  });
  return h;
}

function renderSessions() {
  var list = document.getElementById('session-list');
  list.innerHTML = '';
  if (wmTipEl) wmTipEl.classList.remove('show');  // drop stale tip on re-render
  var collapsed = getCollapsed();
  var projects = buildProjects();
  var hasCategories = groupCfg.categories.length > 0;

  if (!hasCategories) {
    renderProjectList(projects, list, null);
    return;
  }

  // Bucket projects by assigned category (keyed by cwd); rest → Uncategorized.
  var buckets = {};
  groupCfg.categories.forEach(function(c) { buckets[c] = []; });
  var uncategorized = [];
  projects.forEach(function(p) {
    var cat = groupCfg.assign[p.cwd];
    if (cat && buckets[cat]) buckets[cat].push(p);
    else uncategorized.push(p);
  });

  groupCfg.categories.forEach(function(cat) {
    list.appendChild(makeCategoryHeader(cat));
    if (!collapsed['cat:' + cat]) {
      var wrap = document.createElement('div');
      wrap.className = 'category-body';
      renderProjectList(buckets[cat], wrap, cat);
      list.appendChild(wrap);
    }
  });

  // Uncategorized bucket (drop target to un-assign) — always a drop target,
  // header shown only when it has projects.
  if (uncategorized.length > 0) {
    var uh = document.createElement('div');
    uh.className = 'category-header uncat';
    uh.innerHTML = '<span class="repo-caret">&#9662;</span><span class="cat-name">Uncategorized</span>';
    uh.addEventListener('dragover', function(e) { e.preventDefault(); uh.classList.add('drag-over'); });
    uh.addEventListener('dragleave', function() { uh.classList.remove('drag-over'); });
    uh.addEventListener('drop', function(e) {
      e.preventDefault();
      uh.classList.remove('drag-over');
      var cwd = e.dataTransfer.getData('text/plain');
      if (cwd) assignToCategory(cwd, null);
    });
    list.appendChild(uh);
    renderProjectList(uncategorized, list, null);
  }
}

function esc(s) { return s.replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;'); }
function shortPath(p) { return p.replace(/^\/Users\/[^/]+\/Developer\//, ''); }

// --- Kill Confirm ---

function showConfirm(name, anchor) {
  removeConfirm();
  var popup = document.createElement('div');
  popup.className = 'confirm-popup';
  popup.id = 'confirm-popup';
  var rect = anchor.getBoundingClientRect();
  popup.style.top = rect.bottom + 6 + 'px';
  popup.style.left = Math.min(rect.left, window.innerWidth - 240) + 'px';
  popup.innerHTML = 'Kill <strong>' + esc(name) + '</strong>?' +
    '<button class="btn-kill" onclick="doKill(\'' + esc(name) + '\')">Kill</button>' +
    '<button class="btn-cancel" onclick="removeConfirm()">Cancel</button>';
  document.body.appendChild(popup);
}

function removeConfirm() {
  var el = document.getElementById('confirm-popup');
  if (el) el.remove();
}

// Confirm + delete a non-live conversation's .jsonl (no attach needed).
function showDeleteConv(cwd, c, anchor) {
  removeConfirm();
  var label = c.summary || c.id.slice(0, 8);
  var popup = document.createElement('div');
  popup.className = 'confirm-popup';
  popup.id = 'confirm-popup';
  var rect = anchor.getBoundingClientRect();
  popup.style.top = rect.bottom + 6 + 'px';
  popup.style.left = Math.min(rect.left, window.innerWidth - 240) + 'px';
  popup.innerHTML = 'Delete <strong>' + esc(label) + '</strong>?' +
    '<button class="btn-kill" id="del-conv-yes">Delete</button>' +
    '<button class="btn-cancel" onclick="removeConfirm()">Cancel</button>';
  document.body.appendChild(popup);
  document.getElementById('del-conv-yes').onclick = function() { doDeleteConv(cwd, c.id); };
}

function doDeleteConv(cwd, convId) {
  removeConfirm();
  fetch('/api/conversations', {
    method: 'DELETE',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({cwd: cwd, id: convId})
  }).then(function(r) { return r.json(); }).then(function(d) {
    if (d.ok) {
      if (convCache[cwd]) {
        convCache[cwd] = convCache[cwd].filter(function(x) { return x.id !== convId; });
      }
      renderSessions();
    } else {
      alert('Delete failed: ' + (d.error || 'unknown'));
    }
  });
}

function restartAll() {
  if (!confirm('Restart ALL Claude sessions (/exit + relaunch)?')) return;
  var btn = document.getElementById('restart-all-btn');
  btn.classList.add('busy');
  btn.innerHTML = '&#8987;';
  fetch('/api/sessions/restart-all', {method: 'POST'})
    .then(function(r) { return r.json(); })
    .finally(function() {
      btn.classList.remove('busy');
      btn.innerHTML = '&#x21bb;';
    });
}

function doKill(name) {
  removeConfirm();
  var s = sessions.find(function(s) { return s.name === name; });
  if (s) {
    var history = getSessionHistory();
    history.push({name: s.name, cwd: s.cwd});
    saveSessionHistory(history);
  }
  fetch('/api/sessions/' + encodeURIComponent(name), {method: 'DELETE'})
    .then(function(r) { return r.json(); })
    .then(function() {
      if (activeSession === name) {
        if (ws) ws.close();
        activeSession = null;
        document.getElementById('no-session').style.display = '';
        document.getElementById('terminal-container').style.display = 'none';
      }
      loadSessions();
    });
}

// --- New Session Modal ---

function openModal() {
  document.getElementById('modal').classList.add('open');
  var search = document.getElementById('modal-search');
  search.value = '';
  search.focus();
  fetch('/api/projects').then(function(r) { return r.json(); }).then(function(data) {
    allProjects = data;
    filterProjects();
  });
}

function closeModal() {
  document.getElementById('modal').classList.remove('open');
}

function filterProjects() {
  var q = document.getElementById('modal-search').value.toLowerCase();
  var list = document.getElementById('modal-list');
  list.innerHTML = '';
  allProjects.forEach(function(p) {
    if (q && (p.org + '/' + p.name).toLowerCase().indexOf(q) === -1) return;
    var el = document.createElement('div');
    el.className = 'project-item';
    el.innerHTML = '<div><div class="project-name">' + esc(p.name) + '</div>' +
      '<div class="project-org">' + esc(p.org) + '</div></div>';
    el.onclick = function() { doCreate(p.name, p.path); };
    list.appendChild(el);
  });
}

function doCreate(name, path) {
  closeModal();
  fetch('/api/sessions/create', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({name: name, directory: path})
  })
  .then(function(r) { return r.json(); })
  .then(function(data) {
    if (data.ok) {
      setTimeout(function() {
        loadSessions();
        setTimeout(function() { connectSession(data.name); }, 500);
      }, 1000);
    }
  });
}

// Spawn a fresh claude session in an existing project's cwd. Prompts for an
// optional session name (blank = auto). The name is given to `claude -n` so it
// also shows in Claude's own /resume picker.
var nsCwd = null;  // cwd of the project the New Session modal is for

function newSessionInProject(cwd) {
  nsCwd = cwd;
  document.getElementById('ns-name').value = '';
  document.getElementById('ns-branch').value = '';
  // Warn if other sessions are already live here — a branch checkout in the
  // shared working dir would switch THEM too (plain git, one branch per dir).
  var liveHere = sessions.filter(function(s) { return s.cwd === cwd; });
  var warn = document.getElementById('ns-warn');
  if (liveHere.length > 0) {
    warn.style.display = '';
    warn.textContent = '⚠ ' + liveHere.length + ' session' + (liveHere.length > 1 ? 's' : '') +
      ' already live here. Setting a branch will switch ALL of them (shared working dir).';
  } else {
    warn.style.display = 'none';
  }
  document.getElementById('ns-modal').classList.add('open');
  setTimeout(function() { document.getElementById('ns-name').focus(); }, 40);
}

function closeNewSession() {
  document.getElementById('ns-modal').classList.remove('open');
  nsCwd = null;
}

function submitNewSession() {
  if (!nsCwd) return;
  var cwd = nsCwd;
  var wanted = document.getElementById('ns-name').value.trim();
  var branch = document.getElementById('ns-branch').value.trim();
  closeNewSession();
  var base = projName(cwd).replace(/[^A-Za-z0-9_.-]/g, '-') || 'session';
  var taken = {};
  sessions.forEach(function(s) { taken[s.name] = true; });
  var nameBase = (wanted || base).replace(/[^A-Za-z0-9_.-]/g, '-') || base;
  var name = nameBase, n = 2;
  while (taken[name]) { name = nameBase + '-' + n; n++; }
  fetch('/api/sessions/create', {
    method: 'POST', headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({name: name, directory: cwd, fresh: true,
      claude_name: wanted || null, branch: branch || null})
  }).then(function(r) { return r.json(); }).then(function(data) {
    if (data.ok) {
      setTimeout(function() {
        loadSessions();
        setTimeout(function() { connectSession(data.name); }, 500);
      }, 1000);
    } else {
      alert('New session failed: ' + (data.error || 'unknown'));
    }
  });
}

// Create a brand-new project directory under Developer, then open a session in it.
function newProject() {
  var input = document.getElementById('np-input');
  input.value = '';
  updateNewProjectHint();
  document.getElementById('np-modal').classList.add('open');
  setTimeout(function() { input.focus(); }, 50);
}

function closeNewProject() {
  document.getElementById('np-modal').classList.remove('open');
}

// Live preview of the path the name will resolve to.
function updateNewProjectHint() {
  var raw = document.getElementById('np-input').value.trim().replace(/^\/+|\/+$/g, '');
  var parts = raw.split('/').filter(Boolean);
  var org = 'leo-chang', name = 'name';
  if (parts.length >= 2) { org = parts[0]; name = parts.slice(1).join('/'); }
  else if (parts.length === 1) { name = parts[0]; }
  document.getElementById('np-hint').innerHTML =
    'Creates <span class="np-path">~/Developer/' + esc(org) + '/<span class="np-name">' + esc(name) + '</span></span>';
}

function submitNewProject() {
  var name = document.getElementById('np-input').value.trim();
  if (!name) return;
  closeNewProject();
  fetch('/api/projects/create', {
    method: 'POST',
    headers: {'Content-Type': 'application/json'},
    body: JSON.stringify({name: name})
  })
  .then(function(r) { return r.json(); })
  .then(function(data) {
    if (data.ok) {
      doCreate(data.session, data.path);
    } else {
      alert('Create project failed: ' + (data.error || 'unknown'));
    }
  });
}

// --- Sidebar Toggle ---

if (window.matchMedia('(pointer: coarse)').matches) {
  var _touchY = null;
  var _inTerm = false;
  document.addEventListener('touchstart', function(e) {
    var el = e.target;
    _inTerm = !!(el.closest('#terminal-container') || el.closest('.xterm'));
    if (_inTerm) _touchY = e.touches[0].clientY;
  }, {passive: true});
  document.addEventListener('touchmove', function(e) {
    if (!term || !_inTerm || _touchY === null) return;
    var dy = _touchY - e.touches[0].clientY;
    _touchY = e.touches[0].clientY;
    if (Math.abs(dy) > 3) {
      term.scrollLines(dy > 0 ? 2 : -2);
    }
  }, {passive: true});
  document.addEventListener('touchend', function() { _touchY = null; _inTerm = false; }, {passive: true});
}

function sendKey(seq) {
  if (ws && ws.readyState === WebSocket.OPEN) {
    ws.send(new TextEncoder().encode(seq));
  }
  if (term) term.focus();
}

function toggleSidebar() {
  document.getElementById('sidebar').classList.toggle('collapsed');
  setTimeout(function() { if (fitAddon) fitAddon.fit(); }, 300);
}

// --- Font Size ---
var termFontSize = parseInt(localStorage.getItem('webmux-font-size') || '18', 10);

function changeFontSize(delta) {
  termFontSize = Math.max(10, Math.min(36, termFontSize + delta));
  localStorage.setItem('webmux-font-size', termFontSize);
  document.getElementById('font-size-display').textContent = termFontSize;
  if (term) {
    term.options.fontSize = termFontSize;
    if (fitAddon) fitAddon.fit();
  }
}

// Sidebar text size — independent of the terminal font. Scales every tree
// level together via the --tree-font-scale CSS var (0.8×–1.4×), persisted.
var sidebarFontScale = parseFloat(localStorage.getItem('webmux-sidebar-scale') || '1');
function applySidebarFont() {
  document.documentElement.style.setProperty('--tree-font-scale', sidebarFontScale);
}
function changeSidebarFont(dir) {
  sidebarFontScale = Math.max(0.8, Math.min(1.4, Math.round((sidebarFontScale + dir * 0.1) * 10) / 10));
  localStorage.setItem('webmux-sidebar-scale', sidebarFontScale);
  applySidebarFont();
}
applySidebarFont();


function updateTerminalTheme(isLight) {
  if (!term) return;
  term.options.theme = isLight ? lightTermTheme : darkTermTheme;
  // In light mode, force a minimum contrast ratio so ANY foreground a program
  // emits (incl. 256-color/truecolor light greys that bypass the palette) is
  // auto-darkened enough to stay readable on the off-white bg. Dark mode keeps 1
  // (no adjustment) so the themed colors render exactly as designed.
  term.options.minimumContrastRatio = isLight ? 4.5 : 1;
}

function updateThemeIcon() {
  var btn = document.getElementById('theme-toggle');
  if (!btn) return;
  var isLight = document.body.classList.contains('light');
  btn.innerHTML = isLight
    ? '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><circle cx="12" cy="12" r="5"></circle><line x1="12" y1="1" x2="12" y2="3"></line><line x1="12" y1="21" x2="12" y2="23"></line><line x1="4.22" y1="4.22" x2="5.64" y2="5.64"></line><line x1="18.36" y1="18.36" x2="19.78" y2="19.78"></line><line x1="1" y1="12" x2="3" y2="12"></line><line x1="21" y1="12" x2="23" y2="12"></line><line x1="4.22" y1="19.78" x2="5.64" y2="18.36"></line><line x1="18.36" y1="5.64" x2="19.78" y2="4.22"></line></svg>'
    : '<svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2" stroke-linecap="round" stroke-linejoin="round"><path d="M21 12.79A9 9 0 1 1 11.21 3 7 7 0 0 0 21 12.79z"></path></svg>';
  btn.title = isLight ? 'Switch to dark theme' : 'Switch to light theme';
}

function toggleTheme() {
  var isLight = document.body.classList.toggle('light');
  localStorage.setItem('webmux-theme', isLight ? 'light' : 'dark');
  updateThemeIcon();
  updateTerminalTheme(isLight);
}

if (document.documentElement.classList.contains('light')) document.body.classList.add('light');
updateThemeIcon();

document.addEventListener('keydown', function(e) {
  // Cmd/Ctrl+K → quick session switcher (works even while focused in the terminal).
  if ((e.metaKey || e.ctrlKey) && (e.key === 'k' || e.key === 'K')) {
    e.preventDefault();
    togglePalette();
    return;
  }
  if (e.key === 'Escape') { closeModal(); closeNewProject(); closeNewSession(); closePalette(); removeConfirm(); }
});

// Click outside modal
document.getElementById('modal').addEventListener('click', function(e) {
  if (e.target === this) closeModal();
});

// --- Cmd+K session switcher ---
var paletteIdx = 0;     // highlighted row
var paletteItems = [];  // flat list of {name, label, sub, live, attached}

// Build the searchable flat list: every Claude session (live + history) across
// all projects, live ones first, labeled by their display name.
function paletteSource() {
  var out = [];
  var seenConv = {};
  buildProjects().forEach(function(p) {
    var convs = convCache[p.cwd];
    var liveMap = convs ? mapLiveToConv(p.live, convs) : {byConv: {}, leftover: p.live.map(function(s){return s;})};
    // Live sessions (mapped to a conv or leftover) — these are attach-able now.
    p.live.forEach(function(s) {
      // Prefer the Claude session name (from the matched conversation) over the
      // raw tmux name when we have history loaded.
      var cn = null;
      if (convs) {
        var cid = Object.keys(liveMap.byConv).find(function(k){ return liveMap.byConv[k] === s.name; });
        if (cid) { var cc = convs.find(function(x){ return x.id === cid; }); if (cc) cn = cc.claude_name || cc.summary; }
      }
      out.push({ name: s.name, kind: 'live',
        label: (cn || s.name),
        sub: p.name + (s.branch ? '  ·  ' + s.branch : ''),
        attached: s.name === activeSession });
    });
    // Past conversations — resume-able.
    if (convs) {
      convs.forEach(function(c) {
        if (liveMap.byConv[c.id]) return; // already shown as live
        out.push({ name: null, kind: 'past', cwd: p.cwd, id: c.id,
          label: (c.claude_name || (groupCfg.names && groupCfg.names[c.id]) || c.summary || c.id.slice(0,8)),
          sub: p.name + '  ·  ' + timeAgo(c.mtime) });
      });
    }
  });
  return out;
}

function togglePalette() {
  var m = document.getElementById('palette-modal');
  if (m.classList.contains('open')) { closePalette(); return; }
  // Make sure every project's history is loaded so past sessions are searchable.
  buildProjects().forEach(function(p) { if (convCache[p.cwd] === undefined) loadConversations(p.cwd); });
  m.classList.add('open');
  var inp = document.getElementById('palette-search');
  inp.value = '';
  paletteIdx = 0;
  renderPalette('');
  setTimeout(function() { inp.focus(); }, 30);
}

function closePalette() {
  document.getElementById('palette-modal').classList.remove('open');
  if (term) term.focus();
}

function fuzzy(hay, needle) {
  // Simple subsequence match; returns true if all needle chars appear in order.
  if (!needle) return true;
  hay = hay.toLowerCase(); needle = needle.toLowerCase();
  var i = 0;
  for (var j = 0; j < hay.length && i < needle.length; j++) {
    if (hay[j] === needle[i]) i++;
  }
  return i === needle.length;
}

function renderPalette(q) {
  var list = document.getElementById('palette-list');
  var all = paletteSource();
  paletteItems = all.filter(function(it) { return fuzzy(it.label + ' ' + it.sub, q); });
  // Live/attached first, then past — preserve source order within groups.
  paletteItems.sort(function(a, b) {
    var ar = (a.attached ? 0 : a.kind === 'live' ? 1 : 2);
    var br = (b.attached ? 0 : b.kind === 'live' ? 1 : 2);
    return ar - br;
  });
  if (paletteIdx >= paletteItems.length) paletteIdx = Math.max(0, paletteItems.length - 1);
  list.innerHTML = '';
  if (!paletteItems.length) {
    list.innerHTML = '<div class="conv-empty" style="padding:12px">No matching sessions</div>';
    return;
  }
  paletteItems.forEach(function(it, i) {
    var row = document.createElement('div');
    row.className = 'palette-item' + (i === paletteIdx ? ' sel' : '');
    var dot = it.attached ? 'session-dot active' : (it.kind === 'live' ? 'session-dot active' : 'session-dot hist-dot');
    row.innerHTML =
      '<span class="' + dot + '"></span>' +
      '<div class="session-info">' +
        '<div class="session-name">' + esc(it.label) + (it.attached ? ' <span class="palette-tag">attached</span>' : '') + '</div>' +
        '<div class="session-path">' + esc(it.sub) + '</div>' +
      '</div>';
    row.onclick = function() { paletteIdx = i; activatePalette(); };
    list.appendChild(row);
  });
}

function activatePalette() {
  var it = paletteItems[paletteIdx];
  if (!it) return;
  closePalette();
  if (it.kind === 'live') connectSession(it.name);
  else resumeConversation(it.cwd, it.id);
}

document.getElementById('palette-search').addEventListener('input', function(e) {
  paletteIdx = 0;
  renderPalette(e.target.value.trim());
});
document.getElementById('palette-search').addEventListener('keydown', function(e) {
  if (e.key === 'ArrowDown') { e.preventDefault(); paletteIdx = Math.min(paletteItems.length - 1, paletteIdx + 1); renderPalette(this.value.trim()); scrollPaletteSel(); }
  else if (e.key === 'ArrowUp') { e.preventDefault(); paletteIdx = Math.max(0, paletteIdx - 1); renderPalette(this.value.trim()); scrollPaletteSel(); }
  else if (e.key === 'Enter') { e.preventDefault(); activatePalette(); }
});
function scrollPaletteSel() {
  var sel = document.querySelector('#palette-list .palette-item.sel');
  if (sel) sel.scrollIntoView({block: 'nearest'});
}
document.getElementById('palette-modal').addEventListener('click', function(e) {
  if (e.target === this) closePalette();
});
document.getElementById('ns-modal').addEventListener('click', function(e) {
  if (e.target === this) closeNewSession();
});

// --- File Upload ---

function uploadFiles(files) {
  if (!files.length || !activeSession) return;
  var form = new FormData();
  form.append('session', activeSession);
  for (var i = 0; i < files.length; i++) {
    form.append('files', files[i]);
  }
  fetch('/api/upload', { method: 'POST', body: form })
    .then(function(r) {
      if (!r.ok) { return r.text().then(function(t) { throw new Error('HTTP ' + r.status + ' ' + t.slice(0,120)); }); }
      return r.json();
    })
    .then(function(data) {
      if (term) {
        if (data.ok) {
          term.write('\r\n\x1b[33m[webmux] Attached: ' + data.paths.join(', ') + '\x1b[0m\r\n');
        } else {
          term.write('\r\n\x1b[31m[webmux] Upload failed: ' + (data.error || 'unknown') + '\x1b[0m\r\n');
        }
      }
    })
    .catch(function(e) {
      if (term) term.write('\r\n\x1b[31m[webmux] Upload failed: ' + (e.message || e) + '\x1b[0m\r\n');
    });
}

document.getElementById('file-input').addEventListener('change', function(e) {
  uploadFiles(e.target.files);
  e.target.value = '';
});

// Drag and drop on main area
(function() {
  var mainArea = document.getElementById('main-area');
  var overlay = document.getElementById('drop-overlay');
  var dragCount = 0;
  mainArea.addEventListener('dragenter', function(e) {
    e.preventDefault();
    dragCount++;
    overlay.classList.add('visible');
  });
  mainArea.addEventListener('dragleave', function(e) {
    dragCount--;
    if (dragCount <= 0) { overlay.classList.remove('visible'); dragCount = 0; }
  });
  mainArea.addEventListener('dragover', function(e) { e.preventDefault(); });
  mainArea.addEventListener('drop', function(e) {
    e.preventDefault();
    overlay.classList.remove('visible');
    dragCount = 0;
    if (e.dataTransfer.files.length) uploadFiles(e.dataTransfer.files);
  });
})();

// --- Init ---

fetch('/api/groups').then(function(r) { return r.json(); }).then(function(g) {
  if (g && g.categories) groupCfg = g;
  if (!groupCfg.names) groupCfg.names = {};
  loadSessions();
}).catch(function() { loadSessions(); });
setInterval(loadSessions, 3000);

if (window.visualViewport && window.matchMedia('(pointer: coarse)').matches) {
  window.visualViewport.addEventListener('resize', function() {
    var vh = window.visualViewport.height;
    document.getElementById('main-area').style.height = vh + 'px';
    if (fitAddon) fitAddon.fit();
  });
}
</script>
</body>
</html>"""


# --- App Setup ---

def _build_app():
    # client_max_size defaults to 2MB in aiohttp — anything bigger (e.g. a
    # screenshot) is rejected with 413 BEFORE the upload handler runs, which the
    # frontend saw as "Upload failed". Raise it to 200MB for file attachments.
    app = web.Application(client_max_size=200 * 1024 * 1024)
    app.router.add_get("/ws/terminal", terminal_ws)
    app.router.add_get("/api/sessions", api_sessions)
    app.router.add_get("/api/projects", api_projects)
    app.router.add_get("/api/conversations", api_conversations)
    app.router.add_delete("/api/conversations", api_delete_conversation)
    app.router.add_get("/api/messages", api_messages)
    app.router.add_post("/api/sessions/create", api_create_session)
    app.router.add_post("/api/projects/create", api_create_project)
    app.router.add_post("/api/sessions/rename", api_rename_session)
    app.router.add_post("/api/sessions/rename-claude", api_rename_claude)
    app.router.add_delete("/api/sessions/{name}", api_kill_session)
    app.router.add_post("/api/upload", api_upload)
    app.router.add_post("/api/sessions/restart-all", api_restart_all)
    app.router.add_get("/login", handle_login)
    app.router.add_get("/api/browse", api_browse)
    app.router.add_get("/api/groups", api_groups)
    app.router.add_post("/api/groups", api_groups)
    app.router.add_get("/settings", handle_settings)
    app.router.add_post("/settings", handle_settings)
    app.router.add_post("/login", handle_login)
    app.router.add_get("/{path:.*}", serve_html)
    return app


def main():
    # Multiple webmux tabs/devices can attach the same session — let the
    # most recent client drive the window size instead of shrinking to the
    # smallest. Makes attach behave consistently across clients.
    try:
        subprocess.run(["tmux", "set-option", "-g", "window-size", "latest"],
                       capture_output=True, timeout=5)
    except (OSError, subprocess.TimeoutExpired):
        pass
    app = _build_app()
    print(f"webmux — http://localhost:{PORT}")
    if REMOTE:
        ssl_ctx = _get_ssl_context()
        print(f"webmux — https://0.0.0.0:{REMOTE_PORT} (remote)")
        print(f"User: {AUTH_USER}  Pass: {AUTH_PASS}")

        async def _start():
            runner = web.AppRunner(app)
            await runner.setup()
            await web.TCPSite(runner, "127.0.0.1", PORT).start()
            await web.TCPSite(runner, "0.0.0.0", REMOTE_PORT, ssl_context=ssl_ctx).start()
            await asyncio.Event().wait()
        asyncio.run(_start())
    else:
        web.run_app(app, host="127.0.0.1", port=PORT, print=None)



if __name__ == "__main__":
    main()
