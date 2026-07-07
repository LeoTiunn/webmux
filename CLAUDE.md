# webmux

Browser-based tmux terminal client. Single Python file (`webmux.py`) with embedded HTML/JS/CSS.

## Tech Stack

- **Backend:** Python 3.9+, aiohttp (async HTTP + WebSocket), `pty.fork()` for real terminal I/O
- **Frontend:** xterm.js terminal emulator, vanilla JS, dark theme
- **Transport:** WebSocket per session, binary frames for PTY data

## Architecture

`webmux.py` is the entire app — server, HTML, JS, CSS all in one file. No build step, no npm.

- aiohttp request handler (GET/POST/DELETE/WebSocket)
- `pty.fork()` → `tmux attach-session -t <name>` per WebSocket connection
- Session list polls `tmux list-sessions`
- Token auth via URL param → httpOnly cookie; localhost bypasses auth

## Key Design Decisions

- **Single file** — entire app in `webmux.py`, no external templates or static files
- **xterm.js from CDN** — no bundling
- **Font size** — adjustable via A-/A+ buttons, persisted in localStorage (large default for accessibility)
- **Session order** — drag-to-reorder, persisted in localStorage
- **File attachment** — drag-drop or button, saves to `~/Downloads/webmux-uploads/`, sends file path as text to the active session
- **Native scroll/selection** — relies on Claude Code's fullscreen renderer (`CLAUDE_CODE_NO_FLICKER=1`) so the mouse wheel and text selection work natively in the browser (see README)

## Commands

```bash
# Run
python3 webmux.py

# Custom port
WEBMUX_PORT=8080 python3 webmux.py

# Fixed token (for bookmarks)
WEBMUX_TOKEN=your-secret python3 webmux.py
```

Secrets (tokens, passwords, TLS certs) live in `~/.config/webmux/env` — never commit them.

## Release

```bash
./scripts/release.sh <version>   # bumps version, tags, pushes, updates the Homebrew tap
```
