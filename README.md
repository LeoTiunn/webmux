# webmux

Browser-based tmux terminal client.

## Why

Claude Code runs in the terminal — close the terminal and the session is gone. tmux solves this by keeping sessions alive in the background, but it introduces new problems: no scrollback visibility, no easy way to manage multiple sessions or agents, and a clunky workflow switching between them.

webmux gives you a browser UI on top of tmux, organized around your **Claude Code sessions**: projects grouped into categories, each project expandable to its full Claude session history (resume any past conversation, or reconnect to a live one), rename sessions, native text selection, and drag-and-drop file upload. Built for engineers running many Claude Code agents in parallel. Desktop is the primary experience; mobile works for remote viewing and light interaction.

## Install

```bash
brew tap LeoTiunn/tap https://github.com/LeoTiunn/homebrew-tap.git
brew install webmux
brew services start webmux        # background, auto-restart at login
```

Then open **http://localhost:3033**.

To run in the foreground instead: `webmux`

### Prerequisites

- **tmux** — installed automatically by the formula
- **Claude Code** (optional) — install from [claude.com/product/claude-code](https://claude.com/product/claude-code)

> Set `WEBMUX_CMD=""` in Settings to use without Claude Code (plain shell).

### Native mouse scroll (Claude Code)

By default Claude Code runs its **classic** renderer, which stays in the normal
screen buffer. Inside tmux that means the mouse wheel triggers tmux copy-mode (a
yellow `[x/y]` bar hijacks the scroll) instead of scrolling Claude's own history.

To get native wheel scroll / drag-select in the browser, run Claude in its
**fullscreen** renderer so it takes the alternate screen and handles the mouse
itself. Add to `~/.claude/settings.json`:

```json
{ "env": { "CLAUDE_CODE_NO_FLICKER": "1" } }
```

(equivalent to running `/tui fullscreen` in a session). Recommended tmux config
so fullscreen + mouse work cleanly:

```tmux
set -g allow-passthrough on
set -s extended-keys on
set -as terminal-features 'xterm*:extkeys'
```

Restart existing Claude sessions to pick it up; new sessions get it
automatically. `CLAUDE_CODE_DISABLE_ALTERNATE_SCREEN=1` forces the classic
renderer back.

## Features

- **Projects** — sidebar groups live-tmux repos by name; drag to reorder or into custom categories (synced server-side). Click a project to attach its latest session.
- **Session history** — expand a project to see every Claude conversation (newest first, with time · branch · size). Live sessions float to the top; click to reconnect, or click a past one to `claude --resume` it.
- **Rename** — rename a live session via Claude's own `/rename` (syncs to the `/resume` picker); past sessions get a webmux-side display name.
- **Terminal** — xterm.js (Canvas renderer) over a WebSocket per session; 256-color, full CJK alignment, attaches at the right size instantly.
- **File upload** — drag-and-drop or 📎 button; multiple/large files supported (up to 200MB), saved to `~/Downloads/webmux-uploads/` and the paths sent to the session.
- **Native selection & scroll** — with Claude Code's fullscreen renderer (see below), the mouse wheel scrolls and text selects natively in the browser; no copy-mode toggle needed.
- **Remote access** — HTTPS + login page, toggle from Settings.
- **Theme** — dark/light toggle (light theme keeps terminal text readable via contrast adjustment).
- **Settings** — web config page (port, remote, SSL certs, projects root).
- **Mobile** — on-screen shortcut keys + virtual keyboard support.

## Configuration

All settings at **http://localhost:3033/settings**. Stored in `~/.config/webmux/env`.

| Var | Default | Purpose |
|-----|---------|---------|
| `WEBMUX_PORT` | 3033 | Local HTTP port |
| `WEBMUX_REMOTE` | off | Enable remote HTTPS access |
| `WEBMUX_REMOTE_PORT` | 3034 | Remote HTTPS port |
| `WEBMUX_USER` / `WEBMUX_PASS` | admin / random | Remote login |
| `WEBMUX_DEV_ROOT` | ~/Developer | Projects root for New Session |
| `WEBMUX_CMD` | `claude ...` | Command run in new sessions (empty = shell) |

## Update / Uninstall

```bash
brew update           # refresh the tap first — brew won't see a new version without this
brew upgrade webmux
brew services restart webmux   # load the new version

brew uninstall webmux
```

> If `brew upgrade` says the current version is "already installed" but a newer
> one was released, your local copy of the tap is stale. `brew update` pulls the
> latest formula; then `brew upgrade webmux` picks it up.

## Development

```bash
# Run locally
python3 webmux.py

# Release a new version (bumps version, tags, updates homebrew-tap)
./scripts/release.sh 1.12.2
```

## License

MIT
