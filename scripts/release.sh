#!/bin/bash
# Release webmux — bump version, tag, push, update Homebrew formula.
# Usage: ./scripts/release.sh 1.5.0
set -euo pipefail

VERSION="${1:?Usage: ./scripts/release.sh <version>}"
REPO_DIR="$(cd "$(dirname "$0")/.."; pwd)"
TAG="v${VERSION}"
BRANCH="$(cd "$REPO_DIR"; git rev-parse --abbrev-ref HEAD)"
# The Homebrew tap Homebrew actually reads from (falls back to /tmp clone).
BREW_TAP="$(brew --repository 2>/dev/null)/Library/Taps/LeoTiunn/homebrew-tap"
TAP_DIR="${WEBMUX_TAP_DIR:-$BREW_TAP}"
[ -d "$TAP_DIR" ] || TAP_DIR="/tmp/homebrew-tap"

echo "==> Releasing webmux ${TAG} (branch ${BRANCH})"

# 1. Bump version
cd "$REPO_DIR"
sed -i '' "s/^__version__ = .*/__version__ = \"${VERSION}\"/" webmux.py

# 2. Commit, tag, push — push EXPLICITLY to origin/<branch> and the tag.
#    (A bare `git push` no-ops silently when the branch has no upstream, which
#    repeatedly shipped stale builds because the tag never reached the remote.)
git add webmux.py
git commit -m "Release ${TAG}"
git tag -a "${TAG}" -m "${TAG}"
git push origin "${BRANCH}"
git push origin "${TAG}"
SHA="$(git rev-parse HEAD)"

# 2b. VERIFY the remote actually has this branch HEAD and tag before proceeding.
REMOTE_HEAD="$(git ls-remote origin "refs/heads/${BRANCH}" 2>/dev/null | awk '{print $1}')"
REMOTE_TAG="$(git ls-remote origin "refs/tags/${TAG}" 2>/dev/null | awk '{print $1}')"
if [ "$REMOTE_HEAD" != "$SHA" ]; then
  echo "!! ERROR: origin/${BRANCH} is ${REMOTE_HEAD:-<missing>}, expected ${SHA}." >&2
  echo "   The branch push did not land — aborting before the formula points at a" >&2
  echo "   commit brew can't fetch. Run: git push origin ${BRANCH}" >&2
  exit 1
fi
if [ -z "$REMOTE_TAG" ]; then
  echo "!! ERROR: tag ${TAG} is not on origin — brew clones by tag and would fail." >&2
  echo "   Run: git push origin ${TAG}" >&2
  exit 1
fi
echo "==> Pushed & verified ${TAG} (${SHA})"

# 3. Update the Homebrew formula (in the tap brew actually reads from).
if [ ! -d "$TAP_DIR" ]; then
  git clone https://github.com/LeoTiunn/homebrew-tap.git "$TAP_DIR"
fi
cd "$TAP_DIR"
git pull -q
sed -i '' \
  -e "s/tag: \"v[0-9.]*\"/tag: \"${TAG}\"/" \
  -e "s/revision: \"[a-f0-9]*\"/revision: \"${SHA}\"/" \
  Formula/webmux.rb
git add Formula/webmux.rb
git commit -m "webmux ${TAG}"
git push
echo "==> Updated homebrew-tap formula to ${TAG} (${TAP_DIR})"

# 4. Drop brew's cached git clone so `brew upgrade` re-fetches the new tag
#    instead of building from a stale cached checkout.
rm -rf "$(brew --cache 2>/dev/null)/webmux--git" 2>/dev/null || true

echo ""
echo "Done! webmux ${TAG} released."
echo "Update this machine:  brew upgrade webmux && brew services restart webmux"
echo "Verify:               grep __version__ \"\$(brew --prefix)/opt/webmux/libexec/webmux.py\""
