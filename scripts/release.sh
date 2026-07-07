#!/bin/bash
# Release webmux — bump version, tag, push, update Homebrew formula.
# Usage: ./scripts/release.sh 1.5.0
set -euo pipefail

VERSION="${1:?Usage: ./scripts/release.sh <version>}"
REPO_DIR="$(cd "$(dirname "$0")/.."; pwd)"
TAP_DIR="${WEBMUX_TAP_DIR:-/tmp/homebrew-tap}"
TAG="v${VERSION}"

echo "==> Releasing webmux ${TAG}"

# 1. Bump version
cd "$REPO_DIR"
sed -i '' "s/^__version__ = .*/__version__ = \"${VERSION}\"/" webmux.py

# 2. Commit, tag, push
git add webmux.py
git commit -m "Release ${TAG}"
git tag -a "${TAG}" -m "${TAG}"
git push && git push origin "${TAG}"
SHA="$(git rev-parse HEAD)"
echo "==> Pushed ${TAG} (${SHA})"

# 3. Update Homebrew formula
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
echo "==> Updated homebrew-tap formula to ${TAG}"

echo ""
echo "Done! webmux ${TAG} released."
echo "Users update with: brew upgrade webmux"
