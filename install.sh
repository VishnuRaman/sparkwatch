#!/bin/sh
# Install the latest sparkwatch release for this machine.
#
#   curl -fsSL https://raw.githubusercontent.com/VishnuRaman/sparkwatch/main/install.sh | sh
#
# Options via environment:
#   SPARKWATCH_VERSION=v0.1.1   pin a version (default: latest release)
#   SPARKWATCH_INSTALL_DIR=...  where to put the binary (default: ~/.local/bin,
#                               or /usr/local/bin when run as root)
set -eu

repo="VishnuRaman/sparkwatch"
version="${SPARKWATCH_VERSION:-latest}"

os="$(uname -s)"
arch="$(uname -m)"
case "$os" in
  Linux)  os_t="unknown-linux-musl" ;;
  Darwin) os_t="apple-darwin" ;;
  *) echo "unsupported OS: $os (Windows: download the .zip from the releases page)" >&2; exit 1 ;;
esac
case "$arch" in
  x86_64|amd64)  arch_t="x86_64" ;;
  arm64|aarch64) arch_t="aarch64" ;;
  *) echo "unsupported architecture: $arch" >&2; exit 1 ;;
esac
target="${arch_t}-${os_t}"

if [ "$version" = "latest" ]; then
  # The redirect target of /releases/latest carries the tag; no API token needed.
  version="$(curl -fsSLI -o /dev/null -w '%{url_effective}' "https://github.com/$repo/releases/latest" | sed 's#.*/##')"
fi
[ -n "$version" ] || { echo "could not determine the latest version" >&2; exit 1; }
num="${version#v}"

asset="sparkwatch-${num}-${target}.tar.gz"
base="https://github.com/$repo/releases/download/$version"

if [ -n "${SPARKWATCH_INSTALL_DIR:-}" ]; then
  dir="$SPARKWATCH_INSTALL_DIR"
elif [ "$(id -u)" = "0" ]; then
  dir="/usr/local/bin"
else
  dir="$HOME/.local/bin"
fi

tmp="$(mktemp -d)"
trap 'rm -rf "$tmp"' EXIT
echo "downloading $asset"
curl -fsSL "$base/$asset" -o "$tmp/$asset"
curl -fsSL "$base/SHA256SUMS" -o "$tmp/SHA256SUMS"

cd "$tmp"
if command -v sha256sum >/dev/null 2>&1; then
  grep " $asset\$" SHA256SUMS | sha256sum -c - >/dev/null
elif command -v shasum >/dev/null 2>&1; then
  grep " $asset\$" SHA256SUMS | shasum -a 256 -c - >/dev/null
else
  echo "warning: no sha256 tool found, skipping checksum" >&2
fi
tar -xzf "$asset"

mkdir -p "$dir"
install -m 755 "sparkwatch-${num}-${target}/sparkwatch" "$dir/sparkwatch"
echo "installed sparkwatch $version to $dir/sparkwatch"
case ":$PATH:" in
  *":$dir:"*) ;;
  *) echo "note: $dir is not on your PATH" ;;
esac
