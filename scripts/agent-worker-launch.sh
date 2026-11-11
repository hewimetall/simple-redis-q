#!/bin/sh
# Installed as `agent-worker`. The ELF is `agent-worker.bin` in the same directory.
# pyvenv.cfg `home` is the base interpreter's bin directory. uv keeps
# libpython3.12.so.1.0 in that tree's lib, which is not on the default linker path.
set -eu
if [ -n "${0##*/*}" ]; then
  resolved=$(command -v -- "$0") || resolved=$0
else
  resolved=$0
fi
here=$(CDPATH= cd -- "$(dirname -- "$resolved")" && pwd)
elf=$here/agent-worker.bin
cfg=$here/../pyvenv.cfg
lib=""
if [ -f "$cfg" ]; then
  home=$(sed -n 's/^home[[:space:]]*=[[:space:]]*//p' "$cfg" | head -n 1 | sed 's/[[:space:]]*$//')
  if [ -n "$home" ] && [ -d "$home/.." ]; then
    base=$(CDPATH= cd -- "$home/.." && pwd)
    for dir in "$base/lib" "$base/lib64"; do
      if [ -f "$dir/libpython3.12.so.1.0" ] || [ -f "$dir/libpython3.12.so" ]; then
        lib=$dir
        break
      fi
    done
  fi
fi
if [ -n "$lib" ]; then
  LD_LIBRARY_PATH="$lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
  export LD_LIBRARY_PATH
fi
exec "$elf" "$@"
