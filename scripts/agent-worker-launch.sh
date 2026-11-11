#!/bin/sh
# Installed as `agent-worker`. The ELF is `agent-worker.bin` in the same directory.
# pyvenv.cfg `home` is the base interpreter's bin directory. uv keeps
# libpython3.X.so.1.0 in that tree's lib, which is not on the default linker path.
# `version` selects the minor (3.15.2 -> libpython3.15). Without it, any
# libpython3.*.so* in that tree is used. Free-threaded names are not special-cased;
# the wheel is linked to the GIL soname for that minor.
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
want=""
if [ -f "$cfg" ]; then
  home=$(sed -n 's/^home[[:space:]]*=[[:space:]]*//p' "$cfg" | head -n 1 | sed 's/[[:space:]]*$//')
  raw=$(sed -n 's/^version[[:space:]]*=[[:space:]]*//p' "$cfg" | head -n 1 | tr -d '[:space:]')
  want=$(printf '%s\n' "$raw" | cut -d. -f1-2)
  if [ -n "$home" ] && [ -d "$home/.." ]; then
    base=$(CDPATH= cd -- "$home/.." && pwd)
    for dir in "$base/lib" "$base/lib64"; do
      if [ ! -d "$dir" ]; then
        continue
      fi
      if [ -n "$want" ]; then
        if [ -f "$dir/libpython${want}.so.1.0" ] || [ -f "$dir/libpython${want}.so" ]; then
          lib=$dir
          break
        fi
      else
        for file in "$dir"/libpython3.*.so.1.0 "$dir"/libpython3.*.so; do
          if [ -f "$file" ]; then
            lib=$dir
            break
          fi
        done
        if [ -n "$lib" ]; then
          break
        fi
      fi
    done
  fi
fi
if [ -n "$lib" ]; then
  LD_LIBRARY_PATH="$lib${LD_LIBRARY_PATH:+:$LD_LIBRARY_PATH}"
  export LD_LIBRARY_PATH
fi
exec "$elf" "$@"
