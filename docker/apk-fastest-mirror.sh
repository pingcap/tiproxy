#!/bin/sh
# shellcheck disable=SC3043

get_hostname_url() {
    local n
    n="${1#*://}"
    echo "${n%%/*}"
}

time_cmd() {
    local proc start end

    proc=$(cut -d ' ' -f1 /proc/uptime)
    start="$(echo "$proc" | cut -d . -f1)$(echo "$proc" | cut -d . -f2)"
    "$@" >/dev/null 2>&1 || return
    proc=$(cut -d ' ' -f1 /proc/uptime)
    end="$(echo "$proc" | cut -d . -f1)$(echo "$proc" | cut -d . -f2)" 
    echo $((end - start))
}

DATA=""
MIRRORS=$(wget -qO- "http://rsync.alpinelinux.org/alpine/MIRRORS.txt")
DST=/etc/apk/mirrors.txt

#find best
for URL in $MIRRORS; do
    TIME=$(time_cmd wget -T 1 -t 1 -q "${URL%/}" -O /dev/null)

    if [ -n "$TIME" ]; then
        echo "$(get_hostname_url "$URL") was $TIME"
        DATA="$DATA$TIME $URL\n"
        if [ "$TIME" -lt "50" ]; then
          break
        fi
    fi

done

BEST=$(printf '%b' "$DATA" | sort -n | head -n 1 | cut -d ' ' -f2)
echo "Best mirror is: $BEST"

sed -i -r 's#^http.+/(.+/main)#'"${BEST%/}"'/\1#' /etc/apk/repositories
sed -i -r 's#^http.+/(.+/community)#'"${BEST%/}"'/\1#' /etc/apk/repositories
sed -i -r 's#^http.+/(.+/testing)#'"${BEST%/}"'/\1#' /etc/apk/repositories
