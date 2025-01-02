#!/bin/bash -l

SRCDIR="$(pwd)"
DESTDIR="$1"

if [ -z "$DESTDIR" ]; then
    echo "Destination folder not provided"
    exit 1
fi


STORAGES=$(find "${SRCDIR}/internal/storage" -name "*Storage.md")

# storage backends
mkdir -p "${DESTDIR}/internal/storage"
for F in $STORAGES; do
    echo "$F"
    FNAME=$(basename "$F")
    TITLE=$(grep -E "^#" "$F" | head -n 1 | sed -e 's+# ++g')
    echo "'${TITLE//\`/}'"
    if [ "${TITLE}" == "" ]; then continue; fi
    rm --force "${DESTDIR}/internal/storage/${FNAME}"
    cat << EOF >> "${DESTDIR}/internal/storage/${FNAME}"
---
title: ${TITLE//\`/}
description: >
  Toplevel ${FNAME/.md/}
categories: [cc-event-store]
tags: [cc-event-store, Storage, ${FNAME/Storage.md/}]
weight: 2
---

EOF
    cat "$F" >> "${DESTDIR}/internal/storage/${FNAME}"
done

if [ -e "${SRCDIR}/internal/storage/README.md" ]; then
    cat << EOF > "${DESTDIR}/internal/storage/_index.md"
---
title: cc-event-store's storage backends
description: Documentation of cc-event-store's storage backends
categories: [cc-event-store]
tags: [cc-event-store, Storage, General]
weight: 40
---

EOF
    cat "${SRCDIR}/internal/storage/README.md" >> "${DESTDIR}/internal/storage/_index.md"
fi

mkdir -p "${DESTDIR}/internal/api"
if [ -e "${SRCDIR}/internal/api/README.md" ]; then
    cat << EOF > "${DESTDIR}/internal/api/_index.md"
---
title: cc-event-store's REST API
description: Documentation of cc-event-store's REST API
categories: [cc-event-store]
tags: [cc-event-store, REST API, General]
weight: 40
---

EOF
    cat "${SRCDIR}/internal/api/README.md" >> "${DESTDIR}/internal/api/_index.md"
fi

if [ -e "${SRCDIR}/README.md" ]; then
    cat << EOF > "${DESTDIR}/_index.md"
---
title: cc-event-store
description: Documentation of cc-event-store
categories: [cc-event-store]
tags: [cc-event-store, General]
weight: 40
---

EOF
    cat "${SRCDIR}/README.md" >> "${DESTDIR}/_index.md"
    sed -i -e 's+README.md+_index.md+g' \
           -e 's+https://github.com/ClusterCockpit/cc-metric-collector/tree/main+../cc-metric-collector+g' "${DESTDIR}/_index.md"
fi



