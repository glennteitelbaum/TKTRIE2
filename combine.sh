#!/bin/bash
# Combine all headers, strip comments, remove #pragma once and internal #includes

output="tktrie_combined.h"

# Order matters - follow include chain
files=(
    "tktrie_defines.h"
    "tktrie_dataptr.h"
    "tktrie_ebr.h"
    "tktrie_node.h"
    "tktrie.h"
    "tktrie_core.h"
    "tktrie_insert.h"
    "tktrie_insert_probe.h"
    "tktrie_erase_probe.h"
    "tktrie_erase.h"
)

{
    echo "#pragma once"
    echo ""
    echo "#include <algorithm>"
    echo "#include <array>"
    echo "#include <atomic>"
    echo "#include <bit>"
    echo "#include <chrono>"
    echo "#include <cstdint>"
    echo "#include <cstring>"
    echo "#include <functional>"
    echo "#include <memory>"
    echo "#include <mutex>"
    echo "#include <new>"
    echo "#include <string>"
    echo "#include <string_view>"
    echo "#include <thread>"
    echo "#include <type_traits>"
    echo "#include <utility>"
    echo "#include <vector>"
    echo ""
    
    for f in "${files[@]}"; do
        if [[ -f "$f" ]]; then
            sed -e 's|//.*||g' \
                -e 's|/\*.*\*/||g' \
                -e '/#pragma once/d' \
                -e '/#include "tktrie/d' \
                -e '/#include <[^>]*>/d' \
                "$f"
            echo ""
        fi
    done
} | sed '/^[[:space:]]*$/N;/^\n$/d' > "$output"

echo "Created $output ($(wc -l < $output) lines)"
