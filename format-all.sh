#!/bin/bash

DIRS="garner clients"

function format_dir() {
    for FILE in $(find $@ -name '*.h' -or -name '*.hpp' -or -name '*.hh' \
                  -or -name '*.c' -or -name '*.cpp' -or -name '*.cc'); do
        echo "formatting ${FILE}"
        clang-format -i ${FILE}
    done
}

for DIR in ${DIRS}; do
    format_dir ${DIR}
done
