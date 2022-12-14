#!/bin/bash

CPP_DIRS="garner tests bench"
PY_DIRS="scripts"

function format_cpp_dir() {
    for FILE in $(find $@ -name '*.h' -or -name '*.hpp' -or -name '*.hh' \
                  -or -name '*.c' -or -name '*.cpp' -or -name '*.cc'); do
        echo " formatting ${FILE}"
        clang-format -i ${FILE}
    done
}

echo "Formatting C++ files..."
for CPP_DIR in ${CPP_DIRS}; do
    format_cpp_dir ${CPP_DIR}
done

echo
echo "Formatting Python files..."
black ${PY_DIRS}
