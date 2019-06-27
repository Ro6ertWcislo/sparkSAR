#!/bin/bash

set -e

git clone --depth 50 https://github.com/opencv/opencv.git opencv
cd opencv
mkdir build
cd build
cmake -D CMAKE_BUILD_TYPE=Release -DBUILD_SHARED_LIBS=OFF ..
make -j4

echo "OpenCV ready"