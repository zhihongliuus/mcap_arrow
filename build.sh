#!/bin/bash
set -e

# Function to check if a directory exists
check_dir() {
  if [ ! -d "$1" ]; then
    mkdir -p "$1"
    return 1
  fi
  return 0
}

# Function to clone a git repository if needed
clone_repo() {
  local url="$1"
  local dir="$2"
  local branch="$3"
  
  if check_dir "$dir"; then
    echo "Directory $dir already exists, skipping clone"
    return 0
  fi
  
  echo "Cloning $url to $dir"
  mkdir -p "$(dirname "$dir")"
  git clone --depth 1 "$url" "$dir" ${branch:+--branch "$branch"}
}

# Create third-party directory if it doesn't exist
mkdir -p third_party

# Download MCAP library
clone_repo "https://github.com/foxglove/mcap.git" "third_party/mcap_repo" "main"
mkdir -p third_party/mcap/include
cp -r third_party/mcap_repo/cpp/mcap/include/* third_party/mcap/include/

# Copy the .inl files which are needed for implementation
if [ ! -f "third_party/mcap/include/mcap/types.inl" ]; then
  cp third_party/mcap_repo/cpp/mcap/include/mcap/types.inl third_party/mcap/include/mcap/
fi

# Create a simple implementation file for MCAP
echo "Creating MCAP implementation file..."
mkdir -p third_party/mcap/src
cat > third_party/mcap/src/mcap_impl.cpp << 'EOF'
#define MCAP_IMPLEMENTATION
#define MCAP_COMPRESSION_NO_LZ4
#define MCAP_COMPRESSION_NO_ZSTD
#include <mcap/reader.hpp>
#include <mcap/writer.hpp>
EOF

# Update mcap_simple_test.cc to add compression disabling at the top
if [ -f "test/mcap_simple_test.cc" ] && ! grep -q "MCAP_COMPRESSION_NO_LZ4" test/mcap_simple_test.cc; then
  sed -i '1i#define MCAP_COMPRESSION_NO_LZ4\n#define MCAP_COMPRESSION_NO_ZSTD' test/mcap_simple_test.cc
fi

# Compile the minimal example with the MCAP implementation
echo "Compiling minimal example with g++..."
g++ -std=c++17 -o minimal_example minimal_example.cc third_party/mcap/src/mcap_impl.cpp -I./third_party/mcap/include -DMCAP_COMPRESSION_NO_LZ4 -DMCAP_COMPRESSION_NO_ZSTD

# Create a basic test implementation
echo "Compiling basic test..."
g++ -std=c++17 -o basic_test test/mcap_basic_test.cc third_party/mcap/src/mcap_impl.cpp -I./third_party/mcap/include -DMCAP_COMPRESSION_NO_LZ4 -DMCAP_COMPRESSION_NO_ZSTD || {
  echo "Basic test g++ compilation failed, but minimal_example compilation succeeded."
}

if [ -f "basic_test" ]; then
  echo "Running basic test compiled with g++..."
  ./basic_test
fi

# Build with Bazel
echo "Building project with Bazel..."
bazel build --verbose_failures //:minimal_example

# Build and run basic test with Bazel
echo "Building basic test with Bazel..."
bazel build --copt="-DMCAP_COMPRESSION_NO_LZ4" --copt="-DMCAP_COMPRESSION_NO_ZSTD" //test:mcap_basic_test || {
  echo "Test build failed, but minimal_example compilation succeeded."
  echo "You can still run the example with ./minimal_example"
}

if bazel info output_base &>/dev/null; then
  echo "Running basic test built with Bazel..."
  bazel run --copt="-DMCAP_COMPRESSION_NO_LZ4" --copt="-DMCAP_COMPRESSION_NO_ZSTD" //test:mcap_basic_test
fi

echo "Build complete."
echo "To run the example:"
echo "  ./minimal_example"
echo "To run the Bazel-built example:"
echo "  bazel run //:minimal_example"
echo "To run the basic test:"
echo "  bazel run --copt=\"-DMCAP_COMPRESSION_NO_LZ4\" --copt=\"-DMCAP_COMPRESSION_NO_ZSTD\" //test:mcap_basic_test" 