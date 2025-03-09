workspace(name = "mcap_arrow")

# Load tools for local repositories
load("@bazel_tools//tools/build_defs/repo:local.bzl", "new_local_repository")

# Local repositories that can't be managed via Bzlmod
new_local_repository(
    name = "apache_arrow",
    path = "third_party/arrow",
    build_file = "third_party/arrow.BUILD",
)

new_local_repository(
    name = "mcap",
    path = "third_party/mcap",
    build_file = "third_party/mcap.BUILD",
)

# Note: zlib is already defined by protobuf_deps, so we'll use that one

new_local_repository(
    name = "lz4",
    path = "third_party/lz4",
    build_file = "third_party/lz4.BUILD",
)

new_local_repository(
    name = "zstd",
    path = "third_party/zstd",
    build_file = "third_party/zstd.BUILD",
)

new_local_repository(
    name = "boost",
    path = "third_party/boost",
    build_file = "third_party/boost.BUILD",
)

# Core dependencies (rules_cc, protobuf, abseil-cpp, googletest, rules_proto)
# are managed via Bzlmod in MODULE.bazel 