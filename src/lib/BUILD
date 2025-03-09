load("@rules_cc//cc:defs.bzl", "cc_library")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "mcap_arrow",
    srcs = [
        "mcap_arrow_reader.cc",
        "mcap_arrow_writer.cc",
        "mcap_arrow_converter.cc",
        "mcap_arrow_index.cc",
        "mcap_arrow_ipc.cc",
    ],
    hdrs = [
        "mcap_arrow_reader.h",
        "mcap_arrow_writer.h",
        "mcap_arrow_converter.h",
        "mcap_arrow_index.h",
        "mcap_arrow_ipc.h",
    ],
    deps = [
        "//src/proto:sensor_data_cc_proto",
        "@mcap",
        "@apache_arrow//:arrow",
        "@apache_arrow//:arrow_ipc",
        "@com_google_protobuf//:protobuf",
        "@boost//:filesystem",
        "@boost//:system",
        "@zlib",
        "@lz4",
        "@zstd",
    ],
    copts = [
        "-std=c++17",
        "-DMCAP_COMPRESSION_NO_LZ4",
        "-DMCAP_COMPRESSION_NO_ZSTD",
    ],
    visibility = ["//visibility:public"],
) 