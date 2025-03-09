load("@rules_cc//cc:defs.bzl", "cc_binary", "cc_test")

package(default_visibility = ["//visibility:public"])

cc_binary(
    name = "mcap_arrow_example",
    srcs = ["src/examples/mcap_arrow_example.cc"],
    deps = [
        "//src/lib:mcap_arrow",
        "//src/proto:sensor_data_cc_proto",
    ],
    copts = ["-std=c++17"],
)

cc_binary(
    name = "mcap_arrow_ipc_example",
    srcs = ["src/examples/mcap_arrow_ipc_example.cc"],
    deps = [
        "//src/lib:mcap_arrow",
        "//src/proto:sensor_data_cc_proto",
    ],
    copts = ["-std=c++17"],
)

cc_test(
    name = "mcap_arrow_test",
    srcs = ["src/test/mcap_arrow_test.cc"],
    deps = [
        "//src/lib:mcap_arrow",
        "@com_google_googletest//:gtest_main",
    ],
    copts = ["-std=c++17"],
)

cc_binary(
    name = "minimal_example",
    srcs = [
        "minimal_example.cc",
        "third_party/mcap/src/mcap_impl.cpp",
    ],
    copts = [
        "-std=c++17",
        "-Ithird_party/mcap/include",
        "-DMCAP_COMPRESSION_NO_LZ4",
        "-DMCAP_COMPRESSION_NO_ZSTD",
    ],
) 