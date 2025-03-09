load("@rules_proto//proto:defs.bzl", "proto_library")
load("@rules_cc//cc:defs.bzl", "cc_proto_library")

package(default_visibility = ["//visibility:public"])

proto_library(
    name = "sensor_data_proto",
    srcs = ["sensor_data.proto"],
)

cc_proto_library(
    name = "sensor_data_cc_proto",
    deps = [":sensor_data_proto"],
) 