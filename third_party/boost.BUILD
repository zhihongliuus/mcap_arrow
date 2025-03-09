load("@rules_cc//cc:defs.bzl", "cc_library")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "boost",
    hdrs = glob(["boost/**/*.hpp"]),
    includes = ["."],
)

cc_library(
    name = "filesystem",
    deps = [":boost"],
)

cc_library(
    name = "system",
    deps = [":boost"],
) 