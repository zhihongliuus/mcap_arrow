load("@rules_cc//cc:defs.bzl", "cc_library")

package(default_visibility = ["//visibility:public"])

cc_library(
    name = "arrow",
    hdrs = glob(["include/arrow/**/*.h"]),
    includes = ["include"],
)

cc_library(
    name = "arrow_ipc",
    hdrs = glob(["include/arrow/ipc/**/*.h"]),
    includes = ["include"],
    deps = [":arrow"],
)
