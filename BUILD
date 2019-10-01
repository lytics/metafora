load("@io_bazel_rules_go//go:def.bzl", "go_library", "go_test")
load("@bazel_gazelle//:def.bzl", "gazelle")

# gazelle:prefix github.com/lytics/metafora
gazelle(name = "gazelle")

go_library(
    name = "go_default_library",
    srcs = [
        "balancer.go",
        "balancer_res.go",
        "balancer_sleep.go",
        "client.go",
        "command.go",
        "coordinator.go",
        "doc.go",
        "handler.go",
        "ignore.go",
        "logger.go",
        "metafora.go",
        "task.go",
    ],
    importpath = "github.com/lytics/metafora",
    visibility = ["//visibility:public"],
)

go_test(
    name = "go_default_test",
    srcs = [
        "balancer_res_test.go",
        "balancer_test.go",
        "command_test.go",
        "ignore_test.go",
        "metafora_test.go",
        "slowtask_test.go",
        "util_test.go",
    ],
    embed = [":go_default_library"],
    size = "small",
)
