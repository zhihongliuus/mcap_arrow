# Apache Arrow extension for Bzlmod

def _apache_arrow_ext_impl(ctx):
    ctx.file("BUILD.bazel", "")
    ctx.file("WORKSPACE", "")
    ctx.file("WORKSPACE.bzlmod", "")
    
    # Use the existing arrow.BUILD file
    ctx.template(
        "arrow/BUILD.bazel",
        ctx.path(Label("//third_party:arrow.BUILD")),
    )

apache_arrow_ext = module_extension(
    implementation = _apache_arrow_ext_impl,
) 