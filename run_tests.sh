#!/bin/bash

# Set verbose output
set -x

# Run all tests
bazel test //:all_tests

# Run individual tests if needed
# bazel test //src/test:simple_test
# bazel test //src/test:mcap_arrow_minimal_test

# Run with more verbose output if tests fail
# bazel test //:all_tests --test_output=errors

# Run with all output regardless of test status
# bazel test //:all_tests --test_output=all 