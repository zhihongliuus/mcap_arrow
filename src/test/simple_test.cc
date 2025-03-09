#include <gtest/gtest.h>

// Simple test case using GoogleTest
TEST(SimpleTest, Addition) {
  // Simple assertion
  EXPECT_EQ(1 + 1, 2) << "Basic addition should work correctly";
}

TEST(SimpleTest, Subtraction) {
  // Another simple assertion
  EXPECT_EQ(5 - 3, 2) << "Basic subtraction should work correctly";
} 