#include <gtest/gtest.h>
#include "src/lib/mcap_arrow_index.h"
#include "src/lib/mcap_arrow_writer.h"
#include "src/proto/sensor_data.pb.h"

#include <arrow/api.h>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>
#include <unordered_map>

namespace mcap_arrow {
namespace {

class McapArrowIndexTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Create a temporary file path
    test_file_ = std::filesystem::temp_directory_path() / "mcap_index_test.mcap";
    
    // Remove the file if it exists
    if (std::filesystem::exists(test_file_)) {
      std::filesystem::remove(test_file_);
    }
    
    // Create test data
    createTestMcapFile();
    
    // Initialize the index
    index_ = std::make_unique<McapArrowIndex>();
    
    // Initialize the MCAP reader
    reader_ = std::make_unique<mcap::McapReader>();
    ASSERT_TRUE(reader_->open(test_file_.string()).ok());
  }

  void TearDown() override {
    // Close the reader
    if (reader_ != nullptr) {
      reader_->close();
    }
    
    // Clean up the test file
    if (std::filesystem::exists(test_file_)) {
      std::filesystem::remove(test_file_);
    }
  }

  // Helper to create a test MCAP file
  void createTestMcapFile() {
    // Create a writer
    McapArrowWriter writer;
    
    // Open the file
    ASSERT_TRUE(writer.open(test_file_.string(), 0));
    
    // Add sensor readings
    for (int i = 0; i < 10; ++i) {
      SensorReading reading;
      reading.set_timestamp(1000000 + i * 1000);  // increasing timestamps
      reading.set_sensor_id("sensor_" + std::to_string(i % 3));  // rotate between 3 sensors
      reading.set_value(20.0 + (i % 5));  // varying values
      reading.set_unit("celsius");
      
      // Add metadata
      (*reading.mutable_metadata())["type"] = (i % 2 == 0) ? "temperature" : "humidity";
      (*reading.mutable_metadata())["batch"] = std::to_string(i / 5);
      
      // Write the message
      std::string topic = "/sensors/" + ((i % 2 == 0) ? "temperature" : "humidity");
      ASSERT_TRUE(writer.writeMessage(topic, reading));
    }
    
    // Add a vehicle status message
    VehicleStatus status;
    status.set_timestamp(1500000);
    status.set_vehicle_id("car_123");
    status.set_speed(65.5);
    status.set_heading(180.0);
    
    (*status.mutable_system_status())["battery"] = 95.0;
    (*status.mutable_system_status())["fuel"] = 75.0;
    
    ASSERT_TRUE(writer.writeMessage("/vehicle/status", status));
    
    // Close the writer
    writer.close();
    
    // Make sure the file was created
    ASSERT_TRUE(std::filesystem::exists(test_file_));
    ASSERT_GT(std::filesystem::file_size(test_file_), 0);
  }

  std::filesystem::path test_file_;
  std::unique_ptr<McapArrowIndex> index_;
  std::unique_ptr<mcap::McapReader> reader_;
};

// Test building an index without any fields to index
TEST_F(McapArrowIndexTest, BuildIndexWithoutFields) {
  // Build the index without specifying fields to index
  EXPECT_TRUE(index_->build(*reader_));
  
  // Get topics and verify
  std::vector<std::string> topics = index_->getTopics();
  ASSERT_EQ(topics.size(), 3);
  
  // Sort topics to make the test deterministic
  std::sort(topics.begin(), topics.end());
  
  // Check topics
  EXPECT_EQ(topics[0], "/sensors/humidity");
  EXPECT_EQ(topics[1], "/sensors/temperature");
  EXPECT_EQ(topics[2], "/vehicle/status");
}

// Test building an index with specific fields
TEST_F(McapArrowIndexTest, BuildIndexWithFields) {
  // Specify fields to index
  std::unordered_map<std::string, std::vector<std::string>> fields_to_index;
  fields_to_index["/sensors/temperature"] = {"metadata.type", "metadata.batch"};
  fields_to_index["/sensors/humidity"] = {"metadata.type", "metadata.batch"};
  fields_to_index["/vehicle/status"] = {"vehicle_id", "system_status.battery"};
  
  // Build the index
  EXPECT_TRUE(index_->build(*reader_, fields_to_index));
  
  // Get indexed fields for each topic
  std::vector<std::string> temp_fields = index_->getIndexedFields("/sensors/temperature");
  std::vector<std::string> humid_fields = index_->getIndexedFields("/sensors/humidity");
  std::vector<std::string> vehicle_fields = index_->getIndexedFields("/vehicle/status");
  
  // Verify indexed fields
  ASSERT_EQ(temp_fields.size(), 2);
  ASSERT_EQ(humid_fields.size(), 2);
  ASSERT_EQ(vehicle_fields.size(), 2);
}

// Test querying by time range
TEST_F(McapArrowIndexTest, QueryByTimeRange) {
  // Build the index
  ASSERT_TRUE(index_->build(*reader_));
  
  // Query with time range
  std::vector<MessageInfo> messages = index_->query(
    "/sensors/temperature",
    1002000,  // start time
    1004000   // end time
  );
  
  // Verify we got results within the time range
  ASSERT_GT(messages.size(), 0);
  
  // Verify timestamps are within the range
  for (const auto& msg : messages) {
    EXPECT_GE(msg.log_time, 1002000);
    EXPECT_LE(msg.log_time, 1004000);
    EXPECT_EQ(msg.topic, "/sensors/temperature");
  }
}

// Test querying with filters
TEST_F(McapArrowIndexTest, QueryWithFilters) {
  // Specify fields to index
  std::unordered_map<std::string, std::vector<std::string>> fields_to_index;
  fields_to_index["/sensors/temperature"] = {"metadata.type", "metadata.batch"};
  
  // Build the index
  ASSERT_TRUE(index_->build(*reader_, fields_to_index));
  
  // Query with filters
  std::unordered_map<std::string, std::string> filters;
  filters["metadata.type"] = "temperature";
  
  std::vector<MessageInfo> messages = index_->query(
    "/sensors/temperature",
    0,
    std::numeric_limits<int64_t>::max(),
    filters
  );
  
  // Verify we got results
  ASSERT_GT(messages.size(), 0);
}

// Test querying with complex filters
TEST_F(McapArrowIndexTest, QueryWithComplexFilters) {
  // Specify fields to index
  std::unordered_map<std::string, std::vector<std::string>> fields_to_index;
  fields_to_index["/sensors/temperature"] = {"metadata.type", "metadata.batch"};
  
  // Build the index
  ASSERT_TRUE(index_->build(*reader_, fields_to_index));
  
  // Query with multiple filters
  std::unordered_map<std::string, std::string> filters;
  filters["metadata.type"] = "temperature";
  filters["metadata.batch"] = "0";
  
  std::vector<MessageInfo> messages = index_->query(
    "/sensors/temperature",
    0,
    std::numeric_limits<int64_t>::max(),
    filters
  );
  
  // Verify we got results
  ASSERT_GT(messages.size(), 0);
}

// Test querying a topic with no messages
TEST_F(McapArrowIndexTest, QueryTopicWithNoMessages) {
  // Build the index
  ASSERT_TRUE(index_->build(*reader_));
  
  // Query a non-existent topic
  std::vector<MessageInfo> messages = index_->query("non_existent_topic");
  
  // Verify we got no results
  ASSERT_EQ(messages.size(), 0);
}

// Test querying with a time range that has no messages
TEST_F(McapArrowIndexTest, QueryTimeRangeWithNoMessages) {
  // Build the index
  ASSERT_TRUE(index_->build(*reader_));
  
  // Query with a time range that has no messages
  std::vector<MessageInfo> messages = index_->query(
    "/sensors/temperature",
    2000000,  // start time after all messages
    3000000   // end time after all messages
  );
  
  // Verify we got no results
  ASSERT_EQ(messages.size(), 0);
}

// Test querying with filters that match no messages
TEST_F(McapArrowIndexTest, QueryFiltersWithNoMatches) {
  // Specify fields to index
  std::unordered_map<std::string, std::vector<std::string>> fields_to_index;
  fields_to_index["/sensors/temperature"] = {"metadata.type", "metadata.batch"};
  
  // Build the index
  ASSERT_TRUE(index_->build(*reader_, fields_to_index));
  
  // Query with filters that match no messages
  std::unordered_map<std::string, std::string> filters;
  filters["metadata.type"] = "non_existent_type";
  
  std::vector<MessageInfo> messages = index_->query(
    "/sensors/temperature",
    0,
    std::numeric_limits<int64_t>::max(),
    filters
  );
  
  // Verify we got no results
  ASSERT_EQ(messages.size(), 0);
}

// Test index performance with a large number of messages
TEST_F(McapArrowIndexTest, IndexPerformance) {
  // Create a temporary file for this specific test
  std::filesystem::path large_file = std::filesystem::temp_directory_path() / "large_index_test.mcap";
  
  // Remove the file if it exists
  if (std::filesystem::exists(large_file)) {
    std::filesystem::remove(large_file);
  }
  
  // Create a writer
  McapArrowWriter writer;
  ASSERT_TRUE(writer.open(large_file.string(), 0));
  
  // Write a large number of messages
  const int num_messages = 1000;
  for (int i = 0; i < num_messages; ++i) {
    SensorReading reading;
    reading.set_timestamp(1000000 + i * 1000);
    reading.set_sensor_id("sensor_" + std::to_string(i % 10));
    reading.set_value(20.0 + (i % 100) * 0.1);
    reading.set_unit("celsius");
    
    // Add metadata for filtering
    (*reading.mutable_metadata())["type"] = (i % 3 == 0) ? "temperature" : 
                                           (i % 3 == 1) ? "humidity" : "pressure";
    (*reading.mutable_metadata())["batch"] = std::to_string(i / 100);
    
    // Write to different topics
    std::string topic = "/sensors/" + 
                       ((i % 3 == 0) ? "temperature" : 
                       (i % 3 == 1) ? "humidity" : "pressure");
    ASSERT_TRUE(writer.writeMessage(topic, reading));
  }
  
  writer.close();
  
  // Open the file for reading
  auto large_reader = std::make_unique<mcap::McapReader>();
  ASSERT_TRUE(large_reader->open(large_file.string()).ok());
  
  // Create an index
  auto large_index = std::make_unique<McapArrowIndex>();
  
  // Specify fields to index
  std::unordered_map<std::string, std::vector<std::string>> fields_to_index;
  fields_to_index["/sensors/temperature"] = {"metadata.type", "metadata.batch"};
  fields_to_index["/sensors/humidity"] = {"metadata.type", "metadata.batch"};
  fields_to_index["/sensors/pressure"] = {"metadata.type", "metadata.batch"};
  
  // Measure time to build the index
  auto start_time = std::chrono::high_resolution_clock::now();
  
  ASSERT_TRUE(large_index->build(*large_reader, fields_to_index));
  
  auto end_time = std::chrono::high_resolution_clock::now();
  auto build_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
  
  // Measure time to query
  start_time = std::chrono::high_resolution_clock::now();
  
  std::unordered_map<std::string, std::string> filters;
  filters["metadata.type"] = "temperature";
  
  std::vector<MessageInfo> messages = large_index->query(
    "/sensors/temperature",
    0,
    std::numeric_limits<int64_t>::max(),
    filters
  );
  
  end_time = std::chrono::high_resolution_clock::now();
  auto query_duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
  
  // Check results
  ASSERT_GT(messages.size(), 0);
  
  // We don't assert on durations since they're hardware dependent,
  // but we can log them for informational purposes
  std::cout << "Index build time: " << build_duration.count() << "ms" << std::endl;
  std::cout << "Query time for " << messages.size() << " messages: " 
            << query_duration.count() << "ms" << std::endl;
  
  // Clean up
  large_reader->close();
  std::filesystem::remove(large_file);
}

// Test index memory usage with different indexing strategies
TEST_F(McapArrowIndexTest, IndexMemoryUsage) {
  // Create a temporary file for this specific test
  std::filesystem::path memory_file = std::filesystem::temp_directory_path() / "memory_test.mcap";
  
  // Remove the file if it exists
  if (std::filesystem::exists(memory_file)) {
    std::filesystem::remove(memory_file);
  }
  
  // Create a writer
  McapArrowWriter writer;
  ASSERT_TRUE(writer.open(memory_file.string(), 0));
  
  // Write messages with many fields and varying values
  const int num_messages = 100;
  for (int i = 0; i < num_messages; ++i) {
    SensorReading reading;
    reading.set_timestamp(1000000 + i * 1000);
    reading.set_sensor_id("sensor_" + std::to_string(i % 10));
    reading.set_value(20.0 + (i % 100) * 0.1);
    reading.set_unit("celsius");
    
    // Add lots of metadata entries (simulating high cardinality)
    (*reading.mutable_metadata())["id"] = std::to_string(i);  // Unique for each message
    (*reading.mutable_metadata())["type"] = (i % 3 == 0) ? "temperature" : 
                                           (i % 3 == 1) ? "humidity" : "pressure";
    (*reading.mutable_metadata())["batch"] = std::to_string(i / 10);
    (*reading.mutable_metadata())["category"] = (i % 5 == 0) ? "critical" : 
                                               (i % 5 == 1) ? "important" : 
                                               (i % 5 == 2) ? "normal" : 
                                               (i % 5 == 3) ? "low" : "debug";
    
    ASSERT_TRUE(writer.writeMessage("/sensors/data", reading));
  }
  
  writer.close();
  
  // Open the file for reading
  auto memory_reader = std::make_unique<mcap::McapReader>();
  ASSERT_TRUE(memory_reader->open(memory_file.string()).ok());
  
  // Test with different indexing strategies
  
  // 1. No field indexing
  {
    auto index_no_fields = std::make_unique<McapArrowIndex>();
    ASSERT_TRUE(index_no_fields->build(*memory_reader));
    
    // Query without filters (just time-based)
    std::vector<MessageInfo> messages = index_no_fields->query("/sensors/data");
    ASSERT_EQ(messages.size(), num_messages);
  }
  
  // 2. Minimal field indexing (just one field)
  {
    auto index_minimal = std::make_unique<McapArrowIndex>();
    
    std::unordered_map<std::string, std::vector<std::string>> fields_minimal;
    fields_minimal["/sensors/data"] = {"metadata.type"};
    
    ASSERT_TRUE(index_minimal->build(*memory_reader, fields_minimal));
    
    // Query with the indexed field
    std::unordered_map<std::string, std::string> filters;
    filters["metadata.type"] = "temperature";
    
    std::vector<MessageInfo> messages = index_minimal->query(
      "/sensors/data", 0, std::numeric_limits<int64_t>::max(), filters);
    
    // Should get approximately 1/3 of the messages
    ASSERT_GT(messages.size(), 0);
    ASSERT_LT(messages.size(), num_messages);
  }
  
  // 3. Comprehensive field indexing (all fields)
  {
    auto index_comprehensive = std::make_unique<McapArrowIndex>();
    
    std::unordered_map<std::string, std::vector<std::string>> fields_comprehensive;
    fields_comprehensive["/sensors/data"] = {
      "metadata.id", "metadata.type", "metadata.batch", "metadata.category"
    };
    
    ASSERT_TRUE(index_comprehensive->build(*memory_reader, fields_comprehensive));
    
    // Query with multiple filters
    std::unordered_map<std::string, std::string> filters;
    filters["metadata.type"] = "temperature";
    filters["metadata.category"] = "critical";
    
    std::vector<MessageInfo> messages = index_comprehensive->query(
      "/sensors/data", 0, std::numeric_limits<int64_t>::max(), filters);
    
    // Should get approximately 1/15 of the messages (1/3 * 1/5)
    ASSERT_GT(messages.size(), 0);
    ASSERT_LT(messages.size(), num_messages / 3);
  }
  
  // Clean up
  memory_reader->close();
  std::filesystem::remove(memory_file);
}

}  // namespace
}  // namespace mcap_arrow 