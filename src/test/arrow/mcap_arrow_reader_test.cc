#include <gtest/gtest.h>
#include "src/lib/mcap_arrow_reader.h"
#include "src/lib/mcap_arrow_writer.h"
#include "src/proto/sensor_data.pb.h"

#include <arrow/api.h>
#include <filesystem>
#include <memory>
#include <string>
#include <vector>

namespace mcap_arrow {
namespace {

class McapArrowReaderTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Create a temporary file path
    test_file_ = std::filesystem::temp_directory_path() / "mcap_reader_test.mcap";
    
    // Remove the file if it exists
    if (std::filesystem::exists(test_file_)) {
      std::filesystem::remove(test_file_);
    }
    
    // Create test data
    createTestMcapFile();
    
    // Initialize the reader
    reader_ = std::make_unique<McapArrowReader>();
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
      
      // Add location data
      Point3D* location = reading.mutable_location();
      location->set_x(1.0 * i);
      location->set_y(2.0 * i);
      location->set_z(3.0 * i);
      
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
    
    Point3D* position = status.mutable_position();
    position->set_x(10.0);
    position->set_y(20.0);
    position->set_z(0.0);
    
    status.add_active_systems("engine");
    status.add_active_systems("transmission");
    
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
  std::unique_ptr<McapArrowReader> reader_;
};

// Test opening and closing a file
TEST_F(McapArrowReaderTest, OpenAndClose) {
  // Open the file
  EXPECT_TRUE(reader_->open(test_file_.string()));
  
  // Close the file
  reader_->close();
}

// Test getting topics
TEST_F(McapArrowReaderTest, GetTopics) {
  // Open the file
  ASSERT_TRUE(reader_->open(test_file_.string()));
  
  // Get the topics
  std::vector<std::string> topics = reader_->getTopics();
  
  // Verify we have the expected topics
  ASSERT_EQ(topics.size(), 3);
  
  // Sort topics to make the test deterministic
  std::sort(topics.begin(), topics.end());
  
  // Check topics
  EXPECT_EQ(topics[0], "/sensors/humidity");
  EXPECT_EQ(topics[1], "/sensors/temperature");
  EXPECT_EQ(topics[2], "/vehicle/status");
}

// Test getting schemas
TEST_F(McapArrowReaderTest, GetSchemas) {
  // Open the file
  ASSERT_TRUE(reader_->open(test_file_.string()));
  
  // Get schemas for all topics
  std::vector<std::string> topics = reader_->getTopics();
  for (const auto& topic : topics) {
    std::shared_ptr<arrow::Schema> schema = reader_->getSchema(topic);
    ASSERT_NE(schema, nullptr);
    EXPECT_GT(schema->num_fields(), 0);
  }
  
  // Check for non-existent topic
  EXPECT_EQ(reader_->getSchema("non_existent_topic"), nullptr);
}

// Test building an index
TEST_F(McapArrowReaderTest, BuildIndex) {
  // Open the file
  ASSERT_TRUE(reader_->open(test_file_.string()));
  
  // Build the index
  EXPECT_TRUE(reader_->buildIndex());
}

// Test querying without filters
TEST_F(McapArrowReaderTest, QueryWithoutFilters) {
  // Open the file
  ASSERT_TRUE(reader_->open(test_file_.string()));
  
  // Build the index
  ASSERT_TRUE(reader_->buildIndex());
  
  // Query a topic
  std::shared_ptr<arrow::Table> table = reader_->query("/sensors/temperature");
  ASSERT_NE(table, nullptr);
  EXPECT_GT(table->num_rows(), 0);
  
  // Verify the schema
  std::shared_ptr<arrow::Schema> schema = table->schema();
  ASSERT_NE(schema, nullptr);
  EXPECT_GT(schema->num_fields(), 0);
}

// Test querying with filters
TEST_F(McapArrowReaderTest, QueryWithFilters) {
  // Open the file
  ASSERT_TRUE(reader_->open(test_file_.string()));
  
  // Build the index
  ASSERT_TRUE(reader_->buildIndex());
  
  // Query with time range
  std::shared_ptr<arrow::Table> table = reader_->query(
    "/sensors/temperature",
    1002000,  // start time
    1004000  // end time
  );
  
  ASSERT_NE(table, nullptr);
  EXPECT_GT(table->num_rows(), 0);
  EXPECT_LT(table->num_rows(), 10);  // Should be fewer than all rows
  
  // Query with field filter
  std::unordered_map<std::string, std::string> filters;
  filters["type"] = "temperature";
  
  std::shared_ptr<arrow::Table> filtered_table = reader_->query(
    "/sensors/temperature",
    0,
    std::numeric_limits<int64_t>::max(),
    filters
  );
  
  ASSERT_NE(filtered_table, nullptr);
  EXPECT_GT(filtered_table->num_rows(), 0);
}

// Test querying vehicle status
TEST_F(McapArrowReaderTest, QueryVehicleStatus) {
  // Open the file
  ASSERT_TRUE(reader_->open(test_file_.string()));
  
  // Build the index
  ASSERT_TRUE(reader_->buildIndex());
  
  // Query vehicle status
  std::shared_ptr<arrow::Table> table = reader_->query("/vehicle/status");
  ASSERT_NE(table, nullptr);
  EXPECT_EQ(table->num_rows(), 1);
  
  // Verify basic fields
  std::shared_ptr<arrow::Schema> schema = table->schema();
  ASSERT_NE(schema, nullptr);
  EXPECT_NE(schema->GetFieldByName("timestamp"), nullptr);
  EXPECT_NE(schema->GetFieldByName("vehicle_id"), nullptr);
  EXPECT_NE(schema->GetFieldByName("speed"), nullptr);
}

// Test error handling for non-existent file
TEST_F(McapArrowReaderTest, NonExistentFile) {
  // Create a new reader
  McapArrowReader reader;
  
  // Try to open a non-existent file
  EXPECT_FALSE(reader.open("non_existent_file.mcap"));
}

// Test error handling for invalid file format
TEST_F(McapArrowReaderTest, InvalidFileFormat) {
  // Create a temporary file with invalid content
  std::filesystem::path invalid_file = std::filesystem::temp_directory_path() / "invalid.mcap";
  
  // Write some garbage data
  std::ofstream file(invalid_file, std::ios::binary);
  file << "This is not a valid MCAP file";
  file.close();
  
  // Create a new reader
  McapArrowReader reader;
  
  // Try to open the invalid file
  EXPECT_FALSE(reader.open(invalid_file.string()));
  
  // Clean up
  std::filesystem::remove(invalid_file);
}

// Test querying a non-existent topic
TEST_F(McapArrowReaderTest, QueryNonExistentTopic) {
  // Open the file
  ASSERT_TRUE(reader_->open(test_file_.string()));
  
  // Build the index
  ASSERT_TRUE(reader_->buildIndex());
  
  // Query a non-existent topic
  std::shared_ptr<arrow::Table> table = reader_->query("non_existent_topic");
  EXPECT_EQ(table, nullptr);
}

// Test querying with an empty time range
TEST_F(McapArrowReaderTest, QueryEmptyTimeRange) {
  // Open the file
  ASSERT_TRUE(reader_->open(test_file_.string()));
  
  // Build the index
  ASSERT_TRUE(reader_->buildIndex());
  
  // Query with a time range that has no messages
  std::shared_ptr<arrow::Table> table = reader_->query(
    "/sensors/temperature",
    2000000,  // start time after all messages
    3000000   // end time after all messages
  );
  
  // Should return an empty table, not nullptr
  ASSERT_NE(table, nullptr);
  EXPECT_EQ(table->num_rows(), 0);
}

// Test querying with filters that match no messages
TEST_F(McapArrowReaderTest, QueryNoMatchingFilters) {
  // Open the file
  ASSERT_TRUE(reader_->open(test_file_.string()));
  
  // Build the index
  ASSERT_TRUE(reader_->buildIndex());
  
  // Query with filters that match no messages
  std::unordered_map<std::string, std::string> filters;
  filters["type"] = "non_existent_type";
  
  std::shared_ptr<arrow::Table> table = reader_->query(
    "/sensors/temperature",
    0,
    std::numeric_limits<int64_t>::max(),
    filters
  );
  
  // Should return an empty table, not nullptr
  ASSERT_NE(table, nullptr);
  EXPECT_EQ(table->num_rows(), 0);
}

// Test query performance with a large number of messages
TEST_F(McapArrowReaderTest, QueryPerformance) {
  // Create a temporary file for this specific test
  std::filesystem::path large_file = std::filesystem::temp_directory_path() / "large_test.mcap";
  
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
  
  // Create a reader
  McapArrowReader large_reader;
  ASSERT_TRUE(large_reader.open(large_file.string()));
  ASSERT_TRUE(large_reader.buildIndex());
  
  // Measure time to query all temperature messages
  auto start_time = std::chrono::high_resolution_clock::now();
  
  std::unordered_map<std::string, std::string> filters;
  filters["type"] = "temperature";
  
  std::shared_ptr<arrow::Table> table = large_reader.query(
    "/sensors/temperature",
    0,
    std::numeric_limits<int64_t>::max(),
    filters
  );
  
  auto end_time = std::chrono::high_resolution_clock::now();
  auto duration = std::chrono::duration_cast<std::chrono::milliseconds>(end_time - start_time);
  
  // Check results
  ASSERT_NE(table, nullptr);
  EXPECT_GT(table->num_rows(), 0);
  
  // We don't assert on the duration since it's hardware dependent,
  // but we can log it for informational purposes
  std::cout << "Query time for " << table->num_rows() << " rows: " 
            << duration.count() << "ms" << std::endl;
  
  // Clean up
  large_reader.close();
  std::filesystem::remove(large_file);
}

// Test extracting data from the queried Arrow table
TEST_F(McapArrowReaderTest, ExtractDataFromTable) {
  // Open the file
  ASSERT_TRUE(reader_->open(test_file_.string()));
  
  // Build the index
  ASSERT_TRUE(reader_->buildIndex());
  
  // Query a topic
  std::shared_ptr<arrow::Table> table = reader_->query("/sensors/temperature");
  ASSERT_NE(table, nullptr);
  ASSERT_GT(table->num_rows(), 0);
  
  // Get the schema
  std::shared_ptr<arrow::Schema> schema = table->schema();
  
  // Find the timestamp column
  int timestamp_index = schema->GetFieldIndex("timestamp");
  ASSERT_GE(timestamp_index, 0);
  
  // Extract timestamps
  auto timestamp_array = std::static_pointer_cast<arrow::Int64Array>(
    table->column(timestamp_index)->chunk(0));
  
  // Check that timestamps are ascending
  for (int i = 1; i < timestamp_array->length(); ++i) {
    EXPECT_LE(timestamp_array->Value(i-1), timestamp_array->Value(i));
  }
}

}  // namespace
}  // namespace mcap_arrow 