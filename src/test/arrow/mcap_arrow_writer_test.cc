#include <gtest/gtest.h>
#include "src/lib/mcap_arrow_writer.h"
#include "src/proto/sensor_data.pb.h"

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <filesystem>
#include <fstream>
#include <memory>
#include <string>

namespace mcap_arrow {
namespace {

class McapArrowWriterTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Create a temporary file path
    test_file_ = std::filesystem::temp_directory_path() / "mcap_writer_test.mcap";
    
    // Remove the file if it exists
    if (std::filesystem::exists(test_file_)) {
      std::filesystem::remove(test_file_);
    }
    
    // Initialize the writer
    writer_ = std::make_unique<McapArrowWriter>();
  }

  void TearDown() override {
    // Close the writer
    if (writer_ != nullptr) {
      writer_->close();
    }
    
    // Clean up the test file
    if (std::filesystem::exists(test_file_)) {
      std::filesystem::remove(test_file_);
    }
  }

  // Helper to create a test SensorReading
  SensorReading createTestSensorReading(int64_t timestamp = 123456789) {
    SensorReading reading;
    reading.set_timestamp(timestamp);
    reading.set_sensor_id("test_sensor");
    reading.set_value(22.5);
    reading.set_unit("celsius");
    
    Point3D* location = reading.mutable_location();
    location->set_x(1.0);
    location->set_y(2.0);
    location->set_z(3.0);
    
    (*reading.mutable_metadata())["type"] = "temperature";
    (*reading.mutable_metadata())["manufacturer"] = "ACME";
    
    return reading;
  }
  
  // Helper to create a test VehicleStatus
  VehicleStatus createTestVehicleStatus(int64_t timestamp = 123456789) {
    VehicleStatus status;
    status.set_timestamp(timestamp);
    status.set_vehicle_id("test_vehicle");
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
    
    return status;
  }
  
  // Helper to create a test Arrow table
  std::shared_ptr<arrow::Table> createTestTable(int num_rows = 5) {
    // Create schema
    auto schema = arrow::Schema::Make({
      arrow::field("timestamp", arrow::int64()),
      arrow::field("sensor_id", arrow::utf8()),
      arrow::field("value", arrow::float64()),
      arrow::field("unit", arrow::utf8())
    });
    
    // Create arrays
    arrow::Int64Builder timestamp_builder;
    arrow::StringBuilder sensor_id_builder;
    arrow::DoubleBuilder value_builder;
    arrow::StringBuilder unit_builder;
    
    for (int i = 0; i < num_rows; ++i) {
      EXPECT_TRUE(timestamp_builder.Append(1000000 + i * 1000).ok());
      EXPECT_TRUE(sensor_id_builder.Append("sensor_" + std::to_string(i % 3)).ok());
      EXPECT_TRUE(value_builder.Append(20.0 + i * 0.5).ok());
      EXPECT_TRUE(unit_builder.Append("celsius").ok());
    }
    
    std::shared_ptr<arrow::Array> timestamp_array;
    std::shared_ptr<arrow::Array> sensor_id_array;
    std::shared_ptr<arrow::Array> value_array;
    std::shared_ptr<arrow::Array> unit_array;
    
    EXPECT_TRUE(timestamp_builder.Finish(&timestamp_array).ok());
    EXPECT_TRUE(sensor_id_builder.Finish(&sensor_id_array).ok());
    EXPECT_TRUE(value_builder.Finish(&value_array).ok());
    EXPECT_TRUE(unit_builder.Finish(&unit_array).ok());
    
    return arrow::Table::Make(schema, {timestamp_array, sensor_id_array, value_array, unit_array});
  }

  std::filesystem::path test_file_;
  std::unique_ptr<McapArrowWriter> writer_;
};

// Test opening and closing a file
TEST_F(McapArrowWriterTest, OpenAndClose) {
  // Open the file
  EXPECT_TRUE(writer_->open(test_file_.string(), 0));
  
  // Close the file
  writer_->close();
  
  // Verify the file was created
  EXPECT_TRUE(std::filesystem::exists(test_file_));
}

// Test writing a protobuf message
TEST_F(McapArrowWriterTest, WriteMessage) {
  // Open the file
  ASSERT_TRUE(writer_->open(test_file_.string(), 0));
  
  // Create a sensor reading
  SensorReading reading = createTestSensorReading();
  
  // Write the message
  EXPECT_TRUE(writer_->writeMessage("/sensors/temperature", reading));
  
  // Close the file
  writer_->close();
  
  // Verify the file was created
  EXPECT_TRUE(std::filesystem::exists(test_file_));
  EXPECT_GT(std::filesystem::file_size(test_file_), 0);
}

// Test writing a protobuf message with specified timestamp
TEST_F(McapArrowWriterTest, WriteMessageWithTimestamp) {
  // Open the file
  ASSERT_TRUE(writer_->open(test_file_.string(), 0));
  
  // Create a sensor reading (with timestamp set to 0)
  SensorReading reading = createTestSensorReading(0);
  
  // Specify a timestamp for the message
  int64_t specified_timestamp = 987654321;
  
  // Write the message with the specified timestamp
  EXPECT_TRUE(writer_->writeMessage("/sensors/temperature", reading, specified_timestamp));
  
  // Close the file
  writer_->close();
  
  // Verify the file was created
  EXPECT_TRUE(std::filesystem::exists(test_file_));
  EXPECT_GT(std::filesystem::file_size(test_file_), 0);
  
  // Note: We would need to read back the file to verify the timestamp was used,
  // which would require a reader. This test just verifies the API works.
}

// Test writing an Arrow table
TEST_F(McapArrowWriterTest, WriteTable) {
  // Open the file
  ASSERT_TRUE(writer_->open(test_file_.string(), 0));
  
  // Create a table
  auto table = createTestTable();
  
  // Write the table
  EXPECT_TRUE(writer_->writeTable("/sensors/temperature", table));
  
  // Close the file
  writer_->close();
  
  // Verify the file was created
  EXPECT_TRUE(std::filesystem::exists(test_file_));
  EXPECT_GT(std::filesystem::file_size(test_file_), 0);
}

// Test writing an Arrow table with custom timestamp column
TEST_F(McapArrowWriterTest, WriteTableWithCustomTimestampColumn) {
  // Open the file
  ASSERT_TRUE(writer_->open(test_file_.string(), 0));
  
  // Create schema with custom timestamp column name
  auto schema = arrow::Schema::Make({
    arrow::field("custom_time", arrow::int64()),
    arrow::field("sensor_id", arrow::utf8()),
    arrow::field("value", arrow::float64())
  });
  
  // Create arrays
  arrow::Int64Builder timestamp_builder;
  arrow::StringBuilder sensor_id_builder;
  arrow::DoubleBuilder value_builder;
  
  for (int i = 0; i < 5; ++i) {
    EXPECT_TRUE(timestamp_builder.Append(1000000 + i * 1000).ok());
    EXPECT_TRUE(sensor_id_builder.Append("sensor_" + std::to_string(i)).ok());
    EXPECT_TRUE(value_builder.Append(20.0 + i * 0.5).ok());
  }
  
  std::shared_ptr<arrow::Array> timestamp_array;
  std::shared_ptr<arrow::Array> sensor_id_array;
  std::shared_ptr<arrow::Array> value_array;
  
  EXPECT_TRUE(timestamp_builder.Finish(&timestamp_array).ok());
  EXPECT_TRUE(sensor_id_builder.Finish(&sensor_id_array).ok());
  EXPECT_TRUE(value_builder.Finish(&value_array).ok());
  
  auto table = arrow::Table::Make(schema, {timestamp_array, sensor_id_array, value_array});
  
  // Write the table with custom timestamp column
  EXPECT_TRUE(writer_->writeTable("/sensors/temperature", table, "custom_time"));
  
  // Close the file
  writer_->close();
  
  // Verify the file was created
  EXPECT_TRUE(std::filesystem::exists(test_file_));
  EXPECT_GT(std::filesystem::file_size(test_file_), 0);
}

// Test writing with compression
TEST_F(McapArrowWriterTest, WriteWithCompression) {
  // Open the file with compression
  ASSERT_TRUE(writer_->open(test_file_.string(), 9)); // Max compression
  
  // Create and write some sensor readings
  for (int i = 0; i < 100; ++i) {
    SensorReading reading = createTestSensorReading(1000000 + i * 1000);
    EXPECT_TRUE(writer_->writeMessage("/sensors/temperature", reading));
  }
  
  // Close the file
  writer_->close();
  
  // Verify the file was created
  EXPECT_TRUE(std::filesystem::exists(test_file_));
  EXPECT_GT(std::filesystem::file_size(test_file_), 0);
  
  // Note: We could potentially compare compressed vs uncompressed file sizes to
  // verify compression took place, but this is implementation dependent.
}

// Test writing different message types
TEST_F(McapArrowWriterTest, WriteDifferentMessageTypes) {
  // Open the file
  ASSERT_TRUE(writer_->open(test_file_.string(), 0));
  
  // Write a sensor reading
  SensorReading reading = createTestSensorReading();
  EXPECT_TRUE(writer_->writeMessage("/sensors/temperature", reading));
  
  // Write a vehicle status
  VehicleStatus status = createTestVehicleStatus();
  EXPECT_TRUE(writer_->writeMessage("/vehicle/status", status));
  
  // Close the file
  writer_->close();
  
  // Verify the file was created
  EXPECT_TRUE(std::filesystem::exists(test_file_));
  EXPECT_GT(std::filesystem::file_size(test_file_), 0);
}

// Test error handling for invalid file
TEST_F(McapArrowWriterTest, InvalidFile) {
  // Try to open a file in a nonexistent directory
  EXPECT_FALSE(writer_->open("/nonexistent/directory/test.mcap", 0));
}

// Test error handling for invalid compression level
TEST_F(McapArrowWriterTest, InvalidCompressionLevel) {
  // Try to open with an invalid compression level
  EXPECT_FALSE(writer_->open(test_file_.string(), 10)); // Valid range is 0-9
}

// Test writing a large number of messages
TEST_F(McapArrowWriterTest, WriteManyMessages) {
  // Open the file
  ASSERT_TRUE(writer_->open(test_file_.string(), 0));
  
  // Write a large number of messages
  const int num_messages = 1000;
  for (int i = 0; i < num_messages; ++i) {
    SensorReading reading = createTestSensorReading(1000000 + i * 1000);
    reading.set_value(20.0 + (i % 100) * 0.1); // Vary the values
    EXPECT_TRUE(writer_->writeMessage("/sensors/temperature", reading));
  }
  
  // Close the file
  writer_->close();
  
  // Verify the file was created
  EXPECT_TRUE(std::filesystem::exists(test_file_));
  EXPECT_GT(std::filesystem::file_size(test_file_), 0);
}

// Test writing an empty protobuf message
TEST_F(McapArrowWriterTest, WriteEmptyMessage) {
  // Open the file
  ASSERT_TRUE(writer_->open(test_file_.string(), 0));
  
  // Create an empty message (all fields at default values)
  SensorReading empty_reading;
  
  // Write the empty message
  EXPECT_TRUE(writer_->writeMessage("/sensors/temperature", empty_reading));
  
  // Close the file
  writer_->close();
  
  // Verify the file was created
  EXPECT_TRUE(std::filesystem::exists(test_file_));
  EXPECT_GT(std::filesystem::file_size(test_file_), 0);
}

}  // namespace
}  // namespace mcap_arrow 