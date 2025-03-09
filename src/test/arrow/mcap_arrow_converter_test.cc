#include <gtest/gtest.h>
#include <arrow/api.h>
#include <memory>
#include <string>
#include <vector>

#include "src/lib/mcap_arrow_converter.h"
#include "src/proto/sensor_data.pb.h"

namespace {

class McapArrowConverterTest : public ::testing::Test {
 protected:
  void SetUp() override {
    converter_ = std::make_unique<mcap_arrow::McapArrowConverter>();
  }

  void TearDown() override {
    converter_.reset();
  }

  std::unique_ptr<mcap_arrow::McapArrowConverter> converter_;
};

// Test converting a protobuf message to an Arrow record batch
TEST_F(McapArrowConverterTest, ConvertProtoToArrow) {
  // Create a sensor reading message
  sensor_msgs::SensorReading reading;
  reading.set_timestamp(1234567890);
  reading.set_sensor_id(42);
  reading.set_value(123.456);
  reading.mutable_metadata()->insert({"unit", "meters"});

  // Convert to Arrow
  auto record_batch = converter_->ProtoToRecordBatch(reading);

  // Verify record batch contents
  ASSERT_NE(record_batch, nullptr);
  ASSERT_EQ(record_batch->num_rows(), 1);
  ASSERT_EQ(record_batch->num_columns(), 4);

  // Verify schema
  auto schema = record_batch->schema();
  ASSERT_EQ(schema->num_fields(), 4);
  ASSERT_EQ(schema->field(0)->name(), "timestamp");
  ASSERT_EQ(schema->field(1)->name(), "sensor_id");
  ASSERT_EQ(schema->field(2)->name(), "value");
  ASSERT_EQ(schema->field(3)->name(), "metadata");

  // Verify data
  auto timestamp_array = std::static_pointer_cast<arrow::Int64Array>(record_batch->column(0));
  ASSERT_EQ(timestamp_array->Value(0), 1234567890);

  auto sensor_id_array = std::static_pointer_cast<arrow::Int32Array>(record_batch->column(1));
  ASSERT_EQ(sensor_id_array->Value(0), 42);

  auto value_array = std::static_pointer_cast<arrow::DoubleArray>(record_batch->column(2));
  ASSERT_DOUBLE_EQ(value_array->Value(0), 123.456);

  // Check metadata (this assumes metadata is stored as a list or map array)
  auto metadata_array = std::static_pointer_cast<arrow::MapArray>(record_batch->column(3));
  ASSERT_EQ(metadata_array->length(), 1); // One row
  ASSERT_EQ(metadata_array->value_length(0), 1); // One key-value pair
}

// Test converting a complex protobuf message to an Arrow record batch
TEST_F(McapArrowConverterTest, ConvertComplexProtoToArrow) {
  // Create a vehicle status message
  sensor_msgs::VehicleStatus status;
  status.set_timestamp(1234567890);
  status.set_vehicle_id("ABC123");
  status.set_speed(65.5);
  status.set_fuel_level(0.75);
  status.set_engine_temperature(90.2);
  status.set_engine_running(true);

  // Convert to Arrow
  auto record_batch = converter_->ProtoToRecordBatch(status);

  // Verify record batch contents
  ASSERT_NE(record_batch, nullptr);
  ASSERT_EQ(record_batch->num_rows(), 1);
  ASSERT_EQ(record_batch->num_columns(), 6);

  // Verify schema
  auto schema = record_batch->schema();
  ASSERT_EQ(schema->num_fields(), 6);
  ASSERT_EQ(schema->field(0)->name(), "timestamp");
  ASSERT_EQ(schema->field(1)->name(), "vehicle_id");
  ASSERT_EQ(schema->field(2)->name(), "speed");
  ASSERT_EQ(schema->field(3)->name(), "fuel_level");
  ASSERT_EQ(schema->field(4)->name(), "engine_temperature");
  ASSERT_EQ(schema->field(5)->name(), "engine_running");

  // Verify data
  auto timestamp_array = std::static_pointer_cast<arrow::Int64Array>(record_batch->column(0));
  ASSERT_EQ(timestamp_array->Value(0), 1234567890);

  auto vehicle_id_array = std::static_pointer_cast<arrow::StringArray>(record_batch->column(1));
  ASSERT_EQ(vehicle_id_array->GetString(0), "ABC123");

  auto speed_array = std::static_pointer_cast<arrow::DoubleArray>(record_batch->column(2));
  ASSERT_DOUBLE_EQ(speed_array->Value(0), 65.5);

  auto fuel_level_array = std::static_pointer_cast<arrow::DoubleArray>(record_batch->column(3));
  ASSERT_DOUBLE_EQ(fuel_level_array->Value(0), 0.75);

  auto engine_temp_array = std::static_pointer_cast<arrow::DoubleArray>(record_batch->column(4));
  ASSERT_DOUBLE_EQ(engine_temp_array->Value(0), 90.2);

  auto engine_running_array = std::static_pointer_cast<arrow::BooleanArray>(record_batch->column(5));
  ASSERT_EQ(engine_running_array->Value(0), true);
}

// Test converting an Arrow record batch to a protobuf message
TEST_F(McapArrowConverterTest, ConvertArrowToProto) {
  // Create Arrow schema
  auto schema = arrow::Schema::Make({
      arrow::field("timestamp", arrow::int64()),
      arrow::field("sensor_id", arrow::int32()),
      arrow::field("value", arrow::float64()),
      arrow::field("metadata", arrow::map(arrow::utf8(), arrow::utf8()))
  });

  // Create Arrow arrays
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  
  // Timestamp array
  arrow::Int64Builder timestamp_builder;
  ASSERT_OK(timestamp_builder.Append(1234567890));
  std::shared_ptr<arrow::Array> timestamp_array;
  ASSERT_OK(timestamp_builder.Finish(&timestamp_array));
  arrays.push_back(timestamp_array);

  // Sensor ID array
  arrow::Int32Builder sensor_id_builder;
  ASSERT_OK(sensor_id_builder.Append(42));
  std::shared_ptr<arrow::Array> sensor_id_array;
  ASSERT_OK(sensor_id_builder.Finish(&sensor_id_array));
  arrays.push_back(sensor_id_array);

  // Value array
  arrow::DoubleBuilder value_builder;
  ASSERT_OK(value_builder.Append(123.456));
  std::shared_ptr<arrow::Array> value_array;
  ASSERT_OK(value_builder.Finish(&value_array));
  arrays.push_back(value_array);

  // Metadata array (simplified for test)
  std::shared_ptr<arrow::Array> metadata_array = arrow::MakeNullArray(arrow::map(arrow::utf8(), arrow::utf8()), 1);
  arrays.push_back(metadata_array);

  // Create record batch
  auto record_batch = arrow::RecordBatch::Make(schema, 1, arrays);

  // Convert to protobuf
  sensor_msgs::SensorReading reading;
  bool success = converter_->RecordBatchToProto(*record_batch, 0, &reading);

  // Verify conversion success
  ASSERT_TRUE(success);

  // Verify protobuf contents
  ASSERT_EQ(reading.timestamp(), 1234567890);
  ASSERT_EQ(reading.sensor_id(), 42);
  ASSERT_DOUBLE_EQ(reading.value(), 123.456);
}

// Test error handling for invalid record batch to proto conversion
TEST_F(McapArrowConverterTest, InvalidArrowToProtoConversion) {
  // Create schema with missing required fields
  auto schema = arrow::Schema::Make({
      arrow::field("some_other_field", arrow::int64()),
  });

  // Create arrays
  std::vector<std::shared_ptr<arrow::Array>> arrays;
  arrow::Int64Builder builder;
  ASSERT_OK(builder.Append(100));
  std::shared_ptr<arrow::Array> array;
  ASSERT_OK(builder.Finish(&array));
  arrays.push_back(array);

  // Create record batch
  auto record_batch = arrow::RecordBatch::Make(schema, 1, arrays);

  // Try to convert to protobuf
  sensor_msgs::SensorReading reading;
  bool success = converter_->RecordBatchToProto(*record_batch, 0, &reading);

  // Verify conversion fails
  ASSERT_FALSE(success);
}

// Test creating an Arrow schema from a protobuf message
TEST_F(McapArrowConverterTest, CreateSchemaFromProto) {
  // Create a sensor reading message
  sensor_msgs::SensorReading reading;
  reading.set_timestamp(1234567890);
  reading.set_sensor_id(42);
  reading.set_value(123.456);
  reading.mutable_metadata()->insert({"unit", "meters"});

  // Convert to schema by first creating a record batch
  auto record_batch = converter_->ProtoToRecordBatch(reading);
  auto schema = record_batch->schema();

  // Verify schema
  ASSERT_EQ(schema->num_fields(), 4);
  ASSERT_EQ(schema->field(0)->name(), "timestamp");
  ASSERT_EQ(schema->field(0)->type()->id(), arrow::Type::INT64);
  ASSERT_EQ(schema->field(1)->name(), "sensor_id");
  ASSERT_EQ(schema->field(1)->type()->id(), arrow::Type::INT32);
  ASSERT_EQ(schema->field(2)->name(), "value");
  ASSERT_EQ(schema->field(2)->type()->id(), arrow::Type::DOUBLE);
  ASSERT_EQ(schema->field(3)->name(), "metadata");
  ASSERT_TRUE(schema->field(3)->type()->id() == arrow::Type::MAP ||
              schema->field(3)->type()->id() == arrow::Type::STRUCT);
}

// Test converting an empty protobuf message
TEST_F(McapArrowConverterTest, EmptyProtoToArrow) {
  // Create an empty sensor reading message (all fields at default values)
  sensor_msgs::SensorReading reading;
  
  // Convert to Arrow
  auto record_batch = converter_->ProtoToRecordBatch(reading);
  
  // Verify record batch contents
  ASSERT_NE(record_batch, nullptr);
  ASSERT_EQ(record_batch->num_rows(), 1);
  
  // Verify data - should be default values
  auto timestamp_array = std::static_pointer_cast<arrow::Int64Array>(record_batch->column(0));
  ASSERT_EQ(timestamp_array->Value(0), 0);
  
  auto sensor_id_array = std::static_pointer_cast<arrow::Int32Array>(record_batch->column(1));
  ASSERT_EQ(sensor_id_array->Value(0), 0);
  
  auto value_array = std::static_pointer_cast<arrow::DoubleArray>(record_batch->column(2));
  ASSERT_DOUBLE_EQ(value_array->Value(0), 0.0);
}

// Test converting a protobuf message with nested fields
TEST_F(McapArrowConverterTest, NestedFieldsProtoToArrow) {
  // Create a sensor reading with location data
  sensor_msgs::SensorReading reading;
  reading.set_timestamp(1234567890);
  reading.set_sensor_id(42);
  reading.set_value(123.456);
  
  // Add location data
  auto* location = reading.mutable_location();
  location->set_x(10.1);
  location->set_y(20.2);
  location->set_z(30.3);
  
  // Convert to Arrow
  auto record_batch = converter_->ProtoToRecordBatch(reading);
  
  // Verify record batch contents
  ASSERT_NE(record_batch, nullptr);
  
  // Get schema and check for nested fields
  auto schema = record_batch->schema();
  ASSERT_NE(schema->GetFieldByName("location"), nullptr);
  
  // Check either nested struct approach or flattened fields
  if (schema->GetFieldByName("location.x") != nullptr) {
    // Flattened fields approach
    ASSERT_NE(schema->GetFieldByName("location.x"), nullptr);
    ASSERT_NE(schema->GetFieldByName("location.y"), nullptr);
    ASSERT_NE(schema->GetFieldByName("location.z"), nullptr);
    
    // Get field indices
    int x_idx = schema->GetFieldIndex("location.x");
    int y_idx = schema->GetFieldIndex("location.y");
    int z_idx = schema->GetFieldIndex("location.z");
    
    // Verify data
    auto x_array = std::static_pointer_cast<arrow::DoubleArray>(record_batch->column(x_idx));
    auto y_array = std::static_pointer_cast<arrow::DoubleArray>(record_batch->column(y_idx));
    auto z_array = std::static_pointer_cast<arrow::DoubleArray>(record_batch->column(z_idx));
    
    ASSERT_DOUBLE_EQ(x_array->Value(0), 10.1);
    ASSERT_DOUBLE_EQ(y_array->Value(0), 20.2);
    ASSERT_DOUBLE_EQ(z_array->Value(0), 30.3);
  } else {
    // Struct approach
    int loc_idx = schema->GetFieldIndex("location");
    ASSERT_GE(loc_idx, 0);
    
    // Check the type is a struct
    auto struct_type = std::static_pointer_cast<arrow::StructType>(schema->field(loc_idx)->type());
    ASSERT_NE(struct_type, nullptr);
    
    // Check struct has x, y, z fields
    ASSERT_NE(struct_type->GetFieldByName("x"), nullptr);
    ASSERT_NE(struct_type->GetFieldByName("y"), nullptr);
    ASSERT_NE(struct_type->GetFieldByName("z"), nullptr);
    
    // Verify data (struct extraction is more complex, simplified for this test)
    auto struct_array = std::static_pointer_cast<arrow::StructArray>(record_batch->column(loc_idx));
    ASSERT_NE(struct_array, nullptr);
  }
}

}  // namespace 