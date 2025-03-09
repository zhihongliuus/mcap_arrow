#include <iostream>
#include <string>
#include <memory>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/table.h>

#include "src/lib/mcap_arrow_reader.h"
#include "src/lib/mcap_arrow_writer.h"
#include "src/proto/sensor_data.pb.h"

// Helper function to create a SensorReading protobuf message
mcap_arrow::SensorReading createSensorReading(
    int64_t timestamp,
    const std::string& sensor_id,
    double value,
    const std::string& unit,
    double x, double y, double z) {
    
    mcap_arrow::SensorReading reading;
    reading.set_timestamp(timestamp);
    reading.set_sensor_id(sensor_id);
    reading.set_value(value);
    reading.set_unit(unit);
    
    auto* location = reading.mutable_location();
    location->set_x(x);
    location->set_y(y);
    location->set_z(z);
    
    return reading;
}

// Helper function to create a VehicleStatus protobuf message
mcap_arrow::VehicleStatus createVehicleStatus(
    int64_t timestamp,
    const std::string& vehicle_id,
    double speed,
    double heading,
    double x, double y, double z) {
    
    mcap_arrow::VehicleStatus status;
    status.set_timestamp(timestamp);
    status.set_vehicle_id(vehicle_id);
    status.set_speed(speed);
    status.set_heading(heading);
    
    auto* position = status.mutable_position();
    position->set_x(x);
    position->set_y(y);
    position->set_z(z);
    
    // Add some active systems
    status.add_active_systems("engine");
    status.add_active_systems("brakes");
    status.add_active_systems("steering");
    
    // Add system status
    (*status.mutable_system_status())["engine"] = 0.95;
    (*status.mutable_system_status())["brakes"] = 0.87;
    (*status.mutable_system_status())["steering"] = 0.92;
    
    return status;
}

// Helper function to print arrow table
void printTable(const std::shared_ptr<arrow::Table>& table) {
    if (!table) {
        std::cout << "Table is empty" << std::endl;
        return;
    }
    
    std::cout << "Table schema: " << table->schema()->ToString() << std::endl;
    std::cout << "Number of rows: " << table->num_rows() << std::endl;
    std::cout << "Number of columns: " << table->num_columns() << std::endl;
    std::cout << std::endl;
    
    // Print column names
    std::cout << "Columns: ";
    for (int i = 0; i < table->num_columns(); ++i) {
        std::cout << table->field(i)->name() << " ";
    }
    std::cout << std::endl;
    
    // Print first 10 rows (or all if less than 10)
    int num_rows_to_print = std::min(10, static_cast<int>(table->num_rows()));
    for (int row = 0; row < num_rows_to_print; ++row) {
        std::cout << "Row " << row << ": ";
        for (int col = 0; col < table->num_columns(); ++col) {
            // Print a simple representation of the value
            auto array = table->column(col)->chunk(0);
            switch (array->type_id()) {
                case arrow::Type::INT64: {
                    auto typed_array = std::static_pointer_cast<arrow::Int64Array>(array);
                    std::cout << typed_array->Value(row) << " ";
                    break;
                }
                case arrow::Type::DOUBLE: {
                    auto typed_array = std::static_pointer_cast<arrow::DoubleArray>(array);
                    std::cout << typed_array->Value(row) << " ";
                    break;
                }
                case arrow::Type::STRING: {
                    auto typed_array = std::static_pointer_cast<arrow::StringArray>(array);
                    std::cout << typed_array->GetString(row) << " ";
                    break;
                }
                default:
                    std::cout << "? ";
                    break;
            }
        }
        std::cout << std::endl;
    }
    std::cout << std::endl;
}

int main(int argc, char** argv) {
    const std::string filename = "sensor_data.mcap";
    
    // Create MCAP writer
    mcap_arrow::McapArrowWriter writer;
    if (!writer.open(filename, 1)) {
        std::cerr << "Failed to open MCAP file for writing" << std::endl;
        return 1;
    }
    
    std::cout << "Writing sensor readings to MCAP file..." << std::endl;
    
    // Write some sensor readings
    for (int i = 0; i < 10; ++i) {
        int64_t timestamp = 1000000000 + i * 100000000;  // Nanoseconds
        auto reading = createSensorReading(
            timestamp,
            "sensor_" + std::to_string(i % 3),
            20.0 + i * 0.5,
            "celsius",
            0.1 * i, 0.2 * i, 0.3 * i
        );
        
        if (!writer.writeMessage("sensor_readings", reading, timestamp)) {
            std::cerr << "Failed to write sensor reading " << i << std::endl;
        }
    }
    
    std::cout << "Writing vehicle status messages to MCAP file..." << std::endl;
    
    // Write some vehicle status messages
    for (int i = 0; i < 5; ++i) {
        int64_t timestamp = 1000000000 + i * 200000000;  // Nanoseconds
        auto status = createVehicleStatus(
            timestamp,
            "vehicle_" + std::to_string(i % 2),
            60.0 + i * 2.0,
            45.0 + i * 5.0,
            10.0 + i * 1.0, 20.0 + i * 1.0, 0.0
        );
        
        if (!writer.writeMessage("vehicle_status", status, timestamp)) {
            std::cerr << "Failed to write vehicle status " << i << std::endl;
        }
    }
    
    writer.close();
    std::cout << "MCAP file written successfully" << std::endl;
    
    // Create MCAP reader
    mcap_arrow::McapArrowReader reader;
    if (!reader.open(filename)) {
        std::cerr << "Failed to open MCAP file for reading" << std::endl;
        return 1;
    }
    
    // Build the index
    if (!reader.buildIndex()) {
        std::cerr << "Failed to build index" << std::endl;
        return 1;
    }
    
    // Get all topics
    auto topics = reader.getTopics();
    std::cout << "Topics in MCAP file:" << std::endl;
    for (const auto& topic : topics) {
        std::cout << "  " << topic << std::endl;
    }
    std::cout << std::endl;
    
    // Query sensor readings
    std::cout << "Querying sensor readings..." << std::endl;
    auto sensor_readings = reader.query("sensor_readings");
    printTable(sensor_readings);
    
    // Query vehicle status
    std::cout << "Querying vehicle status..." << std::endl;
    auto vehicle_status = reader.query("vehicle_status");
    printTable(vehicle_status);
    
    // Query with time range
    std::cout << "Querying sensor readings with time range..." << std::endl;
    auto readings_time_range = reader.query("sensor_readings", 1000000000, 1300000000);
    printTable(readings_time_range);
    
    // Query with filter
    std::cout << "Querying sensor readings with filter..." << std::endl;
    std::unordered_map<std::string, std::string> filter = {
        {"sensor_id", "sensor_0"}
    };
    auto filtered_readings = reader.query("sensor_readings", 0, std::numeric_limits<int64_t>::max(), filter);
    printTable(filtered_readings);
    
    reader.close();
    std::cout << "Done" << std::endl;
    
    return 0;
} 