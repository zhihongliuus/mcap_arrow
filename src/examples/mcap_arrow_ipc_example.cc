#include <iostream>
#include <string>
#include <memory>
#include <thread>
#include <chrono>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/table.h>

#include "src/lib/mcap_arrow_ipc.h"
#include "src/proto/sensor_data.pb.h"

using namespace std::chrono_literals;

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

// Helper function to print Arrow table
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

// Writer thread function
void writerThread(const std::string& shared_memory_name, const std::string& mcap_filename, int num_messages) {
    std::cout << "Writer thread starting..." << std::endl;
    
    // Create IPC writer
    mcap_arrow::McapArrowIpcWriter writer;
    if (!writer.initialize(shared_memory_name, 1024 * 1024, mcap_filename, 1)) {
        std::cerr << "Failed to initialize IPC writer" << std::endl;
        return;
    }
    
    std::cout << "Writer initialized. Writing " << num_messages << " messages..." << std::endl;
    
    // Write some sensor readings
    for (int i = 0; i < num_messages; ++i) {
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
        
        // Add a delay to simulate real-time data
        std::this_thread::sleep_for(100ms);
    }
    
    std::cout << "Writer thread finished" << std::endl;
    writer.close();
}

// Reader thread function
void readerThread(const std::string& shared_memory_name, int expected_messages) {
    std::cout << "Reader thread starting..." << std::endl;
    
    // Create IPC reader
    mcap_arrow::McapArrowIpcReader reader;
    if (!reader.initializeFromSharedMemory(shared_memory_name)) {
        std::cerr << "Failed to initialize IPC reader" << std::endl;
        return;
    }
    
    std::cout << "Reader initialized. Waiting for data..." << std::endl;
    
    int messages_received = 0;
    while (messages_received < expected_messages) {
        // Wait for new data
        if (!reader.waitForData(1000)) {
            std::cout << "Timeout waiting for data" << std::endl;
            continue;
        }
        
        // Read the latest data
        auto table = reader.readLatestData("sensor_readings");
        if (!table) {
            std::cout << "No new data available" << std::endl;
            continue;
        }
        
        messages_received += table->num_rows();
        
        std::cout << "Received " << table->num_rows() << " new messages (total: " 
                  << messages_received << "/" << expected_messages << ")" << std::endl;
        
        // Print the table
        printTable(table);
    }
    
    std::cout << "Reader thread finished" << std::endl;
    reader.close();
}

// Example of how to use the IPC reader and writer from different processes
int main(int argc, char** argv) {
    const std::string shared_memory_name = "mcap_arrow_ipc_example";
    const std::string mcap_filename = "ipc_example.mcap";
    const int num_messages = 10;
    
    // Parse command line arguments
    if (argc > 1) {
        std::string mode = argv[1];
        
        if (mode == "writer") {
            // Writer process
            writerThread(shared_memory_name, mcap_filename, num_messages);
            return 0;
        } else if (mode == "reader") {
            // Reader process
            readerThread(shared_memory_name, num_messages);
            return 0;
        }
    }
    
    // If no arguments provided, run both reader and writer in separate threads
    std::cout << "Starting IPC example with both reader and writer threads" << std::endl;
    std::cout << "To run as separate processes, use:" << std::endl;
    std::cout << "  " << argv[0] << " writer" << std::endl;
    std::cout << "  " << argv[0] << " reader" << std::endl;
    std::cout << std::endl;
    
    // Start writer thread
    std::thread writer(writerThread, shared_memory_name, mcap_filename, num_messages);
    
    // Wait a moment to ensure writer initializes first
    std::this_thread::sleep_for(500ms);
    
    // Start reader thread
    std::thread reader(readerThread, shared_memory_name, num_messages);
    
    // Wait for threads to finish
    writer.join();
    reader.join();
    
    std::cout << "IPC example completed successfully" << std::endl;
    
    return 0;
} 