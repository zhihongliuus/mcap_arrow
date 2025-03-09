#include <gtest/gtest.h>
#include <fstream>
#include <string>
#include <vector>
#include <cstdio>

// Helper functions for tests
std::string createTestDataFile() {
    std::string filename = "test_mcap_arrow.txt";
    
    // Write to the file
    std::ofstream outfile(filename);
    if (outfile.is_open()) {
        // Header row
        outfile << "timestamp,topic,message_type,data\n";
        
        // Sample MCAP data entries
        outfile << "1000000000,/sensors/temperature,SensorReading,{\"value\":22.5,\"unit\":\"celsius\"}\n";
        outfile << "1100000000,/sensors/humidity,SensorReading,{\"value\":45.2,\"unit\":\"percent\"}\n";
        outfile << "1200000000,/vehicle/status,VehicleStatus,{\"speed\":65.5,\"fuel_level\":0.75}\n";
        outfile << "1300000000,/sensors/temperature,SensorReading,{\"value\":23.1,\"unit\":\"celsius\"}\n";
        outfile << "1400000000,/sensors/humidity,SensorReading,{\"value\":44.8,\"unit\":\"percent\"}\n";
        
        outfile.close();
    }
    
    return filename;
}

std::vector<std::string> readFileData(const std::string& filename) {
    std::vector<std::string> lines;
    std::ifstream infile(filename);
    if (infile.is_open()) {
        std::string line;
        while (std::getline(infile, line)) {
            lines.push_back(line);
        }
        infile.close();
    }
    return lines;
}

void deleteTestFile(const std::string& filename) {
    std::remove(filename.c_str());
}

// Test file creation
TEST(McapArrow, FileCreation) {
    std::string filename = createTestDataFile();
    
    std::ifstream infile(filename);
    EXPECT_TRUE(infile.good()) << "Test file should exist and be readable";
    infile.close();
    
    deleteTestFile(filename);
}

// Test file content validation
TEST(McapArrow, FileContent) {
    std::string filename = createTestDataFile();
    
    auto lines = readFileData(filename);
    
    // Check number of lines
    EXPECT_EQ(lines.size(), 6) << "File should contain 6 lines (header + 5 data rows)";
    
    // Check header format
    EXPECT_EQ(lines[0], "timestamp,topic,message_type,data") 
        << "Header should match expected format";
    
    // Check a few data entries
    EXPECT_EQ(lines[1], "1000000000,/sensors/temperature,SensorReading,{\"value\":22.5,\"unit\":\"celsius\"}")
        << "First data line should match expected format";
    
    EXPECT_EQ(lines[3], "1200000000,/vehicle/status,VehicleStatus,{\"speed\":65.5,\"fuel_level\":0.75}")
        << "Third data line should match expected format";
        
    deleteTestFile(filename);
}

// Test filtering by timestamp
TEST(McapArrow, FilterByTimestamp) {
    std::string filename = createTestDataFile();
    
    auto lines = readFileData(filename);
    
    // Filter lines with timestamp > 1200000000
    std::vector<std::string> filtered;
    for (size_t i = 1; i < lines.size(); i++) { // Skip header
        std::string timestampStr = lines[i].substr(0, lines[i].find(','));
        int64_t timestamp = std::stoll(timestampStr);
        
        if (timestamp > 1200000000) {
            filtered.push_back(lines[i]);
        }
    }
    
    // Verify filter results
    EXPECT_EQ(filtered.size(), 2) << "Should have 2 entries with timestamp > 1200000000";
    
    deleteTestFile(filename);
}

// Test filtering by topic
TEST(McapArrow, FilterByTopic) {
    std::string filename = createTestDataFile();
    
    auto lines = readFileData(filename);
    
    // Filter lines with topic = /sensors/temperature
    std::vector<std::string> filtered;
    for (size_t i = 1; i < lines.size(); i++) { // Skip header
        if (lines[i].find("/sensors/temperature") != std::string::npos) {
            filtered.push_back(lines[i]);
        }
    }
    
    // Verify filter results
    EXPECT_EQ(filtered.size(), 2) << "Should have 2 entries with topic /sensors/temperature";
    
    deleteTestFile(filename);
} 