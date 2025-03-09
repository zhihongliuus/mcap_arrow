#include <gtest/gtest.h>
#include <fstream>
#include <string>
#include <vector>
#include <cstdio>

// Helper functions for tests
std::string createTestFile() {
    std::string filename = "test_mcap_arrow_minimal.txt";
    std::ofstream outfile(filename);
    if (outfile.is_open()) {
        outfile << "timestamp,sensor_id,value,unit,x,y,z\n";
        outfile << "1000000000,sensor_0,20.0,celsius,0.0,0.0,0.0\n";
        outfile << "1100000000,sensor_1,20.5,celsius,0.1,0.2,0.3\n";
        outfile << "1200000000,sensor_2,21.0,celsius,0.2,0.4,0.6\n";
        outfile << "1300000000,sensor_0,21.5,celsius,0.3,0.6,0.9\n";
        outfile << "1400000000,sensor_1,22.0,celsius,0.4,0.8,1.2\n";
        outfile.close();
    }
    return filename;
}

std::vector<std::string> readFileLines(const std::string& filename) {
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

void cleanupTestFile(const std::string& filename) {
    std::remove(filename.c_str());
}

// Test file creation and reading
TEST(McapArrowMinimal, FileCreation) {
    // Create a test file
    std::string filename = createTestFile();
    
    // Verify the file exists
    std::ifstream infile(filename);
    EXPECT_TRUE(infile.good()) << "Test file should exist";
    infile.close();
    
    // Clean up
    cleanupTestFile(filename);
}

// Test data content
TEST(McapArrowMinimal, FileContent) {
    // Create a test file
    std::string filename = createTestFile();
    
    // Read file content
    auto lines = readFileLines(filename);
    
    // Verify the number of lines
    EXPECT_EQ(lines.size(), 6) << "Expected 6 lines in the file";
    
    // Verify header
    EXPECT_EQ(lines[0], "timestamp,sensor_id,value,unit,x,y,z") 
        << "Header line should match expected format";
    
    // Verify a few data points
    EXPECT_EQ(lines[1], "1000000000,sensor_0,20.0,celsius,0.0,0.0,0.0")
        << "First data line should match expected format";
    
    EXPECT_EQ(lines[3], "1200000000,sensor_2,21.0,celsius,0.2,0.4,0.6")
        << "Third data line should match expected format";
        
    // Clean up
    cleanupTestFile(filename);
} 