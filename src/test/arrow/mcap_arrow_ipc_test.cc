#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <vector>
#include <thread>
#include <chrono>
#include <random>
#include <filesystem>
#include <fstream>
#include <iostream>

#include "src/lib/mcap_arrow_ipc.h"

namespace {

// Helper function to generate random strings
std::string generateRandomString(size_t length) {
    static const char alphanum[] =
        "0123456789"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz";
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dist(0, sizeof(alphanum) - 2);
    
    std::string result;
    result.reserve(length);
    for (size_t i = 0; i < length; ++i) {
        result += alphanum[dist(gen)];
    }
    return result;
}

// Test fixture for IpcSyncContext
class IpcSyncContextTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create a unique name for each test to avoid conflicts
        context_name_ = "test_ipc_context_" + generateRandomString(8);
        context_ = std::make_unique<mcap_arrow::IpcSyncContext>();
    }

    void TearDown() override {
        // Clean up
        std::string mutex_path = "/dev/shm/" + context_name_ + "_mutex";
        std::string condvar_path = "/dev/shm/" + context_name_ + "_condvar";
        std::string counter_path = "/dev/shm/" + context_name_ + "_counter";
        
        if (std::filesystem::exists(mutex_path)) {
            std::filesystem::remove(mutex_path);
        }
        if (std::filesystem::exists(condvar_path)) {
            std::filesystem::remove(condvar_path);
        }
        if (std::filesystem::exists(counter_path)) {
            std::filesystem::remove(counter_path);
        }
    }

    std::unique_ptr<mcap_arrow::IpcSyncContext> context_;
    std::string context_name_;
};

// Test fixture for McapArrowIpcWriter/Reader
class McapArrowIpcTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create unique names for shared memory and MCAP file
        shared_memory_name_ = "test_shared_memory_" + generateRandomString(8);
        mcap_file_ = std::filesystem::temp_directory_path() / ("test_mcap_" + generateRandomString(8) + ".mcap");
        
        writer_ = std::make_unique<mcap_arrow::McapArrowIpcWriter>();
        reader_ = std::make_unique<mcap_arrow::McapArrowIpcReader>();
    }

    void TearDown() override {
        // Close and clean up
        if (writer_) writer_->close();
        if (reader_) reader_->close();
        
        // Clean up shared memory file
        std::string shm_path = "/dev/shm/" + shared_memory_name_;
        if (std::filesystem::exists(shm_path)) {
            std::filesystem::remove(shm_path);
        }
        
        // Clean up MCAP file
        if (std::filesystem::exists(mcap_file_)) {
            std::filesystem::remove(mcap_file_);
        }
    }

    std::unique_ptr<mcap_arrow::McapArrowIpcWriter> writer_;
    std::unique_ptr<mcap_arrow::McapArrowIpcReader> reader_;
    std::string shared_memory_name_;
    std::filesystem::path mcap_file_;
};

// Helper function to create a simple Arrow table
std::shared_ptr<arrow::Table> createTestTable() {
    // Create schema
    auto schema = arrow::schema({
        arrow::field("timestamp", arrow::int64()),
        arrow::field("value", arrow::float64())
    });
    
    // Create arrays
    arrow::Int64Builder timestamp_builder;
    arrow::DoubleBuilder value_builder;
    
    for (int i = 0; i < 10; ++i) {
        EXPECT_TRUE(timestamp_builder.Append(1000 + i).ok());
        EXPECT_TRUE(value_builder.Append(i * 1.5).ok());
    }
    
    std::shared_ptr<arrow::Array> timestamp_array;
    std::shared_ptr<arrow::Array> value_array;
    
    EXPECT_TRUE(timestamp_builder.Finish(&timestamp_array).ok());
    EXPECT_TRUE(value_builder.Finish(&value_array).ok());
    
    // Create table
    return arrow::Table::Make(schema, {timestamp_array, value_array});
}

} // namespace

// Tests for IpcSyncContext

TEST_F(IpcSyncContextTest, Initialize) {
    EXPECT_TRUE(context_->initialize(context_name_));
}

TEST_F(IpcSyncContextTest, InitializeMultiple) {
    ASSERT_TRUE(context_->initialize(context_name_));
    
    // Create another context with the same name
    mcap_arrow::IpcSyncContext context2;
    EXPECT_TRUE(context2.initialize(context_name_));
}

TEST_F(IpcSyncContextTest, Counter) {
    ASSERT_TRUE(context_->initialize(context_name_));
    
    // Initial counter should be 0
    EXPECT_EQ(context_->getCounter(), 0);
    
    // Increment counter
    EXPECT_EQ(context_->incrementCounter(), 1);
    EXPECT_EQ(context_->getCounter(), 1);
    
    // Increment again
    EXPECT_EQ(context_->incrementCounter(), 2);
    EXPECT_EQ(context_->getCounter(), 2);
}

TEST_F(IpcSyncContextTest, Lock) {
    ASSERT_TRUE(context_->initialize(context_name_));
    
    // Test lock and unlock
    context_->lock();
    context_->unlock();
    
    // Test try_lock
    EXPECT_TRUE(context_->try_lock());
    context_->unlock();
}

TEST_F(IpcSyncContextTest, Notification) {
    ASSERT_TRUE(context_->initialize(context_name_));
    
    bool notified = false;
    
    // Start a thread that waits for notification
    std::thread waiter([&]() {
        // Wait with timeout (should timeout)
        EXPECT_FALSE(context_->wait(100));
        
        // Wait for actual notification
        if (context_->wait(5000)) {  // 5-second timeout
            notified = true;
        }
    });
    
    // Sleep a bit to ensure the waiter is waiting
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    
    // Notify
    context_->notify();
    
    // Wait for the waiter thread to finish
    waiter.join();
    
    // Verify notification was received
    EXPECT_TRUE(notified);
}

// Tests for McapArrowIpcWriter and McapArrowIpcReader

TEST_F(McapArrowIpcTest, InitializeWriter) {
    // Initialize writer with shared memory only
    EXPECT_TRUE(writer_->initialize(shared_memory_name_, 1024 * 1024));
    
    // Verify shared memory info
    mcap_arrow::SharedMemoryInfo info = writer_->getSharedMemoryInfo();
    EXPECT_EQ(info.name, shared_memory_name_);
    EXPECT_EQ(info.size, 1024 * 1024);
}

TEST_F(McapArrowIpcTest, InitializeWriterWithMcap) {
    // Initialize writer with both shared memory and MCAP file
    EXPECT_TRUE(writer_->initialize(shared_memory_name_, 1024 * 1024, mcap_file_.string()));
    
    // Writer should have created the MCAP file
    EXPECT_TRUE(std::filesystem::exists(mcap_file_));
}

TEST_F(McapArrowIpcTest, InitializeReader) {
    // Initialize writer first to create shared memory
    ASSERT_TRUE(writer_->initialize(shared_memory_name_, 1024 * 1024));
    
    // Initialize reader from shared memory
    EXPECT_TRUE(reader_->initializeFromSharedMemory(shared_memory_name_));
}

TEST_F(McapArrowIpcTest, WriteAndReadTable) {
    // Initialize writer and reader
    ASSERT_TRUE(writer_->initialize(shared_memory_name_, 1024 * 1024));
    ASSERT_TRUE(reader_->initializeFromSharedMemory(shared_memory_name_));
    
    // Create test table
    auto table = createTestTable();
    
    // Write table to shared memory
    EXPECT_TRUE(writer_->writeTable("test_topic", table, "timestamp"));
    
    // Wait for data with timeout
    EXPECT_TRUE(reader_->waitForData(1000));
}

TEST_F(McapArrowIpcTest, ResizeSharedMemory) {
    // Initialize writer with small initial size
    ASSERT_TRUE(writer_->initialize(shared_memory_name_, 4096));
    
    // Resize to larger size
    EXPECT_TRUE(writer_->resizeSharedMemory(8192));
    
    // Verify new size
    mcap_arrow::SharedMemoryInfo info = writer_->getSharedMemoryInfo();
    EXPECT_EQ(info.size, 8192);
}

// Integration test with multiple threads

TEST(McapArrowIpcIntegrationTest, WriterReaderThreads) {
    // Setup
    std::string shared_memory_name = "integration_test_" + generateRandomString(8);
    auto temp_dir = std::filesystem::temp_directory_path();
    auto mcap_file = temp_dir / ("integration_" + generateRandomString(8) + ".mcap");
    
    // Synchronization primitives
    std::atomic<bool> writer_ready(false);
    std::atomic<bool> reader_ready(false);
    std::atomic<bool> test_complete(false);
    std::atomic<int> tables_written(0);
    std::atomic<int> tables_read(0);
    
    // Start writer thread
    std::thread writer_thread([&]() {
        mcap_arrow::McapArrowIpcWriter writer;
        ASSERT_TRUE(writer.initialize(shared_memory_name, 1024 * 1024, mcap_file.string()));
        
        writer_ready = true;
        
        // Wait for reader to be ready
        while (!reader_ready) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        
        // Write 5 tables
        for (int i = 0; i < 5; ++i) {
            // Create schema
            auto schema = arrow::schema({
                arrow::field("timestamp", arrow::int64()),
                arrow::field("value", arrow::float64()),
                arrow::field("index", arrow::int32())
            });
            
            // Create arrays
            arrow::Int64Builder timestamp_builder;
            arrow::DoubleBuilder value_builder;
            arrow::Int32Builder index_builder;
            
            for (int j = 0; j < 10; ++j) {
                EXPECT_TRUE(timestamp_builder.Append(1000 + i * 100 + j).ok());
                EXPECT_TRUE(value_builder.Append(i * 10.5 + j * 1.5).ok());
                EXPECT_TRUE(index_builder.Append(i * 10 + j).ok());
            }
            
            std::shared_ptr<arrow::Array> timestamp_array;
            std::shared_ptr<arrow::Array> value_array;
            std::shared_ptr<arrow::Array> index_array;
            
            EXPECT_TRUE(timestamp_builder.Finish(&timestamp_array).ok());
            EXPECT_TRUE(value_builder.Finish(&value_array).ok());
            EXPECT_TRUE(index_builder.Finish(&index_array).ok());
            
            // Create table
            auto table = arrow::Table::Make(schema, {timestamp_array, value_array, index_array});
            
            // Write table
            EXPECT_TRUE(writer.writeTable("test_topic", table, "timestamp"));
            
            tables_written++;
            
            // Sleep a bit to simulate real-world timing
            std::this_thread::sleep_for(std::chrono::milliseconds(50));
        }
        
        // Wait for test to complete
        while (!test_complete) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        
        writer.close();
        
        // Clean up shared memory
        std::string shm_path = "/dev/shm/" + shared_memory_name;
        if (std::filesystem::exists(shm_path)) {
            std::filesystem::remove(shm_path);
        }
        
        // Clean up MCAP file
        if (std::filesystem::exists(mcap_file)) {
            std::filesystem::remove(mcap_file);
        }
    });
    
    // Start reader thread
    std::thread reader_thread([&]() {
        mcap_arrow::McapArrowIpcReader reader;
        
        // Wait for writer to be ready
        while (!writer_ready) {
            std::this_thread::sleep_for(std::chrono::milliseconds(10));
        }
        
        ASSERT_TRUE(reader.initializeFromSharedMemory(shared_memory_name));
        
        reader_ready = true;
        
        // Read tables
        while (tables_read < 5) {
            if (reader.waitForData(100)) {
                tables_read++;
            }
        }
        
        reader.close();
    });
    
    // Wait for both threads to finish their work
    writer_thread.detach();  // We'll explicitly join after setting test_complete
    reader_thread.join();
    
    // Signal completion and join writer thread
    test_complete = true;
    std::this_thread::sleep_for(std::chrono::milliseconds(500));  // Give writer time to clean up
    
    // Verify results
    EXPECT_EQ(tables_written, 5);
    EXPECT_EQ(tables_read, 5);
}

// Main
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
} 