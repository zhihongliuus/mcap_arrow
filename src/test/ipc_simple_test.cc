#include <gtest/gtest.h>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <vector>
#include <chrono>
#include <memory>
#include <string>
#include <cstring> // For memcpy

// Define a simple simulation of IPC functionality 
// that demonstrates the key concepts without dependencies

class SharedMemorySimulation {
private:
    std::vector<uint8_t> memory_region_;
    std::mutex mutex_;
    std::condition_variable cv_;
    std::atomic<int> counter_{0};
    
public:
    SharedMemorySimulation(size_t size) : memory_region_(size, 0) {}
    
    // Lock and synchronization operations
    void lock() { mutex_.lock(); }
    void unlock() { mutex_.unlock(); }
    bool try_lock() { return mutex_.try_lock(); }
    
    void notify() {
        std::lock_guard<std::mutex> lock(mutex_);
        cv_.notify_one();
    }
    
    bool wait(int timeout_ms) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (timeout_ms <= 0) {
            cv_.wait(lock);
            return true;
        } else {
            return cv_.wait_for(lock, std::chrono::milliseconds(timeout_ms)) 
                != std::cv_status::timeout;
        }
    }
    
    // Counter operations
    int increment_counter() { return counter_.fetch_add(1) + 1; }
    int get_counter() const { return counter_.load(); }
    
    // Memory operations
    bool write(const void* data, size_t size, size_t offset = 0) {
        if (offset + size > memory_region_.size()) {
            return false;
        }
        memcpy(memory_region_.data() + offset, data, size);
        return true;
    }
    
    bool read(void* dest, size_t size, size_t offset = 0) const {
        if (offset + size > memory_region_.size()) {
            return false;
        }
        memcpy(dest, memory_region_.data() + offset, size);
        return true;
    }
    
    size_t size() const { return memory_region_.size(); }
};

// Tests for SharedMemorySimulation

TEST(SharedMemoryTest, Creation) {
    // Test that we can create a shared memory region
    SharedMemorySimulation shm(1024);
    EXPECT_EQ(shm.size(), 1024);
}

TEST(SharedMemoryTest, ReadWrite) {
    // Test that we can write and read from the shared memory
    SharedMemorySimulation shm(1024);
    
    // Define test data
    std::vector<int> test_data = {1, 2, 3, 4, 5};
    
    // Write data to shared memory
    EXPECT_TRUE(shm.write(test_data.data(), test_data.size() * sizeof(int)));
    
    // Read data back from shared memory
    std::vector<int> read_data(5);
    EXPECT_TRUE(shm.read(read_data.data(), read_data.size() * sizeof(int)));
    
    // Verify data matches
    for (size_t i = 0; i < test_data.size(); i++) {
        EXPECT_EQ(test_data[i], read_data[i]);
    }
}

TEST(SharedMemoryTest, Counter) {
    // Test atomic counter operations
    SharedMemorySimulation shm(1024);
    
    // Initial counter value should be 0
    EXPECT_EQ(shm.get_counter(), 0);
    
    // Increment counter
    EXPECT_EQ(shm.increment_counter(), 1);
    EXPECT_EQ(shm.get_counter(), 1);
    
    // Multiple increments
    for (int i = 0; i < 10; i++) {
        shm.increment_counter();
    }
    EXPECT_EQ(shm.get_counter(), 11);
}

TEST(SharedMemoryTest, Synchronization) {
    // Test synchronization primitives
    SharedMemorySimulation shm(1024);
    
    // Variable to track notification
    bool notified = false;
    
    // Start a thread that waits for notification
    std::thread waiter([&]() {
        // Should timeout
        EXPECT_FALSE(shm.wait(100));
        
        // Should receive notification
        if (shm.wait(5000)) {
            notified = true;
        }
    });
    
    // Sleep briefly to ensure waiter is waiting
    std::this_thread::sleep_for(std::chrono::milliseconds(200));
    
    // Notify waiter
    shm.notify();
    
    // Wait for waiter to finish
    waiter.join();
    
    // Verify notification was received
    EXPECT_TRUE(notified);
}

TEST(SharedMemoryTest, ThreadedCommunication) {
    // Test inter-thread communication using shared memory
    SharedMemorySimulation shm(1024);
    
    // Flags for synchronization
    std::atomic<bool> writer_done{false};
    std::atomic<bool> reader_done{false};
    
    // Define data to write
    const std::vector<int> data_to_write = {42, 84, 126, 168, 210};
    std::vector<int> data_read(5, 0);
    
    // Start reader thread
    std::thread reader([&]() {
        // Wait for notification with a longer timeout
        EXPECT_TRUE(shm.wait(5000));
        
        // Read size
        size_t size = 0;
        shm.read(&size, sizeof(size));
        EXPECT_EQ(size, data_to_write.size());
        
        // Read data
        shm.read(data_read.data(), size * sizeof(int), sizeof(size));
        
        reader_done = true;
    });
    
    // Give the reader thread a chance to start waiting
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
    
    // Start writer thread
    std::thread writer([&]() {
        // Write size of data
        size_t size = data_to_write.size();
        shm.write(&size, sizeof(size));
        
        // Write data
        shm.write(data_to_write.data(), data_to_write.size() * sizeof(int), sizeof(size));
        
        // Increment counter and notify
        shm.increment_counter();
        shm.notify();
        
        writer_done = true;
    });
    
    // Wait for threads to complete
    writer.join();
    reader.join();
    
    // Verify threads completed
    EXPECT_TRUE(writer_done);
    EXPECT_TRUE(reader_done);
    
    // Verify data was transferred correctly
    for (size_t i = 0; i < data_to_write.size(); i++) {
        EXPECT_EQ(data_to_write[i], data_read[i]);
    }
} 