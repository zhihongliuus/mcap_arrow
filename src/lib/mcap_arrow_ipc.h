#pragma once

#include <memory>
#include <string>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <chrono>
#include <unordered_map>
#include <vector>
#include <thread>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/ipc/api.h>
#include <arrow/ipc/options.h>
#include <arrow/status.h>
#include <arrow/util/logging.h>

#include <google/protobuf/message.h>
#include <mcap/reader.hpp>
#include <mcap/writer.hpp>

#include "mcap_arrow_converter.h"
#include "mcap_arrow_index.h"

namespace mcap_arrow {

/**
 * @brief Shared memory segment information
 */
struct SharedMemoryInfo {
    std::string name;
    int64_t size;
    std::string mutex_name;
    std::string cond_var_name;
    std::string counter_name;
};

/**
 * @brief IPC synchronization context
 */
class IpcSyncContext {
public:
    IpcSyncContext();
    ~IpcSyncContext();

    /**
     * @brief Initialize the sync context
     * @param name Name of the shared memory context
     * @return true if successful, false otherwise
     */
    bool initialize(const std::string& name);
    
    /**
     * @brief Wait for a notification
     * @param timeout_ms Maximum time to wait in milliseconds (0 = wait forever)
     * @return true if notified, false if timed out
     */
    bool wait(int timeout_ms = 0);
    
    /**
     * @brief Notify all waiting processes
     */
    void notify();
    
    /**
     * @brief Lock the mutex
     */
    void lock();
    
    /**
     * @brief Unlock the mutex
     */
    void unlock();
    
    /**
     * @brief Try to lock the mutex
     * @return true if locked, false otherwise
     */
    bool try_lock();
    
    /**
     * @brief Get the current value of the counter
     * @return Current counter value
     */
    int64_t getCounter() const;
    
    /**
     * @brief Increment the counter
     * @return New counter value
     */
    int64_t incrementCounter();

private:
    std::string name_;
    std::shared_ptr<arrow::util::Mutex> mutex_;
    std::shared_ptr<arrow::util::ConditionVariable> cond_var_;
    std::atomic<int64_t>* counter_;
    bool initialized_ = false;
};

/**
 * @brief MCAP Arrow IPC Writer
 * 
 * Allows writing Arrow tables to shared memory for IPC, while also 
 * recording to an MCAP file for persistence. Uses synchronization
 * mechanisms to coordinate with readers.
 */
class McapArrowIpcWriter {
public:
    McapArrowIpcWriter();
    ~McapArrowIpcWriter();

    /**
     * @brief Initialize the IPC writer
     * @param shared_memory_name Name of the shared memory segment
     * @param initial_size Initial size of the shared memory in bytes
     * @param mcap_filename Optional MCAP file for persistent storage
     * @param compression_level Compression level for MCAP (0-9, 0 means no compression)
     * @return true if successful, false otherwise
     */
    bool initialize(
        const std::string& shared_memory_name,
        int64_t initial_size,
        const std::string& mcap_filename = "",
        int compression_level = 0);

    /**
     * @brief Close the writer and release resources
     */
    void close();

    /**
     * @brief Write a protobuf message to shared memory and optionally to MCAP
     * @param topic Topic name
     * @param message Protobuf message to write
     * @param timestamp Optional timestamp (nanoseconds, uses current time if 0)
     * @param notify_readers Whether to notify waiting readers
     * @return true if successful, false otherwise
     */
    bool writeMessage(
        const std::string& topic,
        const google::protobuf::Message& message,
        int64_t timestamp = 0,
        bool notify_readers = true);
    
    /**
     * @brief Write an Arrow table to shared memory and optionally to MCAP
     * @param topic Topic name
     * @param table Arrow table to write
     * @param timestamp_column Name of the timestamp column
     * @param notify_readers Whether to notify waiting readers
     * @return true if successful, false otherwise
     */
    bool writeTable(
        const std::string& topic,
        const std::shared_ptr<arrow::Table>& table,
        const std::string& timestamp_column = "timestamp",
        bool notify_readers = true);

    /**
     * @brief Get information about the shared memory segment
     * @return Shared memory information
     */
    SharedMemoryInfo getSharedMemoryInfo() const;

private:
    // Memory mapped file for shared memory
    std::shared_ptr<arrow::io::MemoryMappedFile> shared_memory_;
    
    // MCAP writer for persistence
    std::unique_ptr<mcap::McapWriter> mcap_writer_;
    
    // Converter for protobuf <-> Arrow conversion
    std::unique_ptr<McapArrowConverter> converter_;
    
    // IPC stream writer for Arrow data
    std::unique_ptr<arrow::ipc::RecordBatchStreamWriter> ipc_writer_;
    
    // Synchronization context
    std::unique_ptr<IpcSyncContext> sync_context_;
    
    // Shared memory information
    SharedMemoryInfo shared_memory_info_;
    
    // Various state tracking variables
    std::string mcap_filename_;
    bool initialized_ = false;
    std::unordered_map<std::string, mcap::ChannelId> topic_to_channel_;
    std::unordered_map<std::string, mcap::SchemaId> type_to_schema_;
    
    // Helper methods
    bool getOrCreateChannel(
        const std::string& topic,
        const google::protobuf::Message& message,
        mcap::ChannelId& channel_id);
    
    mcap::SchemaId registerSchema(const google::protobuf::Message& message);
    
    bool resizeSharedMemory(int64_t new_size);
};

/**
 * @brief MCAP Arrow IPC Reader
 * 
 * Allows reading Arrow tables from shared memory via IPC, while also
 * supporting reading from an MCAP file. Uses synchronization
 * mechanisms to coordinate with writers.
 */
class McapArrowIpcReader {
public:
    McapArrowIpcReader();
    ~McapArrowIpcReader();

    /**
     * @brief Initialize the IPC reader from shared memory
     * @param shared_memory_info Information about the shared memory segment
     * @return true if successful, false otherwise
     */
    bool initializeFromSharedMemory(const SharedMemoryInfo& shared_memory_info);

    /**
     * @brief Initialize the IPC reader from shared memory by name
     * @param shared_memory_name Name of the shared memory segment
     * @return true if successful, false otherwise
     */
    bool initializeFromSharedMemory(const std::string& shared_memory_name);

    /**
     * @brief Initialize the IPC reader from an MCAP file
     * @param mcap_filename MCAP file to read from
     * @return true if successful, false otherwise
     */
    bool initializeFromMcapFile(const std::string& mcap_filename);

    /**
     * @brief Close the reader and release resources
     */
    void close();

    /**
     * @brief Wait for new data to be available
     * @param timeout_ms Maximum time to wait in milliseconds (0 = wait forever)
     * @return true if new data is available, false if timed out
     */
    bool waitForData(int timeout_ms = 0);

    /**
     * @brief Read the latest data as an Arrow table
     * @param topic Topic to read (empty string means all topics)
     * @return Arrow table containing the latest data
     */
    std::shared_ptr<arrow::Table> readLatestData(const std::string& topic = "");

    /**
     * @brief Query messages from a topic in the MCAP file
     * @param topic Topic name
     * @param start_time Start time in nanoseconds
     * @param end_time End time in nanoseconds
     * @param filters Map of field names to values for filtering
     * @return Arrow Table containing the query results
     */
    std::shared_ptr<arrow::Table> queryMcap(
        const std::string& topic,
        int64_t start_time = 0,
        int64_t end_time = std::numeric_limits<int64_t>::max(),
        const std::unordered_map<std::string, std::string>& filters = {});

    /**
     * @brief Get a list of available topics
     * @return Vector of topic names
     */
    std::vector<std::string> getTopics() const;

    /**
     * @brief Get the schema for a specific topic
     * @param topic Topic name
     * @return Arrow schema for the topic, or nullptr if not found
     */
    std::shared_ptr<arrow::Schema> getSchema(const std::string& topic) const;

    /**
     * @brief Get the counter value from the sync context
     * @return Current counter value
     */
    int64_t getCounter() const;

private:
    // Memory mapped file for shared memory
    std::shared_ptr<arrow::io::MemoryMappedFile> shared_memory_;
    
    // MCAP reader for persistent storage
    std::unique_ptr<mcap::McapReader> mcap_reader_;
    
    // Arrow IPC reader for shared memory
    std::unique_ptr<arrow::ipc::RecordBatchStreamReader> ipc_reader_;
    
    // Arrow file for MCAP reading
    std::shared_ptr<arrow::io::RandomAccessFile> mcap_file_;
    
    // MCAP index for efficient queries
    std::unique_ptr<McapArrowIndex> mcap_index_;
    
    // Synchronization context
    std::unique_ptr<IpcSyncContext> sync_context_;
    
    // Cache of record batches for each topic
    std::unordered_map<std::string, std::vector<std::shared_ptr<arrow::RecordBatch>>> topic_batches_;
    
    // Cache of schemas for each topic
    std::unordered_map<std::string, std::shared_ptr<arrow::Schema>> topic_schemas_;
    
    // Last read counter value
    int64_t last_counter_ = -1;
    
    // Reading mode
    enum class Mode { SHARED_MEMORY, MCAP_FILE, NONE } mode_ = Mode::NONE;
    
    // Internal methods
    bool readNewData();
    void processRecordBatch(const std::shared_ptr<arrow::RecordBatch>& batch);
};

} // namespace mcap_arrow 