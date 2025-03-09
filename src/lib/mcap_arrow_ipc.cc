/*
*    This file contains the implementation of the McapArrowIpcWriter and McapArrowIpcReader classes.
*    It provides a way to write and read data to and from a shared memory buffer using Arrow IPC.
*    It also provides a way to read and write to a MCAP file.
*/


#include "mcap_arrow_ipc.h"

#include <iostream>
#include <sstream>
#include <cstring>
#include <filesystem>
#include <chrono>

#include <arrow/buffer.h>
#include <arrow/io/file.h>
#include <arrow/io/memory.h>
#include <arrow/ipc/reader.h>
#include <arrow/ipc/writer.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <arrow/type.h>
#include <arrow/util/io_util.h>

namespace mcap_arrow {

//------------------------------------------------------------------------------
// IpcSyncContext Implementation
//------------------------------------------------------------------------------

IpcSyncContext::IpcSyncContext() 
    : counter_(nullptr), initialized_(false) {}

IpcSyncContext::~IpcSyncContext() {
    // Clean up any resources if needed
    if (counter_) {
        delete counter_;
        counter_ = nullptr;
    }
}

bool IpcSyncContext::initialize(const std::string& name) {
    // Store the base name
    name_ = name;
    
    // Create mutex and condition variable
    // In a real implementation, these would be platform-specific shared objects
    // For simplicity, we're using Arrow's utilities which are not truly cross-process
    mutex_ = std::make_shared<arrow::util::Mutex>();
    cond_var_ = std::make_shared<arrow::util::ConditionVariable>();
    
    // Create shared counter (in reality would be in shared memory)
    counter_ = new std::atomic<int64_t>(0);
    
    initialized_ = true;
    return true;
}

bool IpcSyncContext::wait(int timeout_ms) {
    if (!initialized_) {
        return false;
    }
    
    std::unique_lock<arrow::util::Mutex> lock(*mutex_);
    
    if (timeout_ms <= 0) {
        cond_var_->Wait(*mutex_);
        return true;
    } else {
        auto timeout = std::chrono::milliseconds(timeout_ms);
        return cond_var_->WaitFor(*mutex_, timeout);
    }
}

void IpcSyncContext::notify() {
    if (!initialized_) {
        return;
    }
    
    {
        std::lock_guard<arrow::util::Mutex> lock(*mutex_);
        // Notify all waiters
    }
    cond_var_->NotifyAll();
}

void IpcSyncContext::lock() {
    if (initialized_) {
        mutex_->lock();
    }
}

void IpcSyncContext::unlock() {
    if (initialized_) {
        mutex_->unlock();
    }
}

bool IpcSyncContext::try_lock() {
    if (!initialized_) {
        return false;
    }
    return mutex_->try_lock();
}

int64_t IpcSyncContext::getCounter() const {
    if (!initialized_ || !counter_) {
        return -1;
    }
    return counter_->load();
}

int64_t IpcSyncContext::incrementCounter() {
    if (!initialized_ || !counter_) {
        return -1;
    }
    return ++(*counter_);
}

//------------------------------------------------------------------------------
// McapArrowIpcWriter Implementation
//------------------------------------------------------------------------------

McapArrowIpcWriter::McapArrowIpcWriter()
    : initialized_(false) {
    converter_ = std::make_unique<McapArrowConverter>();
    sync_context_ = std::make_unique<IpcSyncContext>();
}

McapArrowIpcWriter::~McapArrowIpcWriter() {
    close();
}

bool McapArrowIpcWriter::initialize(
    const std::string& shared_memory_name,
    int64_t initial_size,
    const std::string& mcap_filename,
    int compression_level) {
    
    if (initialized_) {
        std::cerr << "McapArrowIpcWriter already initialized" << std::endl;
        return false;
    }
    
    // Store the shared memory name
    shared_memory_info_.name = shared_memory_name;
    shared_memory_info_.size = initial_size;
    shared_memory_info_.mutex_name = shared_memory_name + "_mutex";
    shared_memory_info_.cond_var_name = shared_memory_name + "_cond_var";
    shared_memory_info_.counter_name = shared_memory_name + "_counter";
    
    // Initialize the synchronization context
    if (!sync_context_->initialize(shared_memory_name)) {
        std::cerr << "Failed to initialize synchronization context" << std::endl;
        return false;
    }
    
    // Create a memory-mapped file for shared memory
    // In a full implementation, this would be platform-specific shared memory
    std::string memory_path = std::filesystem::temp_directory_path().string() + "/" + shared_memory_name;
    
    // Create the file if it doesn't exist
    if (!std::filesystem::exists(memory_path)) {
        std::ofstream file(memory_path, std::ios::binary);
        if (!file) {
            std::cerr << "Failed to create shared memory file: " << memory_path << std::endl;
            return false;
        }
        
        // Resize the file to the initial size
        std::vector<char> buffer(initial_size, 0);
        file.write(buffer.data(), buffer.size());
        file.close();
    }
    
    // Open the memory-mapped file
    auto result = arrow::io::MemoryMappedFile::Open(memory_path, arrow::io::FileMode::READWRITE);
    if (!result.ok()) {
        std::cerr << "Failed to memory-map file: " << result.status().ToString() << std::endl;
        return false;
    }
    shared_memory_ = *result;
    
    // Create an IPC writer
    auto ipc_result = arrow::ipc::MakeStreamWriter(shared_memory_, arrow::ipc::IpcWriteOptions::Defaults());
    if (!ipc_result.ok()) {
        std::cerr << "Failed to create IPC writer: " << ipc_result.status().ToString() << std::endl;
        return false;
    }
    ipc_writer_ = std::move(*ipc_result);
    
    // Open the MCAP file if specified
    if (!mcap_filename.empty()) {
        mcap_filename_ = mcap_filename;
        mcap_writer_ = std::make_unique<mcap::McapWriter>();
        
        // Create writer options
        mcap::McapWriterOptions options("ros1");
        
        // Set compression options
        if (compression_level > 0) {
            options.compression = mcap::Compression::Zstd;
            
            // The MCAP library uses an enum for the compression level
            switch (compression_level) {
                case 1: options.compressionLevel = mcap::CompressionLevel::Fastest; break;
                case 2: options.compressionLevel = mcap::CompressionLevel::Fast; break;
                case 3: case 4: options.compressionLevel = mcap::CompressionLevel::Default; break;
                case 5: case 6: case 7: options.compressionLevel = mcap::CompressionLevel::Balanced; break;
                case 8: options.compressionLevel = mcap::CompressionLevel::HighCompression; break;
                case 9: default: options.compressionLevel = mcap::CompressionLevel::MaxCompression; break;
            }
        }
        
        // Open the file
        auto status = mcap_writer_->open(mcap_filename, options);
        if (!status.ok()) {
            std::cerr << "Failed to open MCAP file: " << status.message << std::endl;
            return false;
        }
    }
    
    initialized_ = true;
    return true;
}

void McapArrowIpcWriter::close() {
    if (!initialized_) {
        return;
    }
    
    // Close the IPC writer
    if (ipc_writer_) {
        ipc_writer_->Close().ok();
        ipc_writer_.reset();
    }
    
    // Close the MCAP writer
    if (mcap_writer_) {
        mcap_writer_->close();
        mcap_writer_.reset();
    }
    
    // Close the shared memory
    if (shared_memory_) {
        shared_memory_->Close().ok();
        shared_memory_.reset();
    }
    
    initialized_ = false;
}

bool McapArrowIpcWriter::writeMessage(
    const std::string& topic,
    const google::protobuf::Message& message,
    int64_t timestamp,
    bool notify_readers) {
    
    if (!initialized_) {
        std::cerr << "McapArrowIpcWriter not initialized" << std::endl;
        return false;
    }
    
    // Convert the protobuf message to an Arrow record batch
    auto record_batch = converter_->protoToArrow(message, topic);
    if (!record_batch) {
        std::cerr << "Failed to convert protobuf message to Arrow record batch" << std::endl;
        return false;
    }
    
    // Add timestamp if not present
    if (timestamp <= 0) {
        // Use current time in nanoseconds
        auto now = std::chrono::high_resolution_clock::now();
        timestamp = std::chrono::duration_cast<std::chrono::nanoseconds>(
            now.time_since_epoch()).count();
    }
    
    // Lock the shared memory for writing
    sync_context_->lock();
    
    // Write the record batch to the IPC stream
    auto status = ipc_writer_->WriteRecordBatch(*record_batch);
    if (!status.ok()) {
        std::cerr << "Failed to write record batch to IPC stream: " << status.ToString() << std::endl;
        sync_context_->unlock();
        return false;
    }
    
    // Flush the stream to ensure all data is written
    status = ipc_writer_->Flush();
    if (!status.ok()) {
        std::cerr << "Failed to flush IPC stream: " << status.ToString() << std::endl;
        sync_context_->unlock();
        return false;
    }
    
    // Increment the counter to indicate new data
    sync_context_->incrementCounter();
    
    // Unlock the shared memory
    sync_context_->unlock();
    
    // Write to MCAP file if available
    if (mcap_writer_) {
        mcap::ChannelId channel_id;
        if (!getOrCreateChannel(topic, message, channel_id)) {
            std::cerr << "Failed to get or create channel for topic: " << topic << std::endl;
            return false;
        }
        
        // Serialize the message to binary
        std::string serialized_data;
        if (!message.SerializeToString(&serialized_data)) {
            std::cerr << "Failed to serialize message" << std::endl;
            return false;
        }
        
        // Create MCAP message
        mcap::Message mcap_message;
        mcap_message.channelId = channel_id;
        mcap_message.sequence = 0;  // Optional sequence number
        mcap_message.logTime = timestamp;
        mcap_message.publishTime = timestamp;
        
        // Set message data
        auto data_size = serialized_data.size();
        std::vector<std::byte> data(data_size);
        std::memcpy(data.data(), serialized_data.data(), data_size);
        mcap_message.data = data.data();
        mcap_message.dataSize = data_size;
        
        // Write the message
        auto mcap_status = mcap_writer_->write(mcap_message);
        if (!mcap_status.ok()) {
            std::cerr << "Failed to write message to MCAP: " << mcap_status.message << std::endl;
            return false;
        }
    }
    
    // Notify readers if requested
    if (notify_readers) {
        sync_context_->notify();
    }
    
    return true;
}

bool McapArrowIpcWriter::writeTable(
    const std::string& topic,
    const std::shared_ptr<arrow::Table>& table,
    const std::string& timestamp_column,
    bool notify_readers) {
    
    if (!initialized_) {
        std::cerr << "McapArrowIpcWriter not initialized" << std::endl;
        return false;
    }
    
    if (table->num_rows() == 0) {
        // Nothing to write
        return true;
    }
    
    // Find the timestamp column index
    int timestamp_idx = -1;
    for (int i = 0; i < table->num_columns(); ++i) {
        if (table->field(i)->name() == timestamp_column) {
            timestamp_idx = i;
            break;
        }
    }
    
    if (timestamp_idx == -1) {
        std::cerr << "Timestamp column not found: " << timestamp_column << std::endl;
        return false;
    }
    
    // Lock the shared memory for writing
    sync_context_->lock();
    
    // Convert the table to record batches
    std::vector<std::shared_ptr<arrow::RecordBatch>> batches;
    auto status = arrow::TableBatchReader(*table).ReadAll(&batches);
    if (!status.ok()) {
        std::cerr << "Failed to convert table to record batches: " << status.ToString() << std::endl;
        sync_context_->unlock();
        return false;
    }
    
    // Write each batch
    for (const auto& batch : batches) {
        status = ipc_writer_->WriteRecordBatch(*batch);
        if (!status.ok()) {
            std::cerr << "Failed to write record batch to IPC stream: " << status.ToString() << std::endl;
            sync_context_->unlock();
            return false;
        }
    }
    
    // Flush the stream to ensure all data is written
    status = ipc_writer_->Flush();
    if (!status.ok()) {
        std::cerr << "Failed to flush IPC stream: " << status.ToString() << std::endl;
        sync_context_->unlock();
        return false;
    }
    
    // Increment the counter to indicate new data
    sync_context_->incrementCounter();
    
    // Unlock the shared memory
    sync_context_->unlock();
    
    // Write to MCAP file if available
    if (mcap_writer_) {
        // Get the timestamp column
        auto timestamp_array = std::static_pointer_cast<arrow::Int64Array>(
            table->column(timestamp_idx)->chunk(0));
        
        // TODO: Implement proper conversion from Arrow table to MCAP
        // For now, we'll just write a simplified representation
        for (int64_t row = 0; row < table->num_rows(); ++row) {
            // Get the timestamp for this row
            int64_t timestamp = timestamp_array->Value(row);
            
            // Create a simple serialized representation of the row
            std::ostringstream row_data;
            row_data << "Topic: " << topic << ", Row: " << row;
            std::string data = row_data.str();
            
            // Create MCAP message
            mcap::Message mcap_message;
            mcap_message.channelId = 1;  // Use a fixed channel ID for tables
            mcap_message.sequence = static_cast<uint32_t>(row);
            mcap_message.logTime = timestamp;
            mcap_message.publishTime = timestamp;
            
            // Set message data
            std::vector<std::byte> bytes(data.size());
            std::memcpy(bytes.data(), data.data(), data.size());
            mcap_message.data = bytes.data();
            mcap_message.dataSize = bytes.size();
            
            // Write the message
            auto mcap_status = mcap_writer_->write(mcap_message);
            if (!mcap_status.ok()) {
                std::cerr << "Failed to write message to MCAP: " << mcap_status.message << std::endl;
                return false;
            }
        }
    }
    
    // Notify readers if requested
    if (notify_readers) {
        sync_context_->notify();
    }
    
    return true;
}

SharedMemoryInfo McapArrowIpcWriter::getSharedMemoryInfo() const {
    return shared_memory_info_;
}

bool McapArrowIpcWriter::getOrCreateChannel(
    const std::string& topic,
    const google::protobuf::Message& message,
    mcap::ChannelId& channel_id) {
    
    // Check if we already have a channel for this topic
    auto it = topic_to_channel_.find(topic);
    if (it != topic_to_channel_.end()) {
        channel_id = it->second;
        return true;
    }
    
    // Register a schema for this message type
    auto schema_id = registerSchema(message);
    
    // Create a new channel
    mcap::Channel channel;
    channel.topic = topic;
    channel.messageEncoding = "protobuf";
    channel.schemaId = schema_id;
    
    // Add metadata about the message type
    channel.metadata["message_type"] = message.GetDescriptor()->full_name();
    
    // Add the channel to the MCAP file
    mcap_writer_->addChannel(channel);
    channel_id = channel.id;
    
    // Store the channel ID for future use
    topic_to_channel_[topic] = channel_id;
    
    return true;
}

mcap::SchemaId McapArrowIpcWriter::registerSchema(const google::protobuf::Message& message) {
    // Get the message type name
    std::string message_type = message.GetDescriptor()->full_name();
    
    // Check if we already have a schema for this message type
    auto it = type_to_schema_.find(message_type);
    if (it != type_to_schema_.end()) {
        return it->second;
    }
    
    // Create a new schema
    mcap::Schema schema;
    schema.name = message_type;
    schema.encoding = "protobuf";
    
    // For a full implementation, we would include the protobuf message definition
    // For now, we'll just use a placeholder
    schema.data.resize(1);
    schema.data[0] = std::byte{0};
    
    // Add the schema to the MCAP file
    mcap_writer_->addSchema(schema);
    
    // Store the schema ID for future use
    type_to_schema_[message_type] = schema.id;
    
    return schema.id;
}

bool McapArrowIpcWriter::resizeSharedMemory(int64_t new_size) {
    // This is a simplified implementation that doesn't actually resize
    // In a real implementation, this would resize the shared memory segment
    std::cerr << "Shared memory resizing not implemented" << std::endl;
    return false;
}

//------------------------------------------------------------------------------
// McapArrowIpcReader Implementation
//------------------------------------------------------------------------------

McapArrowIpcReader::McapArrowIpcReader()
    : last_counter_(-1), mode_(Mode::NONE) {
    sync_context_ = std::make_unique<IpcSyncContext>();
    mcap_index_ = std::make_unique<McapArrowIndex>();
}

McapArrowIpcReader::~McapArrowIpcReader() {
    close();
}

bool McapArrowIpcReader::initializeFromSharedMemory(const SharedMemoryInfo& shared_memory_info) {
    if (mode_ != Mode::NONE) {
        std::cerr << "McapArrowIpcReader already initialized" << std::endl;
        return false;
    }
    
    // Initialize the synchronization context
    if (!sync_context_->initialize(shared_memory_info.name)) {
        std::cerr << "Failed to initialize synchronization context" << std::endl;
        return false;
    }
    
    // Open the memory-mapped file
    std::string memory_path = std::filesystem::temp_directory_path().string() + "/" + shared_memory_info.name;
    if (!std::filesystem::exists(memory_path)) {
        std::cerr << "Shared memory file does not exist: " << memory_path << std::endl;
        return false;
    }
    
    auto result = arrow::io::MemoryMappedFile::Open(memory_path, arrow::io::FileMode::READ);
    if (!result.ok()) {
        std::cerr << "Failed to memory-map file: " << result.status().ToString() << std::endl;
        return false;
    }
    shared_memory_ = *result;
    
    // Create an IPC reader
    auto ipc_result = arrow::ipc::RecordBatchStreamReader::Open(shared_memory_);
    if (!ipc_result.ok()) {
        std::cerr << "Failed to create IPC reader: " << ipc_result.status().ToString() << std::endl;
        return false;
    }
    ipc_reader_ = std::move(*ipc_result);
    
    mode_ = Mode::SHARED_MEMORY;
    return true;
}

bool McapArrowIpcReader::initializeFromSharedMemory(const std::string& shared_memory_name) {
    SharedMemoryInfo info;
    info.name = shared_memory_name;
    info.mutex_name = shared_memory_name + "_mutex";
    info.cond_var_name = shared_memory_name + "_cond_var";
    info.counter_name = shared_memory_name + "_counter";
    return initializeFromSharedMemory(info);
}

bool McapArrowIpcReader::initializeFromMcapFile(const std::string& mcap_filename) {
    if (mode_ != Mode::NONE) {
        std::cerr << "McapArrowIpcReader already initialized" << std::endl;
        return false;
    }
    
    // Open the file using Arrow's IO facilities
    auto maybe_file = arrow::io::ReadableFile::Open(mcap_filename);
    if (!maybe_file.ok()) {
        std::cerr << "Failed to open MCAP file: " << maybe_file.status().ToString() << std::endl;
        return false;
    }
    mcap_file_ = *maybe_file;
    
    // Create a reader adapter from Arrow's IO to MCAP's reader
    class ArrowReaderAdapter : public mcap::IReadable {
    public:
        explicit ArrowReaderAdapter(std::shared_ptr<arrow::io::RandomAccessFile> file)
            : file_(std::move(file)) {}
        
        size_t read(std::byte* data, size_t size) override {
            int64_t bytes_read = 0;
            auto status = file_->Read(size, &bytes_read, reinterpret_cast<uint8_t*>(data));
            if (!status.ok()) {
                return 0;
            }
            return static_cast<size_t>(bytes_read);
        }
        
        bool seek(int64_t offset) override {
            auto status = file_->Seek(offset);
            return status.ok();
        }
        
        int64_t size() override {
            auto maybe_size = file_->GetSize();
            if (!maybe_size.ok()) {
                return -1;
            }
            return *maybe_size;
        }
        
        int64_t tell() override {
            auto maybe_pos = file_->Tell();
            if (!maybe_pos.ok()) {
                return -1;
            }
            return *maybe_pos;
        }
        
    private:
        std::shared_ptr<arrow::io::RandomAccessFile> file_;
    };
    
    auto reader_adapter = std::make_unique<ArrowReaderAdapter>(mcap_file_);
    mcap_reader_ = std::make_unique<mcap::McapReader>();
    
    auto status = mcap_reader_->open(*reader_adapter);
    if (!status.ok()) {
        std::cerr << "Failed to open MCAP reader: " << status.message << std::endl;
        return false;
    }
    
    // Build the index
    if (!mcap_index_->build(*mcap_reader_)) {
        std::cerr << "Failed to build MCAP index" << std::endl;
        return false;
    }
    
    mode_ = Mode::MCAP_FILE;
    return true;
}

void McapArrowIpcReader::close() {
    // Close the IPC reader
    if (ipc_reader_) {
        ipc_reader_.reset();
    }
    
    // Close the MCAP reader
    if (mcap_reader_) {
        mcap_reader_->close();
        mcap_reader_.reset();
    }
    
    // Close the shared memory
    if (shared_memory_) {
        shared_memory_->Close().ok();
        shared_memory_.reset();
    }
    
    // Close the MCAP file
    if (mcap_file_) {
        mcap_file_->Close().ok();
        mcap_file_.reset();
    }
    
    // Clear caches
    topic_batches_.clear();
    topic_schemas_.clear();
    
    mode_ = Mode::NONE;
}

bool McapArrowIpcReader::waitForData(int timeout_ms) {
    if (mode_ != Mode::SHARED_MEMORY) {
        std::cerr << "Cannot wait for data in MCAP file mode" << std::endl;
        return false;
    }
    
    return sync_context_->wait(timeout_ms);
}

std::shared_ptr<arrow::Table> McapArrowIpcReader::readLatestData(const std::string& topic) {
    if (mode_ != Mode::SHARED_MEMORY) {
        std::cerr << "Cannot read latest data in MCAP file mode" << std::endl;
        return nullptr;
    }
    
    // Check if there's new data
    int64_t current_counter = sync_context_->getCounter();
    if (current_counter <= last_counter_) {
        // No new data
        return nullptr;
    }
    
    // Read new data
    if (!readNewData()) {
        return nullptr;
    }
    
    // Update the last counter
    last_counter_ = current_counter;
    
    // If topic is specified, return just that topic's data
    if (!topic.empty()) {
        auto it = topic_batches_.find(topic);
        if (it == topic_batches_.end() || it->second.empty()) {
            return nullptr;
        }
        
        // Get the schema
        auto schema_it = topic_schemas_.find(topic);
        if (schema_it == topic_schemas_.end()) {
            return nullptr;
        }
        
        // Convert batches to table
        return arrow::Table::FromRecordBatches(schema_it->second, it->second).ValueOrDie();
    }
    
    // Return all topics as separate tables merged together
    std::vector<std::shared_ptr<arrow::Table>> tables;
    for (const auto& [topic_name, batches] : topic_batches_) {
        if (batches.empty()) {
            continue;
        }
        
        auto schema_it = topic_schemas_.find(topic_name);
        if (schema_it == topic_schemas_.end()) {
            continue;
        }
        
        auto table = arrow::Table::FromRecordBatches(schema_it->second, batches).ValueOrDie();
        tables.push_back(table);
    }
    
    if (tables.empty()) {
        return nullptr;
    }
    
    // Concatenate tables
    std::shared_ptr<arrow::Table> result;
    auto status = arrow::ConcatenateTables(tables, &result);
    if (!status.ok()) {
        std::cerr << "Failed to concatenate tables: " << status.ToString() << std::endl;
        return nullptr;
    }
    
    return result;
}

std::shared_ptr<arrow::Table> McapArrowIpcReader::queryMcap(
    const std::string& topic,
    int64_t start_time,
    int64_t end_time,
    const std::unordered_map<std::string, std::string>& filters) {
    
    if (mode_ != Mode::MCAP_FILE) {
        std::cerr << "Cannot query MCAP in shared memory mode" << std::endl;
        return nullptr;
    }
    
    // Query the index for matching messages
    auto messages = mcap_index_->query(topic, start_time, end_time, filters);
    if (messages.empty()) {
        return nullptr;
    }
    
    // Create a standard schema:
    // - timestamp (int64)
    // - topic (string)
    // - data (binary)
    auto schema = std::make_shared<arrow::Schema>(std::vector<std::shared_ptr<arrow::Field>>{
        arrow::field("timestamp", arrow::int64()),
        arrow::field("topic", arrow::utf8()),
        arrow::field("data", arrow::binary())
    });
    
    // Create builders for the columns
    auto timestamp_builder = std::make_shared<arrow::Int64Builder>();
    auto topic_builder = std::make_shared<arrow::StringBuilder>();
    auto data_builder = std::make_shared<arrow::BinaryBuilder>();
    
    // Reserve capacity
    const size_t num_messages = messages.size();
    timestamp_builder->Reserve(num_messages).ok();
    topic_builder->Reserve(num_messages).ok();
    data_builder->Reserve(num_messages).ok();
    
    // Populate data
    for (const auto& message_info : messages) {
        // Seek to message position
        mcap_file_->Seek(message_info.file_offset).ok();
        
        // TODO: Implement actual message reading
        // For now, we'll just append placeholders
        timestamp_builder->Append(message_info.log_time).ok();
        topic_builder->Append(message_info.topic).ok();
        data_builder->Append(reinterpret_cast<const uint8_t*>(""), 0).ok();
    }
    
    // Finalize arrays
    std::shared_ptr<arrow::Array> timestamp_array;
    timestamp_builder->Finish(&timestamp_array).ok();
    
    std::shared_ptr<arrow::Array> topic_array;
    topic_builder->Finish(&topic_array).ok();
    
    std::shared_ptr<arrow::Array> data_array;
    data_builder->Finish(&data_array).ok();
    
    // Create a RecordBatch
    auto batch = arrow::RecordBatch::Make(schema, num_messages, {
        timestamp_array, topic_array, data_array
    });
    
    // Convert to Table
    return arrow::Table::FromRecordBatches({batch}).ValueOrDie();
}

std::vector<std::string> McapArrowIpcReader::getTopics() const {
    if (mode_ == Mode::MCAP_FILE) {
        return mcap_index_->getTopics();
    } else if (mode_ == Mode::SHARED_MEMORY) {
        std::vector<std::string> topics;
        for (const auto& [topic, _] : topic_schemas_) {
            topics.push_back(topic);
        }
        return topics;
    }
    
    return {};
}

std::shared_ptr<arrow::Schema> McapArrowIpcReader::getSchema(const std::string& topic) const {
    if (mode_ == Mode::SHARED_MEMORY) {
        auto it = topic_schemas_.find(topic);
        if (it != topic_schemas_.end()) {
            return it->second;
        }
    } else if (mode_ == Mode::MCAP_FILE) {
        // For MCAP mode, we return a standard schema
        return std::make_shared<arrow::Schema>(std::vector<std::shared_ptr<arrow::Field>>{
            arrow::field("timestamp", arrow::int64()),
            arrow::field("topic", arrow::utf8()),
            arrow::field("data", arrow::binary())
        });
    }
    
    return nullptr;
}

int64_t McapArrowIpcReader::getCounter() const {
    if (mode_ == Mode::SHARED_MEMORY) {
        return sync_context_->getCounter();
    }
    return -1;
}

bool McapArrowIpcReader::readNewData() {
    if (mode_ != Mode::SHARED_MEMORY) {
        return false;
    }
    
    // Lock the shared memory for reading
    sync_context_->lock();
    
    // Read record batches
    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        auto status = ipc_reader_->ReadNext(&batch);
        if (!status.ok()) {
            std::cerr << "Error reading record batch: " << status.ToString() << std::endl;
            sync_context_->unlock();
            return false;
        }
        
        if (!batch) {
            // End of stream
            break;
        }
        
        // Process the batch
        processRecordBatch(batch);
    }
    
    // Unlock the shared memory
    sync_context_->unlock();
    
    return true;
}

void McapArrowIpcReader::processRecordBatch(const std::shared_ptr<arrow::RecordBatch>& batch) {
    // Extract the topic from the batch
    // This assumes the first column is "topic"
    std::string topic;
    if (batch->schema()->GetFieldIndex("topic") >= 0) {
        int topic_idx = batch->schema()->GetFieldIndex("topic");
        auto topic_array = std::static_pointer_cast<arrow::StringArray>(batch->column(topic_idx));
        if (topic_array->length() > 0) {
            topic = topic_array->GetString(0);
        }
    }
    
    if (topic.empty()) {
        // If no topic found, use a default
        topic = "default_topic";
    }
    
    // Store the schema
    topic_schemas_[topic] = batch->schema();
    
    // Store the batch
    topic_batches_[topic].push_back(batch);
}

} // namespace mcap_arrow 