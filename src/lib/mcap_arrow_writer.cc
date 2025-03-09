#include "mcap_arrow_writer.h"

#include <chrono>
#include <cstring>
#include <iostream>

namespace mcap_arrow {

McapArrowWriter::McapArrowWriter()
    : writer_(std::make_unique<mcap::McapWriter>()) {}

McapArrowWriter::~McapArrowWriter() {
    close();
}

bool McapArrowWriter::open(const std::string& filename, int compression_level) {
    // Create writer options with ROS1 profile (can be changed as needed)
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
    auto status = writer_->open(filename, options);
    if (!status.ok()) {
        std::cerr << "Failed to open MCAP file: " << status.message << std::endl;
        return false;
    }
    
    return true;
}

void McapArrowWriter::close() {
    if (writer_) {
        writer_->close();
    }
}

bool McapArrowWriter::writeMessage(
    const std::string& topic,
    const google::protobuf::Message& message,
    int64_t timestamp) {
    
    // Get the channel ID for this topic/message type
    mcap::ChannelId channel_id;
    if (!getOrCreateChannel(topic, message, channel_id)) {
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
    
    // Use provided timestamp or current time if 0
    if (timestamp > 0) {
        mcap_message.logTime = timestamp;
        mcap_message.publishTime = timestamp;
    } else {
        // Use current time in nanoseconds
        auto now = std::chrono::high_resolution_clock::now();
        auto ns = std::chrono::duration_cast<std::chrono::nanoseconds>(
            now.time_since_epoch()).count();
        mcap_message.logTime = ns;
        mcap_message.publishTime = ns;
    }
    
    // Set message data
    auto data_size = serialized_data.size();
    std::vector<std::byte> data(data_size);
    std::memcpy(data.data(), serialized_data.data(), data_size);
    mcap_message.data = data.data();
    mcap_message.dataSize = data_size;
    
    // Write the message
    auto status = writer_->write(mcap_message);
    if (!status.ok()) {
        std::cerr << "Failed to write message: " << status.message << std::endl;
        return false;
    }
    
    return true;
}

bool McapArrowWriter::writeTable(
    const std::string& topic,
    const std::shared_ptr<arrow::Table>& table,
    const std::string& timestamp_column) {
    
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
    
    // Get the timestamp column
    auto timestamp_array = std::static_pointer_cast<arrow::Int64Array>(
        table->column(timestamp_idx)->chunk(0));
    
    // TODO: Implement conversion from Arrow table to protobuf message
    // This would require defining a schema mapping
    
    // For now, just demonstrate writing each row as a simple message
    for (int64_t row = 0; row < table->num_rows(); ++row) {
        // Create a simple message with the table data
        // In a real implementation, you would convert the row to a proper protobuf message
        
        // Get the timestamp for this row
        int64_t timestamp = timestamp_array->Value(row);
        
        // For demonstration, create a simple serialized representation of the row
        std::string row_data = "Row " + std::to_string(row) + " data";
        
        // Create MCAP message
        mcap::Message mcap_message;
        mcap_message.channelId = 1;  // Use a fixed channel ID for demonstration
        mcap_message.sequence = static_cast<uint32_t>(row);
        mcap_message.logTime = timestamp;
        mcap_message.publishTime = timestamp;
        
        // Set message data
        std::vector<std::byte> data(row_data.size());
        std::memcpy(data.data(), row_data.data(), row_data.size());
        mcap_message.data = data.data();
        mcap_message.dataSize = row_data.size();
        
        // Write the message
        auto status = writer_->write(mcap_message);
        if (!status.ok()) {
            std::cerr << "Failed to write message: " << status.message << std::endl;
            return false;
        }
    }
    
    return true;
}

bool McapArrowWriter::getOrCreateChannel(
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
    writer_->addChannel(channel);
    channel_id = channel.id;
    
    // Store the channel ID for future use
    topic_to_channel_[topic] = channel_id;
    
    return true;
}

mcap::SchemaId McapArrowWriter::registerSchema(const google::protobuf::Message& message) {
    const std::string& message_type = message.GetDescriptor()->full_name();
    
    // Check if we already have a schema for this message type
    auto it = type_to_schema_.find(message_type);
    if (it != type_to_schema_.end()) {
        return it->second;
    }
    
    // Create a new schema
    mcap::Schema schema;
    schema.name = message_type;
    schema.encoding = "protobuf";
    
    // Store the message type as the schema data
    std::vector<std::byte> data(message_type.size());
    std::memcpy(data.data(), message_type.data(), message_type.size());
    schema.data = data;
    
    // Add the schema to the MCAP file
    writer_->addSchema(schema);
    
    // Store the schema ID for future use
    type_to_schema_[message_type] = schema.id;
    
    return schema.id;
}

} // namespace mcap_arrow 