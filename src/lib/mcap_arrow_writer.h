#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <arrow/api.h>
#include <mcap/writer.hpp>
#include <google/protobuf/message.h>

namespace mcap_arrow {

/**
 * @brief Writes Arrow tables and Protocol Buffers messages to MCAP files
 */
class McapArrowWriter {
public:
    McapArrowWriter();
    ~McapArrowWriter();

    /**
     * @brief Open an MCAP file for writing
     * @param filename Path to the MCAP file
     * @param compression_level Compression level (0-9, 0 means no compression)
     * @return true if successful, false otherwise
     */
    bool open(const std::string& filename, int compression_level = 0);

    /**
     * @brief Close the MCAP file
     */
    void close();

    /**
     * @brief Write a protobuf message to the MCAP file
     * @param topic Topic name
     * @param message Protobuf message to write
     * @param timestamp Optional timestamp (nanoseconds, uses current time if 0)
     * @return true if successful, false otherwise
     */
    bool writeMessage(
        const std::string& topic,
        const google::protobuf::Message& message,
        int64_t timestamp = 0);
    
    /**
     * @brief Write an Arrow table to the MCAP file
     * @param topic Topic name
     * @param table Arrow table to write
     * @param timestamp_column Name of the timestamp column
     * @return true if successful, false otherwise
     */
    bool writeTable(
        const std::string& topic,
        const std::shared_ptr<arrow::Table>& table,
        const std::string& timestamp_column = "timestamp");

private:
    /**
     * @brief Get or create a channel for a topic and message type
     * @param topic Topic name
     * @param message Protobuf message
     * @param channel_id Output parameter for the channel ID
     * @return true if successful, false otherwise
     */
    bool getOrCreateChannel(
        const std::string& topic,
        const google::protobuf::Message& message,
        mcap::ChannelId& channel_id);
    
    /**
     * @brief Register a schema for a message type
     * @param message Protobuf message
     * @return Schema ID
     */
    mcap::SchemaId registerSchema(const google::protobuf::Message& message);

    std::unique_ptr<mcap::McapWriter> writer_;
    std::unordered_map<std::string, mcap::ChannelId> topic_to_channel_;
    std::unordered_map<std::string, mcap::SchemaId> type_to_schema_;
};

} // namespace mcap_arrow 