#pragma once

#include <cstdint>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <mcap/reader.hpp>

namespace mcap_arrow {

/**
 * @brief Information about a message in an MCAP file
 */
struct MessageInfo {
    int64_t log_time;
    int64_t publish_time;
    uint64_t file_offset;
    std::string topic;
    std::string schema_name;
};

/**
 * @brief Maintains an index of messages in an MCAP file for fast querying
 */
class McapArrowIndex {
public:
    McapArrowIndex();
    ~McapArrowIndex();

    /**
     * @brief Build the index from an MCAP reader
     * @param reader MCAP reader to index
     * @param fields_to_index Map of topic names to field names to index
     * @return true if successful, false otherwise
     */
    bool build(
        mcap::McapReader& reader,
        const std::unordered_map<std::string, std::vector<std::string>>& fields_to_index = {});

    /**
     * @brief Query the index for messages
     * @param topic Topic name
     * @param start_time Start time in nanoseconds
     * @param end_time End time in nanoseconds
     * @param filters Map of field names to values for filtering
     * @return Vector of matching message info
     */
    std::vector<MessageInfo> query(
        const std::string& topic,
        int64_t start_time = 0,
        int64_t end_time = std::numeric_limits<int64_t>::max(),
        const std::unordered_map<std::string, std::string>& filters = {});

    /**
     * @brief Get all topics in the index
     * @return Vector of topic names
     */
    std::vector<std::string> getTopics() const;

    /**
     * @brief Get all indexed fields for a topic
     * @param topic Topic name
     * @return Vector of field names
     */
    std::vector<std::string> getIndexedFields(const std::string& topic) const;

private:
    /**
     * @brief Add a message to the index
     * @param message_info Message info to index
     * @param fields Map of field names to values
     */
    void addToIndex(
        const MessageInfo& message_info,
        const std::unordered_map<std::string, std::string>& fields);

    /**
     * @brief Extract field values from a message
     * @param message Message data
     * @param schema_name Schema name
     * @param fields Field names to extract
     * @return Map of field names to values
     */
    std::unordered_map<std::string, std::string> extractFields(
        const std::vector<uint8_t>& message,
        const std::string& schema_name,
        const std::vector<std::string>& fields);

    // Time-based index: topic -> (time -> message_info)
    std::unordered_map<std::string, std::unordered_map<int64_t, std::vector<MessageInfo>>> time_index_;

    // Field-based index: topic -> field -> value -> message_info
    std::unordered_map<
        std::string, 
        std::unordered_map<
            std::string, 
            std::unordered_map<std::string, std::vector<MessageInfo>>
        >
    > field_index_;

    // Topic to indexed fields
    std::unordered_map<std::string, std::vector<std::string>> indexed_fields_;
};

} // namespace mcap_arrow 