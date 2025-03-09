#pragma once

#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <mcap/reader.hpp>

#include "mcap_arrow_index.h"

namespace mcap_arrow {

/**
 * @brief Reads MCAP files and provides access via Arrow Tables
 */
class McapArrowReader {
public:
    McapArrowReader();
    ~McapArrowReader();

    /**
     * @brief Open an MCAP file for reading
     * @param filename Path to the MCAP file
     * @return true if successful, false otherwise
     */
    bool open(const std::string& filename);

    /**
     * @brief Close the MCAP file
     */
    void close();

    /**
     * @brief Build an index of the MCAP file
     * @return true if successful, false otherwise
     */
    bool buildIndex();

    /**
     * @brief Get a list of topics in the MCAP file
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
     * @brief Query messages from a topic
     * @param topic Topic name
     * @param start_time Start time in nanoseconds
     * @param end_time End time in nanoseconds
     * @param filters Map of field names to values for filtering
     * @return Arrow Table containing the query results
     */
    std::shared_ptr<arrow::Table> query(
        const std::string& topic,
        int64_t start_time = 0,
        int64_t end_time = std::numeric_limits<int64_t>::max(),
        const std::unordered_map<std::string, std::string>& filters = {});

private:
    std::unique_ptr<mcap::McapReader> reader_;
    std::shared_ptr<arrow::io::RandomAccessFile> file_;
    std::unique_ptr<McapArrowIndex> index_;
};

} // namespace mcap_arrow 