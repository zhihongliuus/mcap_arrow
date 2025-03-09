#include "mcap_arrow_index.h"

#include <iostream>
#include <limits>

namespace mcap_arrow {

McapArrowIndex::McapArrowIndex() {}

McapArrowIndex::~McapArrowIndex() {}

bool McapArrowIndex::build(
    mcap::McapReader& reader,
    const std::unordered_map<std::string, std::vector<std::string>>& fields_to_index) {
    
    // Store the fields to index for each topic
    indexed_fields_ = fields_to_index;
    
    // Map from channel IDs to topic names (for lookups)
    std::unordered_map<mcap::ChannelId, std::string> channel_id_to_topic;
    
    // Get all channels
    const auto& channels = reader.channels();
    for (const auto& [channel_id, channel] : channels) {
        channel_id_to_topic[channel_id] = channel->topic;
    }
    
    // Read all messages
    auto messages = reader.readMessages();
    for (const auto& view : messages) {
        const auto& message = view.message;
        const auto& channel = view.channel;
        
        if (!channel) {
            std::cerr << "Skipping message with null channel" << std::endl;
            continue;
        }
        
        // Create message info
        MessageInfo message_info;
        message_info.log_time = message.logTime;
        message_info.publish_time = message.publishTime;
        message_info.file_offset = view.messageOffset.offset;
        message_info.topic = channel->topic;
        message_info.schema_name = channel->schemaId > 0 ? 
            reader.schemas().at(channel->schemaId)->name : "";
        
        // Extract fields from the message if needed
        std::unordered_map<std::string, std::string> fields;
        auto topic_fields_it = fields_to_index.find(message_info.topic);
        if (topic_fields_it != fields_to_index.end()) {
            // Convert message data to vector for extraction
            std::vector<uint8_t> message_data(
                reinterpret_cast<const uint8_t*>(message.data),
                reinterpret_cast<const uint8_t*>(message.data) + message.dataSize);
            
            // Extract fields
            fields = extractFields(message_data, message_info.schema_name, topic_fields_it->second);
        }
        
        // Add to the index
        addToIndex(message_info, fields);
    }
    
    return true;
}

std::vector<MessageInfo> McapArrowIndex::query(
    const std::string& topic,
    int64_t start_time,
    int64_t end_time,
    const std::unordered_map<std::string, std::string>& filters) {
    
    std::vector<MessageInfo> results;
    
    // Check if we have this topic
    auto topic_it = time_index_.find(topic);
    if (topic_it == time_index_.end()) {
        return results;
    }
    
    // If there are no filters, just use the time index
    if (filters.empty()) {
        for (const auto& [time, messages] : topic_it->second) {
            if (time >= start_time && time <= end_time) {
                // Add all messages at this time
                results.insert(results.end(), messages.begin(), messages.end());
            }
        }
        return results;
    }
    
    // If we have filters, use the field index to find matching messages
    for (const auto& [field, value] : filters) {
        // Check if this field is indexed
        auto field_index_it = field_index_.find(topic);
        if (field_index_it == field_index_.end()) {
            continue;
        }
        
        auto field_it = field_index_it->second.find(field);
        if (field_it == field_index_it->second.end()) {
            continue;
        }
        
        // Find messages with this field value
        auto value_it = field_it->second.find(value);
        if (value_it == field_it->second.end()) {
            continue;
        }
        
        // Filter messages by time
        for (const auto& message_info : value_it->second) {
            if (message_info.log_time >= start_time && message_info.log_time <= end_time) {
                results.push_back(message_info);
            }
        }
    }
    
    return results;
}

std::vector<std::string> McapArrowIndex::getTopics() const {
    std::vector<std::string> topics;
    for (const auto& [topic, _] : time_index_) {
        topics.push_back(topic);
    }
    return topics;
}

std::vector<std::string> McapArrowIndex::getIndexedFields(const std::string& topic) const {
    auto it = indexed_fields_.find(topic);
    if (it != indexed_fields_.end()) {
        return it->second;
    }
    return {};
}

void McapArrowIndex::addToIndex(
    const MessageInfo& message_info,
    const std::unordered_map<std::string, std::string>& fields) {
    
    // Add to time index
    time_index_[message_info.topic][message_info.log_time].push_back(message_info);
    
    // Add to field indices
    for (const auto& [field, value] : fields) {
        field_index_[message_info.topic][field][value].push_back(message_info);
    }
}

std::unordered_map<std::string, std::string> McapArrowIndex::extractFields(
    const std::vector<uint8_t>& message,
    const std::string& schema_name,
    const std::vector<std::string>& fields) {
    
    // This is a placeholder implementation
    // In a real application, you would use the protobuf reflection API to extract fields
    std::unordered_map<std::string, std::string> result;
    
    // For demonstration, just use dummy values
    for (const auto& field : fields) {
        result[field] = "dummy_value";
    }
    
    return result;
}

} // namespace mcap_arrow 