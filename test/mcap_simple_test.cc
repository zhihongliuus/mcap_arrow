#define MCAP_COMPRESSION_NO_LZ4
#define MCAP_COMPRESSION_NO_ZSTD
#include <mcap/reader.hpp>
#include <mcap/writer.hpp>
#include <string>
#include <vector>
#include <filesystem>
#include <iostream>
#include <cstdio>

// Helper function to create a test message with given content
std::vector<std::byte> createTestMessage(const std::string& content) {
    std::vector<std::byte> msgBytes(content.size());
    for (size_t i = 0; i < content.size(); ++i) {
        msgBytes[i] = static_cast<std::byte>(content[i]);
    }
    return msgBytes;
}

// Simple test function for file creation
bool testFileCreation(const std::string& testFile) {
    // Remove test file if it exists
    if (std::filesystem::exists(testFile)) {
        std::filesystem::remove(testFile);
    }
    
    mcap::McapWriter writer;
    mcap::McapWriterOptions writerOptions("");
    writerOptions.compression = mcap::Compression::None;
    
    auto status = writer.open(testFile, writerOptions);
    if (!status.ok()) {
        std::cerr << "Failed to open file for writing: " << status.message << std::endl;
        return false;
    }
    
    writer.close();
    
    // Verify file exists and is not empty
    if (!std::filesystem::exists(testFile)) {
        std::cerr << "MCAP file was not created" << std::endl;
        return false;
    }
    
    if (std::filesystem::file_size(testFile) == 0) {
        std::cerr << "MCAP file is empty" << std::endl;
        return false;
    }
    
    std::cout << "File creation test: PASSED" << std::endl;
    return true;
}

// Simple test function for schema and channel addition
bool testSchemaAndChannelAddition(const std::string& testFile) {
    // Remove test file if it exists
    if (std::filesystem::exists(testFile)) {
        std::filesystem::remove(testFile);
    }
    
    mcap::McapWriter writer;
    mcap::McapWriterOptions writerOptions("");
    writerOptions.compression = mcap::Compression::None;
    
    auto status = writer.open(testFile, writerOptions);
    if (!status.ok()) {
        std::cerr << "Failed to open file for writing: " << status.message << std::endl;
        return false;
    }
    
    // Create schema
    mcap::Schema schema;
    schema.id = 1;
    schema.name = "TestSchema";
    schema.encoding = "test_encoding";
    
    // Copy schema data
    std::vector<std::byte> schemaBytes = createTestMessage("test schema data");
    schema.data = schemaBytes;
    
    // Add schema
    writer.addSchema(schema);
    
    // Create channel
    mcap::Channel channel;
    channel.id = 1;
    channel.topic = "/test_topic";
    channel.messageEncoding = "test_encoding";
    channel.schemaId = 1;
    
    // Add channel
    writer.addChannel(channel);
    
    // Write at least one message to ensure schemas and channels are stored
    std::vector<std::byte> msgBytes = createTestMessage("test message data");
    
    mcap::Message message;
    message.channelId = channel.id;
    message.sequence = 1;
    message.logTime = 1000;
    message.publishTime = 1000;
    message.dataSize = msgBytes.size();
    message.data = msgBytes.data();
    
    status = writer.write(message);
    if (!status.ok()) {
        std::cerr << "Failed to write test message: " << status.message << std::endl;
        return false;
    }
    
    // Close the writer to ensure all data is flushed
    writer.close();
    
    // Verify file exists
    if (!std::filesystem::exists(testFile)) {
        std::cerr << "MCAP file was not created" << std::endl;
        return false;
    }
    
    // Read the file and verify schema and channel
    mcap::McapReader reader;
    status = reader.open(testFile);
    if (!status.ok()) {
        std::cerr << "Failed to open file for reading: " << status.message << std::endl;
        return false;
    }
    
    auto schemas = reader.schemas();
    if (schemas.size() != 1) {
        std::cerr << "Expected 1 schema, got " << schemas.size() << std::endl;
        return false;
    }
    
    if (schemas.begin()->second->name != "TestSchema") {
        std::cerr << "Schema name mismatch: expected 'TestSchema', got '" 
                  << schemas.begin()->second->name << "'" << std::endl;
        return false;
    }
    
    auto channels = reader.channels();
    if (channels.size() != 1) {
        std::cerr << "Expected 1 channel, got " << channels.size() << std::endl;
        return false;
    }
    
    if (channels.begin()->second->topic != "/test_topic") {
        std::cerr << "Channel topic mismatch: expected '/test_topic', got '" 
                  << channels.begin()->second->topic << "'" << std::endl;
        return false;
    }
    
    std::cout << "Schema and channel addition test: PASSED" << std::endl;
    return true;
}

// Simple test function for message writing and reading
bool testMessageWriteAndRead(const std::string& testFile) {
    // Remove test file if it exists
    if (std::filesystem::exists(testFile)) {
        std::filesystem::remove(testFile);
    }
    
    mcap::McapWriter writer;
    mcap::McapWriterOptions writerOptions("");
    writerOptions.compression = mcap::Compression::None;
    
    auto status = writer.open(testFile, writerOptions);
    if (!status.ok()) {
        std::cerr << "Failed to open file for writing: " << status.message << std::endl;
        return false;
    }
    
    // Create schema
    mcap::Schema schema;
    schema.id = 1;
    schema.name = "TestSchema";
    schema.encoding = "test_encoding";
    
    // Copy schema data
    std::vector<std::byte> schemaBytes = createTestMessage("test schema data");
    schema.data = schemaBytes;
    
    writer.addSchema(schema);
    
    // Create channel
    mcap::Channel channel;
    channel.id = 1;
    channel.topic = "/test_topic";
    channel.messageEncoding = "test_encoding";
    channel.schemaId = 1;
    
    writer.addChannel(channel);
    
    // Create message
    const std::string messageData = "Hello, MCAP Test!";
    std::vector<std::byte> msgBytes = createTestMessage(messageData);
    
    mcap::Message message;
    message.channelId = channel.id;
    message.sequence = 1;
    message.logTime = 1000;
    message.publishTime = 1000;
    message.dataSize = msgBytes.size();
    message.data = msgBytes.data();
    
    // Write message
    status = writer.write(message);
    if (!status.ok()) {
        std::cerr << "Failed to write message: " << status.message << std::endl;
        return false;
    }
    
    writer.close();
    
    // Read the file and verify message
    mcap::McapReader reader;
    status = reader.open(testFile);
    if (!status.ok()) {
        std::cerr << "Failed to open file for reading: " << status.message << std::endl;
        return false;
    }
    
    auto messageView = reader.readMessages();
    size_t msgCount = 0;
    std::string readData;
    
    for (const auto& msgView : messageView) {
        msgCount++;
        const auto& msg = msgView.message;
        readData = std::string(reinterpret_cast<const char*>(msg.data), msg.dataSize);
    }
    
    if (msgCount != 1) {
        std::cerr << "Expected 1 message, got " << msgCount << std::endl;
        return false;
    }
    
    if (readData != messageData) {
        std::cerr << "Message data mismatch: expected '" << messageData 
                  << "', got '" << readData << "'" << std::endl;
        return false;
    }
    
    std::cout << "Message write and read test: PASSED" << std::endl;
    return true;
}

// Main function to run all tests
int main() {
    const std::string testFile = "test_mcap_file.mcap";
    bool allTestsPassed = true;
    
    // Run all tests
    allTestsPassed &= testFileCreation(testFile);
    allTestsPassed &= testSchemaAndChannelAddition(testFile);
    allTestsPassed &= testMessageWriteAndRead(testFile);
    
    // Clean up
    if (std::filesystem::exists(testFile)) {
        std::filesystem::remove(testFile);
    }
    
    if (allTestsPassed) {
        std::cout << "ALL TESTS PASSED!" << std::endl;
        return 0;
    } else {
        std::cerr << "SOME TESTS FAILED!" << std::endl;
        return 1;
    }
} 