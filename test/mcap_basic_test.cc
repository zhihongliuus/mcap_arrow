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
    
    // Run test
    allTestsPassed = testMessageWriteAndRead(testFile);
    
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