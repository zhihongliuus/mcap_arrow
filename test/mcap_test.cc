#include <gtest/gtest.h>
#include <mcap/reader.hpp>
#include <mcap/writer.hpp>
#include <string>
#include <filesystem>
#include <vector>
#include <fstream>
#include <cstdio>

class McapTest : public ::testing::Test {
protected:
    void SetUp() override {
        // Create a temporary file for testing
        testFile = "test_mcap_file.mcap";
        
        // Make sure the file doesn't exist
        if (std::filesystem::exists(testFile)) {
            std::filesystem::remove(testFile);
        }
    }

    void TearDown() override {
        // Clean up by removing the temporary file
        if (std::filesystem::exists(testFile)) {
            std::filesystem::remove(testFile);
        }
    }

    std::string testFile;
};

// Helper function to create a test message with given content
std::vector<std::byte> createTestMessage(const std::string& content) {
    std::vector<std::byte> msgBytes(content.size());
    for (size_t i = 0; i < content.size(); ++i) {
        msgBytes[i] = static_cast<std::byte>(content[i]);
    }
    return msgBytes;
}

// Test file creation
TEST_F(McapTest, FileCreation) {
    mcap::McapWriter writer;
    mcap::McapWriterOptions writerOptions("");
    writerOptions.compression = mcap::Compression::None;
    
    auto status = writer.open(testFile, writerOptions);
    ASSERT_TRUE(status.ok()) << "Failed to open file for writing: " << status.message;
    
    writer.close();
    
    // Verify file exists and is not empty
    ASSERT_TRUE(std::filesystem::exists(testFile)) << "MCAP file was not created";
    ASSERT_GT(std::filesystem::file_size(testFile), 0) << "MCAP file is empty";
}

// Test schema and channel addition
TEST_F(McapTest, SchemaAndChannelAddition) {
    mcap::McapWriter writer;
    mcap::McapWriterOptions writerOptions("");
    writerOptions.compression = mcap::Compression::None;
    
    auto status = writer.open(testFile, writerOptions);
    ASSERT_TRUE(status.ok()) << "Failed to open file for writing: " << status.message;
    
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
    
    writer.close();
    
    // Verify file exists
    ASSERT_TRUE(std::filesystem::exists(testFile)) << "MCAP file was not created";
    
    // Read the file and verify schema and channel
    mcap::McapReader reader;
    status = reader.open(testFile);
    ASSERT_TRUE(status.ok()) << "Failed to open file for reading: " << status.message;
    
    auto schemas = reader.schemas();
    ASSERT_EQ(schemas.size(), 1) << "Expected 1 schema";
    ASSERT_EQ(schemas.begin()->second->name, "TestSchema") << "Schema name mismatch";
    
    auto channels = reader.channels();
    ASSERT_EQ(channels.size(), 1) << "Expected 1 channel";
    ASSERT_EQ(channels.begin()->second->topic, "/test_topic") << "Channel topic mismatch";
}

// Test message writing and reading
TEST_F(McapTest, MessageWriteAndRead) {
    mcap::McapWriter writer;
    mcap::McapWriterOptions writerOptions("");
    writerOptions.compression = mcap::Compression::None;
    
    auto status = writer.open(testFile, writerOptions);
    ASSERT_TRUE(status.ok()) << "Failed to open file for writing: " << status.message;
    
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
    ASSERT_TRUE(status.ok()) << "Failed to write message: " << status.message;
    
    writer.close();
    
    // Read the file and verify message
    mcap::McapReader reader;
    status = reader.open(testFile);
    ASSERT_TRUE(status.ok()) << "Failed to open file for reading: " << status.message;
    
    auto messageView = reader.readMessages();
    size_t msgCount = 0;
    std::string readData;
    
    for (const auto& msgView : messageView) {
        msgCount++;
        const auto& msg = msgView.message;
        readData = std::string(reinterpret_cast<const char*>(msg.data), msg.dataSize);
    }
    
    ASSERT_EQ(msgCount, 1) << "Expected 1 message";
    ASSERT_EQ(readData, messageData) << "Message data mismatch";
}

// Test empty message
TEST_F(McapTest, EmptyMessage) {
    mcap::McapWriter writer;
    mcap::McapWriterOptions writerOptions("");
    writerOptions.compression = mcap::Compression::None;
    
    auto status = writer.open(testFile, writerOptions);
    ASSERT_TRUE(status.ok()) << "Failed to open file for writing: " << status.message;
    
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
    
    // Create empty message
    const std::string messageData = "";
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
    ASSERT_TRUE(status.ok()) << "Failed to write empty message: " << status.message;
    
    writer.close();
    
    // Read the file and verify empty message
    mcap::McapReader reader;
    status = reader.open(testFile);
    ASSERT_TRUE(status.ok()) << "Failed to open file for reading: " << status.message;
    
    auto messageView = reader.readMessages();
    size_t msgCount = 0;
    std::string readData;
    
    for (const auto& msgView : messageView) {
        msgCount++;
        const auto& msg = msgView.message;
        readData = std::string(reinterpret_cast<const char*>(msg.data), msg.dataSize);
    }
    
    ASSERT_EQ(msgCount, 1) << "Expected 1 message";
    ASSERT_EQ(readData, messageData) << "Empty message data mismatch";
}

// Test multiple messages
TEST_F(McapTest, MultipleMessages) {
    mcap::McapWriter writer;
    mcap::McapWriterOptions writerOptions("");
    writerOptions.compression = mcap::Compression::None;
    
    auto status = writer.open(testFile, writerOptions);
    ASSERT_TRUE(status.ok()) << "Failed to open file for writing: " << status.message;
    
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
    
    // Define message data
    const std::vector<std::string> messageDataList = {
        "Message 1",
        "Message 2",
        "Message 3",
        "Message 4",
        "Message 5"
    };
    
    // Write multiple messages
    for (size_t i = 0; i < messageDataList.size(); ++i) {
        std::vector<std::byte> msgBytes = createTestMessage(messageDataList[i]);
        
        mcap::Message message;
        message.channelId = channel.id;
        message.sequence = i + 1;
        message.logTime = 1000 + i * 100;  // Different timestamps
        message.publishTime = 1000 + i * 100;
        message.dataSize = msgBytes.size();
        message.data = msgBytes.data();
        
        status = writer.write(message);
        ASSERT_TRUE(status.ok()) << "Failed to write message " << i + 1 << ": " << status.message;
    }
    
    writer.close();
    
    // Read the file and verify multiple messages
    mcap::McapReader reader;
    status = reader.open(testFile);
    ASSERT_TRUE(status.ok()) << "Failed to open file for reading: " << status.message;
    
    auto messageView = reader.readMessages();
    std::vector<std::string> readDataList;
    
    for (const auto& msgView : messageView) {
        const auto& msg = msgView.message;
        readDataList.push_back(std::string(reinterpret_cast<const char*>(msg.data), msg.dataSize));
    }
    
    ASSERT_EQ(readDataList.size(), messageDataList.size()) << "Expected " << messageDataList.size() << " messages";
    
    for (size_t i = 0; i < messageDataList.size(); ++i) {
        ASSERT_EQ(readDataList[i], messageDataList[i]) << "Message " << i + 1 << " data mismatch";
    }
}

// Main function to run all tests
int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
} 