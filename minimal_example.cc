#define MCAP_COMPRESSION_NO_LZ4
#define MCAP_COMPRESSION_NO_ZSTD
#include <iostream>
#include <mcap/reader.hpp>
#include <mcap/writer.hpp>
#include <string>
#include <vector>
#include <cstdlib>
#include <cstring>

// Simple example to verify MCAP functionality
int main() {
  const std::string filename = "test.mcap";
  
  // Create a writer
  mcap::McapWriter writer;
  mcap::McapWriterOptions writerOptions("");  // Empty profile
  writerOptions.compression = mcap::Compression::None;
  
  std::cout << "Opening MCAP file for writing: " << filename << std::endl;
  auto status = writer.open(filename, writerOptions);
  if (!status.ok()) {
    std::cerr << "Failed to open MCAP file for writing: " << status.message << std::endl;
    return 1;
  }
  
  // Create a channel
  const std::string channelId = "example_channel";
  const std::string channelTopic = "/example";
  const std::string messageSchema = "TestMessage";
  const std::string encoding = "cdr";
  
  mcap::Channel channel;
  channel.id = 1;
  channel.topic = channelTopic;
  channel.messageEncoding = encoding;
  channel.schemaId = 1;
  
  // Add schema
  mcap::Schema schema;
  schema.id = 1;
  schema.name = messageSchema;
  schema.encoding = encoding;
  
  // Manually copy the schema data
  std::vector<std::byte> schemaBytes(messageSchema.size());
  for (size_t i = 0; i < messageSchema.size(); ++i) {
    schemaBytes[i] = static_cast<std::byte>(messageSchema[i]);
  }
  schema.data = schemaBytes;
  
  writer.addSchema(schema);
  writer.addChannel(channel);
  
  // Create a simple message
  const std::string messageData = "Hello, MCAP!";
  std::vector<std::byte> msgBytes(messageData.size());
  for (size_t i = 0; i < messageData.size(); ++i) {
    msgBytes[i] = static_cast<std::byte>(messageData[i]);
  }
  
  mcap::Message message;
  message.channelId = channel.id;
  message.sequence = 1;
  message.logTime = 1000;  // Timestamp in nanoseconds
  message.publishTime = 1000;
  message.dataSize = msgBytes.size();
  message.data = msgBytes.data();
  
  // Write message
  status = writer.write(message);
  if (!status.ok()) {
    std::cerr << "Failed to write message: " << status.message << std::endl;
    return 1;
  }
  
  // Close writer
  writer.close();
  
  std::cout << "Successfully wrote to MCAP file." << std::endl;
  
  // Read the file
  mcap::McapReader reader;
  
  std::cout << "Opening MCAP file for reading: " << filename << std::endl;
  status = reader.open(filename);
  if (!status.ok()) {
    std::cerr << "Failed to open MCAP file for reading: " << status.message << std::endl;
    return 1;
  }
  
  // Read messages
  std::cout << "Reading messages from MCAP file..." << std::endl;
  
  auto messageView = reader.readMessages();
  size_t msgCount = 0;
  
  for (const auto& msgView : messageView) {
    msgCount++;
    const auto& msg = msgView.message;
    std::string content(reinterpret_cast<const char*>(msg.data), msg.dataSize);
    std::cout << "Message " << msgCount << ": Channel ID = " << msg.channelId 
              << ", Sequence = " << msg.sequence 
              << ", Topic = " << msgView.channel->topic
              << ", Data = " << content << std::endl;
  }
  
  if (msgCount == 0) {
    std::cerr << "No messages found in the MCAP file." << std::endl;
    return 1;
  }
  
  std::cout << "Successfully read " << msgCount << " messages from MCAP file." << std::endl;
  
  return 0;
} 