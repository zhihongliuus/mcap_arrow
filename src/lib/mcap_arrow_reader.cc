#include "mcap_arrow_reader.h"

#include <chrono>
#include <limits>

#include <arrow/buffer.h>
#include <arrow/io/file.h>
#include <arrow/table.h>
#include <arrow/record_batch.h>
#include <arrow/util/checked_cast.h>
#include <arrow/util/io_util.h>

namespace mcap_arrow {

McapArrowReader::McapArrowReader()
    : reader_(std::make_unique<mcap::McapReader>()),
      index_(std::make_unique<McapArrowIndex>()) {}

McapArrowReader::~McapArrowReader() {
    close();
}

bool McapArrowReader::open(const std::string& filename) {
    // Open the file using Arrow's IO facilities
    auto maybe_file = arrow::io::ReadableFile::Open(filename);
    if (!maybe_file.ok()) {
        return false;
    }
    file_ = *maybe_file;
    
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
    
    auto reader_adapter = std::make_unique<ArrowReaderAdapter>(file_);
    
    auto status = reader_->open(*reader_adapter);
    if (!status.ok()) {
        return false;
    }
    
    return true;
}

void McapArrowReader::close() {
    if (reader_) {
        reader_->close();
    }
    
    if (file_) {
        file_->Close().ok();
        file_.reset();
    }
}

bool McapArrowReader::buildIndex() {
    // Build a general index of all message topics
    return index_->build(*reader_);
}

std::vector<std::string> McapArrowReader::getTopics() const {
    return index_->getTopics();
}

std::shared_ptr<arrow::Schema> McapArrowReader::getSchema(const std::string& topic) const {
    // For simplicity, create a standard schema:
    // - timestamp (int64)
    // - topic (string)
    // - message (binary)
    auto fields = std::vector<std::shared_ptr<arrow::Field>>{
        arrow::field("timestamp", arrow::int64()),
        arrow::field("topic", arrow::utf8()),
        arrow::field("data", arrow::binary())
    };
    
    return std::make_shared<arrow::Schema>(fields);
}

std::shared_ptr<arrow::Table> McapArrowReader::query(
    const std::string& topic,
    int64_t start_time,
    int64_t end_time,
    const std::unordered_map<std::string, std::string>& filters) {
    
    // Query the index for matching messages
    auto messages = index_->query(topic, start_time, end_time, filters);
    if (messages.empty()) {
        return nullptr;
    }
    
    // Create builders for the columns
    auto schema = getSchema(topic);
    auto timestamp_builder = std::make_shared<arrow::Int64Builder>();
    auto topic_builder = std::make_shared<arrow::StringBuilder>();
    auto data_builder = std::make_shared<arrow::BinaryBuilder>();
    
    // Reserve capacity
    const size_t num_messages = messages.size();
    ARROW_RETURN_NOT_OK(timestamp_builder->Reserve(num_messages));
    ARROW_RETURN_NOT_OK(topic_builder->Reserve(num_messages));
    ARROW_RETURN_NOT_OK(data_builder->Reserve(num_messages));
    
    // Populate data
    for (const auto& message_info : messages) {
        // Seek to message position
        file_->Seek(message_info.file_offset).ok();
        
        // TODO: Implement actual message reading
        // For now, we'll just append placeholders
        ARROW_RETURN_NOT_OK(timestamp_builder->Append(message_info.log_time));
        ARROW_RETURN_NOT_OK(topic_builder->Append(message_info.topic));
        ARROW_RETURN_NOT_OK(data_builder->Append(reinterpret_cast<const uint8_t*>(""), 0));
    }
    
    // Finalize arrays
    std::shared_ptr<arrow::Array> timestamp_array;
    ARROW_RETURN_NOT_OK(timestamp_builder->Finish(&timestamp_array));
    
    std::shared_ptr<arrow::Array> topic_array;
    ARROW_RETURN_NOT_OK(topic_builder->Finish(&topic_array));
    
    std::shared_ptr<arrow::Array> data_array;
    ARROW_RETURN_NOT_OK(data_builder->Finish(&data_array));
    
    // Create a RecordBatch
    auto batch = arrow::RecordBatch::Make(schema, num_messages, {
        timestamp_array, topic_array, data_array
    });
    
    // Convert to Table
    return arrow::Table::FromRecordBatches({batch}).ValueOrDie();
}

} // namespace mcap_arrow 