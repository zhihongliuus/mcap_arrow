#pragma once

#include <memory>
#include <string>
#include <unordered_map>

#include <arrow/api.h>
#include <arrow/record_batch.h>
#include <arrow/table.h>
#include <google/protobuf/message.h>

namespace mcap_arrow {

/**
 * @brief Converts between Protocol Buffers messages and Arrow tables
 */
class McapArrowConverter {
public:
    McapArrowConverter();
    ~McapArrowConverter();

    /**
     * @brief Convert a protobuf message to an Arrow record batch
     * @param message Protobuf message to convert
     * @param topic Topic name, included as a column in the batch
     * @return Arrow RecordBatch containing the message data
     */
    std::shared_ptr<arrow::RecordBatch> protoToArrow(
        const google::protobuf::Message& message,
        const std::string& topic = "");

    /**
     * @brief Convert an Arrow record batch to a protobuf message
     * @param batch Arrow record batch to convert
     * @param message Output parameter for the converted message
     * @return true if successful, false otherwise
     */
    bool arrowToProto(
        const std::shared_ptr<arrow::RecordBatch>& batch,
        google::protobuf::Message* message);

private:
    /**
     * @brief Add a protobuf message field to a record batch builder
     * @param builder Record batch builder
     * @param message Protobuf message
     * @param field Field descriptor
     * @param row_index Index of the row in the builder
     */
    void addProtoFieldToBuilder(
        arrow::RecordBatchBuilder* builder,
        const google::protobuf::Message& message,
        const google::protobuf::FieldDescriptor* field,
        int row_index);

    /**
     * @brief Create an Arrow schema from a protobuf message
     * @param message Protobuf message
     * @return Arrow schema
     */
    std::shared_ptr<arrow::Schema> createSchemaFromProto(
        const google::protobuf::Message& message);

    /**
     * @brief Get an Arrow field type for a protobuf field
     * @param field Field descriptor
     * @return Arrow data type
     */
    std::shared_ptr<arrow::DataType> getArrowType(
        const google::protobuf::FieldDescriptor* field);
};

} // namespace mcap_arrow 