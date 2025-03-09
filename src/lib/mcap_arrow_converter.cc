#include "mcap_arrow_converter.h"

#include <iostream>

#include <arrow/builder.h>
#include <arrow/table_builder.h>
#include <arrow/type.h>
#include <google/protobuf/descriptor.h>
#include <google/protobuf/message.h>

namespace mcap_arrow {

McapArrowConverter::McapArrowConverter() {}

McapArrowConverter::~McapArrowConverter() {}

std::shared_ptr<arrow::RecordBatch> McapArrowConverter::protoToArrow(
    const google::protobuf::Message& message,
    const std::string& topic) {
    
    // Create schema from protobuf message
    auto schema = createSchemaFromProto(message);
    
    // Create a record batch builder
    arrow::MemoryPool* pool = arrow::default_memory_pool();
    
    // Using Arrow's C++ API, we need to create a builder for each field in the schema
    std::unique_ptr<arrow::RecordBatchBuilder> builder;
    auto status = arrow::RecordBatchBuilder::Make(schema, pool, 1, &builder);
    if (!status.ok()) {
        std::cerr << "Error creating RecordBatchBuilder: " << status.ToString() << std::endl;
        return nullptr;
    }
    
    // If topic is provided, add it to the record
    if (!topic.empty()) {
        auto topic_builder = builder->GetFieldAs<arrow::StringBuilder>(0);
        status = topic_builder->Append(topic);
        if (!status.ok()) {
            std::cerr << "Error adding topic to record batch: " << status.ToString() << std::endl;
            return nullptr;
        }
    }
    
    // Convert the protobuf message fields to Arrow fields
    // This is a simplified version - a full implementation would handle all protobuf field types
    const google::protobuf::Descriptor* descriptor = message.GetDescriptor();
    for (int i = 0; i < descriptor->field_count(); ++i) {
        const google::protobuf::FieldDescriptor* field = descriptor->field(i);
        addProtoFieldToBuilder(builder.get(), message, field, 0);
    }
    
    // Finalize the record batch
    std::shared_ptr<arrow::RecordBatch> batch;
    status = builder->Flush(&batch);
    if (!status.ok()) {
        std::cerr << "Error flushing record batch: " << status.ToString() << std::endl;
        return nullptr;
    }
    
    return batch;
}

bool McapArrowConverter::arrowToProto(
    const std::shared_ptr<arrow::RecordBatch>& batch,
    google::protobuf::Message* message) {
    
    if (!message) {
        return false;
    }
    
    // Get the descriptor for reflection
    const google::protobuf::Descriptor* descriptor = message->GetDescriptor();
    const google::protobuf::Reflection* reflection = message->GetReflection();
    
    // Map Arrow field indices to protobuf field descriptors
    std::unordered_map<int, const google::protobuf::FieldDescriptor*> field_map;
    for (int i = 0; i < descriptor->field_count(); ++i) {
        const google::protobuf::FieldDescriptor* field = descriptor->field(i);
        
        // Find the matching Arrow field by name
        for (int j = 0; j < batch->schema()->num_fields(); ++j) {
            if (batch->schema()->field(j)->name() == field->name()) {
                field_map[j] = field;
                break;
            }
        }
    }
    
    // For simplicity, we're assuming there's only one row in the batch
    // A full implementation would handle multiple rows
    if (batch->num_rows() == 0) {
        return true;  // Nothing to do
    }
    
    // Process each field
    for (const auto& [arrow_idx, proto_field] : field_map) {
        // Get the Arrow array for this field
        auto array = batch->column(arrow_idx);
        
        // Handle different protobuf field types
        switch (proto_field->type()) {
            case google::protobuf::FieldDescriptor::TYPE_DOUBLE: {
                auto typed_array = std::static_pointer_cast<arrow::DoubleArray>(array);
                reflection->SetDouble(message, proto_field, typed_array->Value(0));
                break;
            }
            case google::protobuf::FieldDescriptor::TYPE_FLOAT: {
                auto typed_array = std::static_pointer_cast<arrow::FloatArray>(array);
                reflection->SetFloat(message, proto_field, typed_array->Value(0));
                break;
            }
            case google::protobuf::FieldDescriptor::TYPE_INT64:
            case google::protobuf::FieldDescriptor::TYPE_SINT64:
            case google::protobuf::FieldDescriptor::TYPE_SFIXED64: {
                auto typed_array = std::static_pointer_cast<arrow::Int64Array>(array);
                reflection->SetInt64(message, proto_field, typed_array->Value(0));
                break;
            }
            case google::protobuf::FieldDescriptor::TYPE_INT32:
            case google::protobuf::FieldDescriptor::TYPE_SINT32:
            case google::protobuf::FieldDescriptor::TYPE_SFIXED32: {
                auto typed_array = std::static_pointer_cast<arrow::Int32Array>(array);
                reflection->SetInt32(message, proto_field, typed_array->Value(0));
                break;
            }
            case google::protobuf::FieldDescriptor::TYPE_BOOL: {
                auto typed_array = std::static_pointer_cast<arrow::BooleanArray>(array);
                reflection->SetBool(message, proto_field, typed_array->Value(0));
                break;
            }
            case google::protobuf::FieldDescriptor::TYPE_STRING: {
                auto typed_array = std::static_pointer_cast<arrow::StringArray>(array);
                reflection->SetString(message, proto_field, typed_array->GetString(0));
                break;
            }
            // Add more type handling as needed
            default:
                // Skip unsupported types for now
                std::cerr << "Unsupported field type: " << proto_field->type_name() << std::endl;
                break;
        }
    }
    
    return true;
}

void McapArrowConverter::addProtoFieldToBuilder(
    arrow::RecordBatchBuilder* builder,
    const google::protobuf::Message& message,
    const google::protobuf::FieldDescriptor* field,
    int row_index) {
    
    const google::protobuf::Reflection* reflection = message.GetReflection();
    
    // Skip if the field doesn't exist
    if (!field) {
        return;
    }
    
    // Find the corresponding builder by name
    for (int i = 0; i < builder->num_fields(); ++i) {
        auto array_builder = builder->GetField(i);
        if (array_builder->name() == field->name()) {
            // Handle different field types
            switch (field->type()) {
                case google::protobuf::FieldDescriptor::TYPE_DOUBLE: {
                    auto typed_builder = static_cast<arrow::DoubleBuilder*>(array_builder);
                    typed_builder->Append(reflection->GetDouble(message, field)).ok();
                    break;
                }
                case google::protobuf::FieldDescriptor::TYPE_FLOAT: {
                    auto typed_builder = static_cast<arrow::FloatBuilder*>(array_builder);
                    typed_builder->Append(reflection->GetFloat(message, field)).ok();
                    break;
                }
                case google::protobuf::FieldDescriptor::TYPE_INT64:
                case google::protobuf::FieldDescriptor::TYPE_SINT64:
                case google::protobuf::FieldDescriptor::TYPE_SFIXED64: {
                    auto typed_builder = static_cast<arrow::Int64Builder*>(array_builder);
                    typed_builder->Append(reflection->GetInt64(message, field)).ok();
                    break;
                }
                case google::protobuf::FieldDescriptor::TYPE_INT32:
                case google::protobuf::FieldDescriptor::TYPE_SINT32:
                case google::protobuf::FieldDescriptor::TYPE_SFIXED32: {
                    auto typed_builder = static_cast<arrow::Int32Builder*>(array_builder);
                    typed_builder->Append(reflection->GetInt32(message, field)).ok();
                    break;
                }
                case google::protobuf::FieldDescriptor::TYPE_BOOL: {
                    auto typed_builder = static_cast<arrow::BooleanBuilder*>(array_builder);
                    typed_builder->Append(reflection->GetBool(message, field)).ok();
                    break;
                }
                case google::protobuf::FieldDescriptor::TYPE_STRING: {
                    auto typed_builder = static_cast<arrow::StringBuilder*>(array_builder);
                    typed_builder->Append(reflection->GetString(message, field)).ok();
                    break;
                }
                // Add more type handling as needed
                default:
                    // Skip unsupported types for now
                    std::cerr << "Unsupported field type: " << field->type_name() << std::endl;
                    break;
            }
            break;
        }
    }
}

std::shared_ptr<arrow::Schema> McapArrowConverter::createSchemaFromProto(
    const google::protobuf::Message& message) {
    
    // Create Arrow fields from protobuf fields
    std::vector<std::shared_ptr<arrow::Field>> fields;
    
    // Add a topic field first (useful for querying)
    fields.push_back(arrow::field("topic", arrow::utf8()));
    
    // Process each protobuf field
    const google::protobuf::Descriptor* descriptor = message.GetDescriptor();
    for (int i = 0; i < descriptor->field_count(); ++i) {
        const google::protobuf::FieldDescriptor* field = descriptor->field(i);
        
        // Get Arrow type for the protobuf field
        auto arrow_type = getArrowType(field);
        
        // Add field to schema
        fields.push_back(arrow::field(field->name(), arrow_type));
    }
    
    return arrow::schema(fields);
}

std::shared_ptr<arrow::DataType> McapArrowConverter::getArrowType(
    const google::protobuf::FieldDescriptor* field) {
    
    // Map protobuf types to Arrow types
    switch (field->type()) {
        case google::protobuf::FieldDescriptor::TYPE_DOUBLE:
            return arrow::float64();
        case google::protobuf::FieldDescriptor::TYPE_FLOAT:
            return arrow::float32();
        case google::protobuf::FieldDescriptor::TYPE_INT64:
        case google::protobuf::FieldDescriptor::TYPE_SINT64:
        case google::protobuf::FieldDescriptor::TYPE_SFIXED64:
            return arrow::int64();
        case google::protobuf::FieldDescriptor::TYPE_UINT64:
        case google::protobuf::FieldDescriptor::TYPE_FIXED64:
            return arrow::uint64();
        case google::protobuf::FieldDescriptor::TYPE_INT32:
        case google::protobuf::FieldDescriptor::TYPE_SINT32:
        case google::protobuf::FieldDescriptor::TYPE_SFIXED32:
            return arrow::int32();
        case google::protobuf::FieldDescriptor::TYPE_UINT32:
        case google::protobuf::FieldDescriptor::TYPE_FIXED32:
            return arrow::uint32();
        case google::protobuf::FieldDescriptor::TYPE_BOOL:
            return arrow::boolean();
        case google::protobuf::FieldDescriptor::TYPE_STRING:
        case google::protobuf::FieldDescriptor::TYPE_BYTES:
            return arrow::utf8();
        // Add more type mappings as needed
        default:
            // Default to binary for unsupported types
            return arrow::binary();
    }
}

} // namespace mcap_arrow 