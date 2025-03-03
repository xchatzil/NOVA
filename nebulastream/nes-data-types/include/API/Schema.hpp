/*
    Licensed under the Apache License, Version 2.0 (the "License");
    you may not use this file except in compliance with the License.
    You may obtain a copy of the License at

        https://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing, software
    distributed under the License is distributed on an "AS IS" BASIS,
    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
    See the License for the specific language governing permissions and
    limitations under the License.
*/

#ifndef NES_DATA_TYPES_INCLUDE_API_SCHEMA_HPP_
#define NES_DATA_TYPES_INCLUDE_API_SCHEMA_HPP_

#include <Common/DataTypes/BasicTypes.hpp>
#include <memory>
#include <string>
#include <vector>

namespace NES {

namespace Configurations {
class SchemaType;
using SchemaTypePtr = std::shared_ptr<SchemaType>;
}// namespace Configurations

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

class DataType;
using DataTypePtr = std::shared_ptr<DataType>;

class AttributeField;
using AttributeFieldPtr = std::shared_ptr<AttributeField>;

class Schema {
  public:
    /**
     * @brief Enum to identify the memory layout in which we want to represent the schema physically.
     */
    enum class MemoryLayoutType : uint8_t { ROW_LAYOUT = 0, COLUMNAR_LAYOUT = 1 };

    explicit Schema(MemoryLayoutType layoutType = MemoryLayoutType::ROW_LAYOUT);
    Schema(SchemaPtr const& query, MemoryLayoutType layoutType = MemoryLayoutType::ROW_LAYOUT);

    /**
     * @brief Schema qualifier separator
     */
    constexpr static const char* const ATTRIBUTE_NAME_SEPARATOR = "$";

    /**
     * @brief Factory method to create a new SchemaPtr.
     * @return SchemaPtr
     */
    static SchemaPtr create(MemoryLayoutType layoutType = MemoryLayoutType::ROW_LAYOUT);

    /**
     * @brief Factory method to create a new SchemaPtr from schema type.
     * @return SchemaPtr
     */
    static SchemaPtr createFromSchemaType(const Configurations::SchemaTypePtr& schemaType,
                                          MemoryLayoutType layoutType = MemoryLayoutType::ROW_LAYOUT);

    /**
     * @brief Prepends the srcName to the substring after the last occurrence of ATTRIBUTE_NAME_SEPARATOR
     * in every field name of the schema.
     * @param srcName
     * @return SchemaPtr
     */
    SchemaPtr updateSourceName(const std::string& srcName) const;

    /**
     * @brief Creates a copy of this schema.
     * @note The containing AttributeFields may still reference the same objects.
     * @return A copy of the Schema
     */
    [[nodiscard]] SchemaPtr copy() const;

    /**
     * @brief Copy all fields of otherSchema into this schema.
     * @param otherSchema
     * @return a copy of this schema.
     */
    SchemaPtr copyFields(const SchemaPtr& otherSchema);

    /**
     * @brief appends a AttributeField to the schema and returns a copy of this schema.
     * @param attribute
     * @return a copy of this schema.
     */
    SchemaPtr addField(const AttributeFieldPtr& attribute);

    /**
    * @brief appends a field with a basic type to the schema and returns a copy of this schema.
    * @param field
    * @return a copy of this schema.
    */
    SchemaPtr addField(const std::string& name, const BasicType& type);

    /**
    * @brief appends a field with a data type to the schema and returns a copy of this schema.
    * @param field
    * @return a copy of this schema.
    */
    SchemaPtr addField(const std::string& name, DataTypePtr data);

    /**
     * @brief removes a AttributeField from the schema
     * @param field
     */
    void removeField(const AttributeFieldPtr& field);

    /**
     * @brief Replaces a field, which is already part of the schema.
     * @param name of the field we want to replace
     * @param DataTypePtr
     */
    void replaceField(const std::string& name, const DataTypePtr& type);

    /**
     * @brief Returns the attribute field based on a qualified or unqualified field name.
     *
     * @details
     * If an unqualified field name is given (e.g., `getField("fieldName")`), the function will match attribute fields with any source name.
     * If a qualified field name is given (e.g., `getField("source$fieldName")`), the entire qualified field must match.
     *
     * @param fieldName: Name of the attribute field that should be returned.
     * @return Pointer to attribute field if present, otherwise `nullptr`.
     */
    AttributeFieldPtr getField(const std::string& fieldName) const;

    /**
     * @brief Checks if attribute field name is defined in the schema and returns its index.
     * If item not in the list, then the return value is equal to fields.size().
     * @param fieldName
     * @return the index
     */
    uint64_t getIndex(const std::string& fieldName) const;

    /**
     * @brief Finds a attribute field by name in the schema
     * @param fieldName
     * @return AttributeField
     */
    AttributeFieldPtr get(const std::string& fieldName) const;

    /**
     * @brief Finds a attribute field by index in the schema
     * @param index
     * @return AttributeField
     */
    AttributeFieldPtr get(uint32_t index);

    /**
     * @brief Returns the number of fields in the schema.
     * @return uint64_t
     */
    [[nodiscard]] uint64_t getSize() const;

    /**
     * @brief Returns the number of bytes all fields in this schema occupy.
     * @return uint64_t
     */
    [[nodiscard]] uint64_t getSchemaSizeInBytes() const;

    /**
     * @brief Checks if two Schemas are equal to each other.
     * @param schema
     * @param considerOrder takes into account if the order of fields in a schema matter.
     * @return boolean
     */
    bool equals(const SchemaPtr& schema, bool considerOrder = true);

    /**
     * @brief Checks if two schemas have same datatypes at same index location
     * @param otherSchema: the other schema to compare agains
     * @return ture if they are equal else false
     */
    bool hasEqualTypes(const SchemaPtr& otherSchema);

    /**
     * @brief Checks if the field exists in the schema
     * @param schema
     * @return boolean
    */
    bool contains(const std::string& fieldName) const;

    /**
     * @brief returns a string representation
     * @param prefix of the string
     * @param delimitor between each field
     * @param suffix, for the end of the string
     * @return schema as string
     */
    [[nodiscard]] std::string
    toString(const std::string& prefix = "", const std::string& sep = " ", const std::string& suffix = "") const;

    /**
     * @brief returns the string representation of layout
     * @param layout
     * @return
     */
    [[nodiscard]] std::string getLayoutTypeAsString() const;

    /**
     * @brief Method to return the source name qualifier, thus everything that is before $
     * @return string
     */
    [[nodiscard]] std::string getSourceNameQualifier() const;

    /**
     * @brief method to get the qualifier of the source without $
     * @return qualifier without $
     */
    std::string getQualifierNameForSystemGeneratedFields() const;

    /**
     * @brief method to get the qualifier of the source with $
     * @return qualifier with $
     */
    std::string getQualifierNameForSystemGeneratedFieldsWithSeparator() const;

    /**
     * @brief Remove all fields and qualifying name
     */
    void clear();

    /**
    * @brief Is checks if the schema is empty (if it has no fields).
    * @return true if empty
    */
    bool empty() const;

    /**
     * @brief method to get the type of the memory layout
     * @return MemoryLayoutType
     */
    [[nodiscard]] MemoryLayoutType getLayoutType() const;

    /**
     * @brief method to set the memory layout
     * @param layoutType
     */
    void setLayoutType(MemoryLayoutType layoutType);

    /**
     * @brief Get the field names as a vector of strings.
     * @return std::vector<std::string> fieldNames
     */
    std::vector<std::string> getFieldNames() const;

    std::vector<AttributeFieldPtr> fields;

  private:
    /**
     * Return the appropriate NES type from yaml string configuration. Ignores
     * fieldLength if it doesn't make sense, errors length is missing and type
     * is string.
     * @param fieldType the type of the schema field from yaml
     * @param fieldLength the length of the field from yaml
     * @return the appropriate DataTypePtr
     */
    static DataTypePtr stringToFieldType(const std::string& fieldType, const std::string& fieldLength);

    MemoryLayoutType layoutType;
};

AttributeFieldPtr createField(const std::string& name, BasicType type);

AttributeFieldPtr createField(const std::string& name, DataTypePtr type);

}// namespace NES
#endif// NES_DATA_TYPES_INCLUDE_API_SCHEMA_HPP_
