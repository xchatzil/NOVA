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

#ifndef NES_RUNTIME_INCLUDE_SOURCES_PARSERS_JSONPARSER_HPP_
#define NES_RUNTIME_INCLUDE_SOURCES_PARSERS_JSONPARSER_HPP_

#include <Sources/Parsers/Parser.hpp>
#include <Util/TestTupleBuffer.hpp>

namespace NES {
class JSONParser : public Parser {

  public:
    /**
   * @brief public constructor for JSON input data parser
   * @param numberOfSchemaFields number of schema fields
   * @param schemaKeys vector with schema keys to identify the keys in the json object
   * @param physicalTypes vector with physical data types
   */
    JSONParser(uint64_t numberOfSchemaFields,
               std::vector<std::string> schemaKeys,
               std::vector<NES::PhysicalTypePtr> physicalTypes);

    /**
   * @brief takes a json tuple as string, parses it using cpprest and calls Parser::writeFieldValueToTupleBuffer() for every value in the tuple
   * @param jsonTuple: string value that is cast to the PhysicalType and written to the TupleBuffer
   * @param tupleCount: the number of tuples already written to the current TupleBuffer
   * @param tupleBuffer: the TupleBuffer to which the value is written containing the currently chosen memory layout
   * @param schema: data schema
   * @param bufferManager: the buffer manager
   */
    bool writeInputTupleToTupleBuffer(std::string_view jsonTuple,
                                      uint64_t tupleCount,
                                      Runtime::MemoryLayouts::TestTupleBuffer& tupleBuffer,
                                      const SchemaPtr& schema,
                                      const Runtime::BufferManagerPtr& bufferManager) override;

  private:
    uint64_t numberOfSchemaFields;
    std::vector<std::string> schemaKeys;
    std::vector<NES::PhysicalTypePtr> physicalTypes;
};
}// namespace NES
#endif// NES_RUNTIME_INCLUDE_SOURCES_PARSERS_JSONPARSER_HPP_
