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
#ifndef NES_RUNTIME_INCLUDE_SOURCES_PARSERS_PARSER_HPP_
#define NES_RUNTIME_INCLUDE_SOURCES_PARSERS_PARSER_HPP_

#include <Runtime/RuntimeForwardRefs.hpp>
#include <string>
#include <string_view>
#include <vector>

namespace NES {

namespace Runtime::MemoryLayouts {
class TestTupleBuffer;
}

namespace Runtime {
class BufferManager;
using BufferManagerPtr = std::shared_ptr<BufferManager>;
}// namespace Runtime

class PhysicalType;
using PhysicalTypePtr = std::shared_ptr<PhysicalType>;
/**
 * @brief Base class for all input data parsers in NES
 */
class Parser {

  public:
    /**
   * @brief public constructor for input data parser
   * @param physicalTypes vector with physical data types
   */
    explicit Parser(std::vector<PhysicalTypePtr> physicalTypes);

    virtual ~Parser() = default;

    /**
   * @brief takes a tuple as string, casts its values to the correct types and writes it to the TupleBuffer
   * @param inputTuple: string value that is cast to the PhysicalType and written to the TupleBuffer
   * @param tupleCount: the number of tuples already written to the current TupleBuffer
   * @param tupleBuffer: the TupleBuffer to which the value is written containing the currently chosen memory layout
   * @param schema: data schema
   * @param bufferManager: the buffer manager
   */
    virtual bool writeInputTupleToTupleBuffer(std::string_view inputTuple,
                                              uint64_t tupleCount,
                                              Runtime::MemoryLayouts::TestTupleBuffer& tupleBuffer,
                                              const SchemaPtr& schema,
                                              const Runtime::BufferManagerPtr& bufferManager) = 0;

    /**
   * @brief casts a value in string format to the correct type and writes it to the TupleBuffer
   * @param value: string value that is cast to the PhysicalType and written to the TupleBuffer
   * @param schemaFieldIndex: field/attribute that is currently processed
   * @param tupleBuffer: the TupleBuffer to which the value is written containing the currently chosen memory layout
   * @param json: denotes whether input comes from JSON for correct parsing
   * @param schema: the schema the data are supposed to have
   * @param tupleCount: current tuple count, i.e. how many tuples have already been produced
   * @param bufferManager: the buffer manager
   */
    void writeFieldValueToTupleBuffer(std::string value,
                                      uint64_t schemaFieldIndex,
                                      Runtime::MemoryLayouts::TestTupleBuffer& tupleBuffer,
                                      const SchemaPtr& schema,
                                      uint64_t tupleCount,
                                      const Runtime::BufferManagerPtr& bufferManager);

  private:
    std::vector<PhysicalTypePtr> physicalTypes;
};
}//namespace NES
#endif// NES_RUNTIME_INCLUDE_SOURCES_PARSERS_PARSER_HPP_
