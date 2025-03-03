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

#ifndef NES_RUNTIME_INCLUDE_SINKS_FORMATS_CSVFORMAT_HPP_
#define NES_RUNTIME_INCLUDE_SINKS_FORMATS_CSVFORMAT_HPP_

#include <Sinks/Formats/SinkFormat.hpp>
namespace NES {

class CsvFormat : public SinkFormat {
  public:
    /**
     * Ctor for CSV format.
     * @param schema Ptr to the schema
     * @param bufferManager Ptr to the buffer manager
     * @param addTimestamp Flag, to indicate if timestamp shall be added when formatting
     */
    CsvFormat(SchemaPtr schema, Runtime::BufferManagerPtr bufferManager, bool addTimestamp);
    CsvFormat(SchemaPtr schema, Runtime::BufferManagerPtr bufferManager);
    virtual ~CsvFormat() noexcept = default;

    /**
     * @brief Returns the schema of formatted according to the specific SinkFormat represented as string.
     * @return The formatted schema as string
     */
    std::string getFormattedSchema() override;

    /**
    * @brief method to format a TupleBuffer
    * @param a tuple buffers pointer
    * @return formatted content of TupleBuffer, contains timestamp if specified
     */
    std::string getFormattedBuffer(Runtime::TupleBuffer& inputBuffer) override;

    /**
    * @brief method to write a TupleBuffer
    * @param a tuple buffers pointer
    * @return vector of Tuple buffer containing the content of the tuplebuffer
     */
    FormatIterator getTupleIterator(Runtime::TupleBuffer& inputBuffer) override;

    /**
     * @brief method to return the format as a string
     * @return format as string
     */
    std::string toString() override;

    /**
     * @brief return sink format
     * @return sink format
     */
    FormatTypes getSinkFormat() override;
};
}// namespace NES
#endif// NES_RUNTIME_INCLUDE_SINKS_FORMATS_CSVFORMAT_HPP_
