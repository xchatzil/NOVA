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

#ifndef NES_RUNTIME_INCLUDE_RUNTIME_ABSTRACTLINEAGEMANAGER_HPP_
#define NES_RUNTIME_INCLUDE_RUNTIME_ABSTRACTLINEAGEMANAGER_HPP_
#include <Util/BufferSequenceNumber.hpp>
#include <cstddef>

namespace NES::Runtime {

/**
 * @brief The Abstract Lineage Manager class is used to map of all tuples that got their sequence number changed
 * by stateful operators
 */
class AbstractLineageManager {
  public:
    virtual ~AbstractLineageManager() noexcept = default;

    /**
     * @brief Inserts a pair newId, oldId into bufferAncestorMapping, where newId is a key
     * @param newId new sequence number that was created by a stateful operator
     * @param oldId old sequence number that the tuple had
     */
    virtual void insert(BufferSequenceNumber newBufferSequenceNumber, BufferSequenceNumber oldBufferSequenceNumber) = 0;

    /**
     * @brief Deletes a pair<newId,oldId> from bufferAncestorMapping manager
     * @param bufferSequenceNumber newId of the tuple
     * @return true in case of a success trimming
     */
    virtual bool trim(BufferSequenceNumber bufferSequenceNumber) = 0;

    /**
     * @brief Return current bufferAncestorMapping size
     * @return Current bufferAncestorMapping size
     */
    virtual size_t getLineageSize() const = 0;
};
}// namespace NES::Runtime
#endif// NES_RUNTIME_INCLUDE_RUNTIME_ABSTRACTLINEAGEMANAGER_HPP_
