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

#ifndef NES_RUNTIME_INCLUDE_RUNTIME_BUFFERRECYCLER_HPP_
#define NES_RUNTIME_INCLUDE_RUNTIME_BUFFERRECYCLER_HPP_

namespace NES::Runtime {
namespace detail {
class MemorySegment;
}
/**
 * @brief Interface for buffer recycling mechanism
 */
class BufferRecycler {

  public:
    /**
     * @brief Interface method for pooled buffer recycling
     * @param buffer the buffer to recycle
     */
    virtual void recyclePooledBuffer(detail::MemorySegment* buffer) = 0;

    /**
     * @brief Interface method for unpooled buffer recycling
     * @param buffer the buffer to recycle
     */
    virtual void recycleUnpooledBuffer(detail::MemorySegment* buffer) = 0;
};

}// namespace NES::Runtime

#endif// NES_RUNTIME_INCLUDE_RUNTIME_BUFFERRECYCLER_HPP_
