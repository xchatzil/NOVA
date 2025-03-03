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
#ifndef NES_COMMON_TESTS_UTIL_INCLUDE_BORROWEDPORT_HPP_
#define NES_COMMON_TESTS_UTIL_INCLUDE_BORROWEDPORT_HPP_
#include <cstdint>
namespace NES::Testing {

namespace detail {
class PortDispatcher;
}
/**
 * @brief A borrowed port from the port pool of nes test base class.
 * It manages garbage collection internally when dtor is called.
 */
class BorrowedPort {
  private:
    uint16_t port;
    uint32_t portIndex;
    detail::PortDispatcher* parent;

  public:
    /**
     * @brief Creates a new port wrapper object
     * @param port the port value
     * @param portIndex the index of the port in the pool
     * @param parent the pool object
     */
    explicit BorrowedPort(uint16_t port, uint32_t portIndex, detail::PortDispatcher* parent);

    ~BorrowedPort() noexcept;

    [[nodiscard]] operator uint16_t() const;
};
}// namespace NES::Testing
#endif// NES_COMMON_TESTS_UTIL_INCLUDE_BORROWEDPORT_HPP_
