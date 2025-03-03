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

#ifndef NES_COMMON_INCLUDE_EXCEPTIONS_INVALIDARGUMENTEXCEPTION_HPP_
#define NES_COMMON_INCLUDE_EXCEPTIONS_INVALIDARGUMENTEXCEPTION_HPP_

#include <stdexcept>
#include <string>

namespace NES {

/**
 * @brief This class is used for adding custom invalid argument exception.
 */
class InvalidArgumentException : public std::exception {
  public:
    explicit InvalidArgumentException(const std::string& name, const std::string& value);
    [[nodiscard]] const char* what() const noexcept override;

  private:
    std::string message;
};
}// namespace NES
#endif// NES_COMMON_INCLUDE_EXCEPTIONS_INVALIDARGUMENTEXCEPTION_HPP_
