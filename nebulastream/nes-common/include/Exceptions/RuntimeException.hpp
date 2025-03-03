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

#ifndef NES_COMMON_INCLUDE_EXCEPTIONS_RUNTIMEEXCEPTION_HPP_
#define NES_COMMON_INCLUDE_EXCEPTIONS_RUNTIMEEXCEPTION_HPP_

#include <Util/SourceLocation.hpp>
#include <Util/StacktraceLoader.hpp>
#include <exception>
#include <stdexcept>
#include <string>

namespace NES::Exceptions {

/**
 * @brief Exception to be used to report errors and stacktraces
 * This is meant to be used for NES-related errors, wrap std exceptions with their own stacktrace, etc..
 */
class RuntimeException : virtual public std::exception {

  protected:
    std::string errorMessage;///< Error message

  public:
    /** Constructor
     *  @param msg The error message
     *  @param stacktrace Error stacktrace
     */
    explicit RuntimeException(std::string msg,
                              std::string&& stacktrace = collectStacktrace(),
                              const std::source_location location = std::source_location::current());

    /** Constructor
    *  @param msg The error message
    *  @param stacktrace Error stacktrace
    */
    explicit RuntimeException(std::string msg, const std::string& stacktrace);

    /** Destructor.
     *  Virtual to allow for subclassing.
     */
    ~RuntimeException() noexcept override = default;

    /** Returns a pointer to the (constant) error description.
     *  @return A pointer to a const char*. The underlying memory
     *  is in possession of the Except object. Callers must
     *  not attempt to free the memory.
     */
    [[nodiscard]] const char* what() const noexcept override;
};

}// namespace NES::Exceptions

#endif// NES_COMMON_INCLUDE_EXCEPTIONS_RUNTIMEEXCEPTION_HPP_
