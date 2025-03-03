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
#ifndef NES_RUNTIME_INCLUDE_QUERYCOMPILER_EXCEPTIONS_QUERYCOMPILATIONEXCEPTION_HPP_
#define NES_RUNTIME_INCLUDE_QUERYCOMPILER_EXCEPTIONS_QUERYCOMPILATIONEXCEPTION_HPP_
#include <Exceptions/RuntimeException.hpp>
#include <stdexcept>

namespace NES::QueryCompilation {

/**
 * @brief Exception thrown by the query compiler.
 */
class QueryCompilationException : public Exceptions::RuntimeException {
  public:
    explicit QueryCompilationException(const std::string& message,
                                       std::source_location location = std::source_location::current());
};

}// namespace NES::QueryCompilation

#endif// NES_RUNTIME_INCLUDE_QUERYCOMPILER_EXCEPTIONS_QUERYCOMPILATIONEXCEPTION_HPP_
