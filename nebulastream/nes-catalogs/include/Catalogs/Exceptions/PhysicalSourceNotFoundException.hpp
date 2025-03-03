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
#ifndef NES_CATALOGS_INCLUDE_CATALOGS_EXCEPTIONS_PHYSICALSOURCENOTFOUNDEXCEPTION_HPP_
#define NES_CATALOGS_INCLUDE_CATALOGS_EXCEPTIONS_PHYSICALSOURCENOTFOUNDEXCEPTION_HPP_

#include <Exceptions/RequestExecutionException.hpp>
#include <Identifiers/Identifiers.hpp>
namespace NES::Exceptions {

/**
 * @brief This exception indicates, that a physical source was not found
 */
class PhysicalSourceNotFoundException : public RequestExecutionException {
  public:
    /**
     * @brief construct an exception containing a human readable message
     * @param message: A string to indicate to the user what caused the exception
     */
    explicit PhysicalSourceNotFoundException(const std::string& message);
};
}// namespace NES::Exceptions
#endif// NES_CATALOGS_INCLUDE_CATALOGS_EXCEPTIONS_PHYSICALSOURCENOTFOUNDEXCEPTION_HPP_
