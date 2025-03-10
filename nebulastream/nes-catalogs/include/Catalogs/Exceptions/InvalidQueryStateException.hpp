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

#ifndef NES_CATALOGS_INCLUDE_CATALOGS_EXCEPTIONS_INVALIDQUERYSTATEEXCEPTION_HPP_
#define NES_CATALOGS_INCLUDE_CATALOGS_EXCEPTIONS_INVALIDQUERYSTATEEXCEPTION_HPP_

#include <Catalogs/Query/QueryCatalogEntry.hpp>
#include <Exceptions/RequestExecutionException.hpp>
#include <stdexcept>
#include <vector>

namespace NES::Exceptions {
/**
 * @brief Exception is raised when the query is in an Invalid status
 */
class InvalidQueryStateException : public RequestExecutionException {
  public:
    explicit InvalidQueryStateException(const std::vector<QueryState>& expectedState, QueryState actualState);

    [[nodiscard]] const char* what() const noexcept override;

    [[nodiscard]] QueryState getActualState();

  private:
    std::string message;
    QueryState actualState;
};
}// namespace NES::Exceptions
#endif// NES_CATALOGS_INCLUDE_CATALOGS_EXCEPTIONS_INVALIDQUERYSTATEEXCEPTION_HPP_
