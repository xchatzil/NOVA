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
#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_EXCEPTIONS_SHAREDQUERYPLANNOTFOUNDEXCEPTION_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_EXCEPTIONS_SHAREDQUERYPLANNOTFOUNDEXCEPTION_HPP_
#include <Exceptions/RequestExecutionException.hpp>
#include <Identifiers/Identifiers.hpp>

namespace NES::Exceptions {

/**
 * @brief This exception indicates, that no shared query plan with the given id could be found
 */
class SharedQueryPlanNotFoundException : public RequestExecutionException {

  public:
    /**
     * @brief construct an exception containing a human readable message and a shared query id
     * @param message: A string to indicate to the user what caused the exception
     * @param id: the id of the sqp that was looked up but not found
     */
    SharedQueryPlanNotFoundException(const std::string& message, SharedQueryId id);

    [[nodiscard]] SharedQueryId getSharedQueryId() const;

  private:
    SharedQueryId id;
};
}// namespace NES::Exceptions
#endif// NES_OPTIMIZER_INCLUDE_OPTIMIZER_EXCEPTIONS_SHAREDQUERYPLANNOTFOUNDEXCEPTION_HPP_
