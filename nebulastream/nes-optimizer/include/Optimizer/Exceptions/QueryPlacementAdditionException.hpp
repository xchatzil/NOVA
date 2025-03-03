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

#ifndef NES_OPTIMIZER_INCLUDE_OPTIMIZER_EXCEPTIONS_QUERYPLACEMENTADDITIONEXCEPTION_HPP_
#define NES_OPTIMIZER_INCLUDE_OPTIMIZER_EXCEPTIONS_QUERYPLACEMENTADDITIONEXCEPTION_HPP_

#include <Exceptions/RequestExecutionException.hpp>
#include <Identifiers/Identifiers.hpp>
#include <stdexcept>
#include <string>

namespace NES::Exceptions {

/**
 * @brief Exception indicating problem during operator placement addition phase
 */
class QueryPlacementAdditionException : public Exceptions::RequestExecutionException {

  public:
    explicit QueryPlacementAdditionException(SharedQueryId sharedQueryId, const std::string& message);

    const char* what() const noexcept override;
};
}// namespace NES::Exceptions
#endif// NES_OPTIMIZER_INCLUDE_OPTIMIZER_EXCEPTIONS_QUERYPLACEMENTADDITIONEXCEPTION_HPP_
