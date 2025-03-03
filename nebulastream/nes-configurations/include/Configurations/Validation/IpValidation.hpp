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

#ifndef NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_VALIDATION_IPVALIDATION_HPP_
#define NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_VALIDATION_IPVALIDATION_HPP_

#include "Configurations/Validation/ConfigurationValidation.hpp"
#include <string>

namespace NES::Configurations {

/**
 * @brief This class implements ip validation for ip configuration options.
 */
class IpValidation : public ConfigurationValidation {
  public:
    /**
     * @brief Method to check the validity of an ip address
     * @param ip ip address
     * @return success if validated
     */
    bool isValid(const std::string& ip) const override;
};
}// namespace NES::Configurations

#endif// NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_VALIDATION_IPVALIDATION_HPP_
