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

#include <Configurations/Validation/NonZeroValidation.hpp>
#include <regex>

namespace NES::Configurations {

bool NonZeroValidation::isValid(const std::string& parameter) const {
    std::regex numberRegex("^0.?0?$");
    if (std::regex_match(parameter, numberRegex)) {
        return false;
    }
    return true;
}
}// namespace NES::Configurations
