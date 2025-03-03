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

#ifndef NES_COMMON_INCLUDE_EXCEPTIONS_COORDINATESOUTOFRANGEEXCEPTION_HPP_
#define NES_COMMON_INCLUDE_EXCEPTIONS_COORDINATESOUTOFRANGEEXCEPTION_HPP_

#include <exception>

namespace NES::Spatial::Exception {

/**
 * @brief an exception indicating that the entered latitude is not in range [-90, 90] or the longitude is not in range [-180, 180]
 */
class CoordinatesOutOfRangeException : public std::exception {
    const char* what() const noexcept;
};
}// namespace NES::Spatial::Exception

#endif// NES_COMMON_INCLUDE_EXCEPTIONS_COORDINATESOUTOFRANGEEXCEPTION_HPP_
