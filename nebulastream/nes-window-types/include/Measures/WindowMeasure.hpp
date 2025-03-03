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

#ifndef NES_WINDOW_TYPES_INCLUDE_MEASURES_WINDOWMEASURE_HPP_
#define NES_WINDOW_TYPES_INCLUDE_MEASURES_WINDOWMEASURE_HPP_
#include <string>
namespace NES::Windowing {
/**
 * Defines the measure of a window, common measures are time and count.
 */
class WindowMeasure {
    /**
     * @brief Creates a string representation
     * @return std::string
     */
    virtual std::string toString() const = 0;

  public:
    virtual ~WindowMeasure() = default;
};

}// namespace NES::Windowing

#endif// NES_WINDOW_TYPES_INCLUDE_MEASURES_WINDOWMEASURE_HPP_
