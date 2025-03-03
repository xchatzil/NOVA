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

#ifndef NES_WINDOW_TYPES_INCLUDE_TYPES_CONTENTBASEDWINDOWTYPE_HPP_
#define NES_WINDOW_TYPES_INCLUDE_TYPES_CONTENTBASEDWINDOWTYPE_HPP_

#include <Types/WindowType.hpp>
#include <vector>

namespace NES::Windowing {

class ContentBasedWindowType;
using ContentBasedWindowTypePtr = std::shared_ptr<ContentBasedWindowType>;
class ThresholdWindow;
using ThresholdWindowPtr = std::shared_ptr<ThresholdWindow>;

class ContentBasedWindowType : public WindowType {

  public:
    enum class ContentBasedSubWindowType : uint8_t { THRESHOLDWINDOW };

    explicit ContentBasedWindowType();

    ~ContentBasedWindowType() override = default;

    /**
     * @brief getter for the SubWindowType, i.e., Thresholdwindow
     * @return the SubWindowType
    */
    virtual ContentBasedSubWindowType getContentBasedSubWindowType() = 0;

    /**
       * Cast the current window type as a threshold window type
       * @return a shared pointer of ThresholdWindow
       */
    static ThresholdWindowPtr asThresholdWindow(ContentBasedWindowTypePtr contentBasedWindowType);
};
}// namespace NES::Windowing

#endif// NES_WINDOW_TYPES_INCLUDE_TYPES_CONTENTBASEDWINDOWTYPE_HPP_
