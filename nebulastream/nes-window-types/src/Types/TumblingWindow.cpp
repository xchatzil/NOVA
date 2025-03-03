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

#include <API/AttributeField.hpp>
#include <Measures/TimeCharacteristic.hpp>
#include <Types/TumblingWindow.hpp>
#include <Util/Logger/Logger.hpp>
#include <utility>
#include <vector>

namespace NES::Windowing {

TumblingWindow::TumblingWindow(TimeCharacteristicPtr timeCharacteristic, TimeMeasure size)
    : TimeBasedWindowType(std::move(timeCharacteristic)), size(std::move(size)) {}

WindowTypePtr TumblingWindow::of(TimeCharacteristicPtr timeCharacteristic, TimeMeasure size) {
    return std::dynamic_pointer_cast<WindowType>(
        std::make_shared<TumblingWindow>(TumblingWindow(std::move(timeCharacteristic), std::move(size))));
}

TimeMeasure TumblingWindow::getSize() { return size; }

TimeMeasure TumblingWindow::getSlide() { return getSize(); }

std::string TumblingWindow::toString() const {
    std::stringstream ss;
    ss << "TumblingWindow: size=" << size.getTime();
    ss << " timeCharacteristic=" << timeCharacteristic->toString();
    ss << std::endl;
    return ss.str();
}

bool TumblingWindow::equal(WindowTypePtr otherWindowType) {
    if (auto otherTumblingWindow = std::dynamic_pointer_cast<TumblingWindow>(otherWindowType)) {
        return this->size.equals(otherTumblingWindow->size)
            && this->timeCharacteristic->equals(*otherTumblingWindow->timeCharacteristic);
    }
    return false;
}

uint64_t TumblingWindow::hash() const {
    uint64_t hashValue = 0;
    hashValue = hashValue * 0x9e3779b1 + std::hash<uint64_t>{}(size.getTime());
    hashValue = hashValue * 0x9e3779b1 + std::hash<size_t>{}(timeCharacteristic->hash());
    return hashValue;
}
}// namespace NES::Windowing
