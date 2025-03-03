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

#include <Expressions/ExpressionNode.hpp>
#include <Types/ThresholdWindow.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES::Windowing {

ThresholdWindow::ThresholdWindow(ExpressionNodePtr predicate) : ContentBasedWindowType(), predicate(std::move(predicate)) {}

ThresholdWindow::ThresholdWindow(ExpressionNodePtr predicate, uint64_t minCount)
    : ContentBasedWindowType(), predicate(predicate), minimumCount(minCount) {}

WindowTypePtr ThresholdWindow::of(ExpressionNodePtr predicate) {
    return std::reinterpret_pointer_cast<WindowType>(std::make_shared<ThresholdWindow>(ThresholdWindow(std::move(predicate))));
}

WindowTypePtr ThresholdWindow::of(ExpressionNodePtr predicate, uint64_t minimumCount) {
    return std::reinterpret_pointer_cast<WindowType>(
        std::make_shared<ThresholdWindow>(ThresholdWindow(std::move(predicate), minimumCount)));
}

bool ThresholdWindow::equal(WindowTypePtr otherWindowType) {
    if (auto otherThresholdWindow = std::dynamic_pointer_cast<ThresholdWindow>(otherWindowType)) {
        return this->minimumCount == otherThresholdWindow->minimumCount
            && this->predicate->equal(otherThresholdWindow->predicate);
    }
    return false;
}

ContentBasedWindowType::ContentBasedSubWindowType ThresholdWindow::getContentBasedSubWindowType() {
    return ContentBasedSubWindowType::THRESHOLDWINDOW;
}

const ExpressionNodePtr& ThresholdWindow::getPredicate() const { return predicate; }

uint64_t ThresholdWindow::getMinimumCount() const { return minimumCount; }

bool ThresholdWindow::inferStamp(const SchemaPtr& schema) {
    NES_INFO("inferStamp for ThresholdWindow")
    predicate->inferStamp(schema);
    if (!predicate->isPredicate()) {
        NES_THROW_RUNTIME_ERROR("the threshold expression is not a valid predicate");
    }
    return true;
}

std::string ThresholdWindow::toString() const {
    std::stringstream ss;
    ss << "Threshold Window: predicate ";
    ss << predicate->toString();
    ss << "and minimumCount";
    ss << minimumCount;
    ss << std::endl;
    return ss.str();
}

uint64_t ThresholdWindow::hash() const {
    uint64_t hashValue = 0;
    hashValue = hashValue * 0x9e3779b1 + std::hash<uint64_t>{}(minimumCount);
    hashValue = hashValue * 0x9e3779b1 + std::hash<std::string>{}(predicate->toString());
    return hashValue;
}
}// namespace NES::Windowing
