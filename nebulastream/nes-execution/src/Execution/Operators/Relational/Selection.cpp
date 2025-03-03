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

#include <Execution/Operators/Relational/Selection.hpp>
#include <Nautilus/Interface/Record.hpp>

namespace NES::Runtime::Execution::Operators {

void Selection::execute(ExecutionContext& ctx, Record& record) const {
    // evaluate expression and call child operator if expression is valid
    if (expression->execute(record)) {
        if (child != nullptr) {
            child->execute(ctx, record);
        }
    }
}

}// namespace NES::Runtime::Execution::Operators
