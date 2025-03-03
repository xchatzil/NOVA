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

#include <Plans/Query/QueryPlan.hpp>
#include <Util/DumpHandler/ConsoleDumpHandler.hpp>
#include <Util/DumpHandler/DumpContext.hpp>
#include <Util/Logger/Logger.hpp>
#include <chrono>
#include <iomanip>
#include <sstream>
#include <string>

namespace NES {

DumpContext::DumpContext(std::string contextIdentifier) : context(std::move(contextIdentifier)) {}

DumpContextPtr DumpContext::create() { return std::make_shared<DumpContext>("NullContext"); }

DumpContextPtr DumpContext::create(const std::string& contextIdentifier) {
    // add time to identifier
    auto now = std::chrono::system_clock::now();
    auto in_time_t = std::chrono::system_clock::to_time_t(now);
    std::stringstream ss;
    ss << contextIdentifier << "-" << std::put_time(std::localtime(&in_time_t), "%Y-%m-%d %H:%M:%S");
    return std::make_shared<DumpContext>(ss.str());
}

void DumpContext::registerDumpHandler(const DebugDumpHandlerPtr& debugDumpHandler) { dumpHandlers.push_back(debugDumpHandler); }

void DumpContext::dump(const NodePtr& node) {
    NES_DEBUG("Dump node - {}", context);
    for (auto& handler : dumpHandlers) {
        handler->dump(node);
    }
}

void DumpContext::dump(const std::string& scope, const DecomposedQueryPlanPtr& decomposedQueryPlan) {
    NES_DEBUG("Dump query plan - {} - {}", context, scope);
    for (auto& handler : dumpHandlers) {
        handler->dump(context, scope, decomposedQueryPlan);
    }
}

void DumpContext::dump(const std::string& scope, const QueryCompilation::PipelineQueryPlanPtr& pipelineQueryPlan) {
    NES_DEBUG("Dump pipelined query plan - {} - {}", context, scope)
    for (auto& handler : dumpHandlers) {
        handler->dump(context, scope, pipelineQueryPlan);
    }
}

}// namespace NES
