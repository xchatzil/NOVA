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
#include <QueryCompiler/QueryCompilationRequest.hpp>
#include <utility>

namespace NES::QueryCompilation {

QueryCompilationRequestPtr QueryCompilationRequest::create(DecomposedQueryPlanPtr decomposedQueryPlan,
                                                           Runtime::NodeEnginePtr nodeEngine) {
    return std::make_shared<QueryCompilationRequest>(
        QueryCompilationRequest(std::move(decomposedQueryPlan), std::move(nodeEngine)));
}

QueryCompilationRequest::QueryCompilationRequest(DecomposedQueryPlanPtr decomposedQueryPlan, Runtime::NodeEnginePtr nodeEngine)
    : decomposedQueryPlan(std::move(decomposedQueryPlan)), nodeEngine(std::move(nodeEngine)), debug(false), optimize(false),
      dumpQueryPlans(false) {}

void QueryCompilationRequest::enableDump() { this->dumpQueryPlans = true; }

void QueryCompilationRequest::enableDebugging() { this->debug = true; }

void QueryCompilationRequest::enableOptimizations() { this->optimize = true; }

bool QueryCompilationRequest::isDebugEnabled() const { return debug; }

bool QueryCompilationRequest::isOptimizeEnabled() const { return optimize; }

bool QueryCompilationRequest::isDumpEnabled() const { return dumpQueryPlans; }

DecomposedQueryPlanPtr QueryCompilationRequest::getDecomposedQueryPlan() { return decomposedQueryPlan; }

Runtime::NodeEnginePtr QueryCompilationRequest::getNodeEngine() { return nodeEngine; }

}// namespace NES::QueryCompilation
