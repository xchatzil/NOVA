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

#include <Nodes/Node.hpp>
#include <Plans/DecomposedQueryPlan/DecomposedQueryPlan.hpp>
#include <QueryCompiler/Operators/OperatorPipeline.hpp>
#include <QueryCompiler/Operators/PipelineQueryPlan.hpp>
#include <Util/DumpHandler/ConsoleDumpHandler.hpp>
#include <Util/Logger/Logger.hpp>
#include <iostream>

namespace NES {

ConsoleDumpHandler::ConsoleDumpHandler(std::ostream& out) : out(out) {}

std::shared_ptr<ConsoleDumpHandler> ConsoleDumpHandler::create(std::ostream& out) {
    return std::make_shared<ConsoleDumpHandler>(out);
}

void ConsoleDumpHandler::dumpHelper(NodePtr const& op, uint64_t depth, uint64_t indent, std::ostream& out) const {
    out << std::string(indent * depth, ' ') << op->toString() << std::endl;
    ++depth;
    auto children = op->getChildren();
    for (auto&& child : children) {
        dumpHelper(child, depth, indent, out);
    }
}

void ConsoleDumpHandler::multilineDumpHelper(const NodePtr& op, uint64_t depth, uint64_t indent, std::ostream& out) const {

    std::vector<std::string> multiLineNodeString = op->toMultilineString();
    for (const std::string& line : multiLineNodeString) {
        for (auto i{0ull}; i < indent * depth; ++i) {
            if (i % indent == 0) {
                out << '|';
            } else {
                if (line == multiLineNodeString.front() && i >= indent * depth - 1) {
                    out << std::string(indent, '-');
                } else {
                    out << std::string(indent, ' ');
                }
            }
        }
        if (line != multiLineNodeString.front()) {
            out << '|' << ' ';
        }
        out << line << std::endl;
    }
    ++depth;
    auto children = op->getChildren();
    for (auto&& child : children) {
        multilineDumpHelper(child, depth, indent, out);
    }
}

void ConsoleDumpHandler::dump(const NodePtr node) { multilineDumpHelper(node, /*depth*/ 0, /*indent*/ 2, out); }

void ConsoleDumpHandler::dump(std::string, std::string, DecomposedQueryPlanPtr decomposedQueryPlan) {
    out << "Dumping pipelineQueryPlan: " << decomposedQueryPlan->toString() << std::endl;
}

void ConsoleDumpHandler::dump(std::string, std::string, QueryCompilation::PipelineQueryPlanPtr pipelineQueryPlan) {
    out << "Dumping pipelineQueryPlan: " << pipelineQueryPlan->toString() << std::endl;
}
}// namespace NES
