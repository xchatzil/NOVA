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

#ifndef NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WINDOWS_JOINS_JOINFORWARDREFS_HPP_
#define NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WINDOWS_JOINS_JOINFORWARDREFS_HPP_
#include <memory>

namespace NES {

class FieldAccessExpressionNode;
using FieldAccessExpressionNodePtr = std::shared_ptr<FieldAccessExpressionNode>;

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

namespace Join {
class LogicalJoinDescriptor;
using LogicalJoinDescriptorPtr = std::shared_ptr<LogicalJoinDescriptor>;

namespace Experimental {
class LogicalBatchJoinDescriptor;
using LogicalBatchJoinDescriptorPtr = std::shared_ptr<LogicalBatchJoinDescriptor>;
}// namespace Experimental
class JoinActionDescriptor;
using JoinActionDescriptorPtr = std::shared_ptr<JoinActionDescriptor>;

class AbstractJoinHandler;
using AbstractJoinHandlerPtr = std::shared_ptr<AbstractJoinHandler>;

class JoinOperatorHandler;
using JoinOperatorHandlerPtr = std::shared_ptr<JoinOperatorHandler>;

namespace Experimental {
class AbstractBatchJoinHandler;
using AbstractBatchJoinHandlerPtr = std::shared_ptr<AbstractBatchJoinHandler>;

class BatchJoinOperatorHandler;
using BatchJoinOperatorHandlerPtr = std::shared_ptr<BatchJoinOperatorHandler>;
}// namespace Experimental
}// namespace Join
}// namespace NES
#endif// NES_OPERATORS_INCLUDE_OPERATORS_LOGICALOPERATORS_WINDOWS_JOINS_JOINFORWARDREFS_HPP_
