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
#ifndef NES_RUNTIME_INCLUDE_QUERYCOMPILER_QUERYCOMPILERFORWARDDECLARATION_HPP_
#define NES_RUNTIME_INCLUDE_QUERYCOMPILER_QUERYCOMPILERFORWARDDECLARATION_HPP_
#include <memory>
namespace NES {

class BasicValue;
using BasicValuePtr = std::shared_ptr<BasicValue>;

namespace Runtime {
class NodeEngine;
using NodeEnginePtr = std::shared_ptr<NodeEngine>;

namespace Execution {
class OperatorHandler;
using OperatorHandlerPtr = std::shared_ptr<OperatorHandler>;

class ExecutablePipelineStage;
using ExecutablePipelineStagePtr = std::shared_ptr<ExecutablePipelineStage>;

class ExecutablePipeline;
using ExecutablePipelinePtr = std::shared_ptr<ExecutablePipeline>;

class ExecutableQueryPlan;
using ExecutableQueryPlanPtr = std::shared_ptr<ExecutableQueryPlan>;

}// namespace Execution

}// namespace Runtime

class ExpressionNode;
using ExpressionNodePtr = std::shared_ptr<ExpressionNode>;

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

class LogicalJoinOperator;
using LogicalJoinOperatorPtr = std::shared_ptr<LogicalJoinOperator>;

namespace Join {
class LogicalJoinDescriptor;
using LogicalJoinDescriptorPtr = std::shared_ptr<LogicalJoinDescriptor>;

class JoinOperatorHandler;
using JoinOperatorHandlerPtr = std::shared_ptr<JoinOperatorHandler>;

}// namespace Join

namespace Windowing {

class LogicalWindowDescriptor;
using LogicalWindowDescriptorPtr = std::shared_ptr<LogicalWindowDescriptor>;

class WindowOperatorHandler;
using WindowOperatorHandlerPtr = std::shared_ptr<WindowOperatorHandler>;

class WatermarkStrategyDescriptor;
using WatermarkStrategyDescriptorPtr = std::shared_ptr<WatermarkStrategyDescriptor>;

class WindowAggregationDescriptor;
using WindowAggregationDescriptorPtr = std::shared_ptr<WindowAggregationDescriptor>;

}// namespace Windowing

class Operator;
using OperatorPtr = std::shared_ptr<Operator>;

class LogicalOperator;
using LogicalOperatorPtr = std::shared_ptr<LogicalOperator>;

class DecomposedQueryPlan;
using DecomposedQueryPlanPtr = std::shared_ptr<DecomposedQueryPlan>;

class SourceDescriptor;
using SourceDescriptorPtr = std::shared_ptr<SourceDescriptor>;

class SinkDescriptor;
using SinkDescriptorPtr = std::shared_ptr<SinkDescriptor>;

namespace QueryCompilation {

class PipelineContext;
using PipelineContextPtr = std::shared_ptr<PipelineContext>;

class CodeGenerator;
using CodeGeneratorPtr = std::shared_ptr<CodeGenerator>;

class QueryCompilationError;
using QueryCompilationErrorPtr = std::shared_ptr<QueryCompilationError>;

class QueryCompilationRequest;
using QueryCompilationRequestPtr = std::shared_ptr<QueryCompilationRequest>;

class QueryCompilationResult;
using QueryCompilationResultPtr = std::shared_ptr<QueryCompilationResult>;

class QueryCompiler;
using QueryCompilerPtr = std::shared_ptr<QueryCompiler>;

class QueryCompilerOptions;
using QueryCompilerOptionsPtr = std::shared_ptr<QueryCompilerOptions>;

class OperatorPipeline;
using OperatorPipelinePtr = std::shared_ptr<OperatorPipeline>;

class LowerLogicalToPhysicalOperators;
using LowerLogicalToPhysicalOperatorsPtr = std::shared_ptr<LowerLogicalToPhysicalOperators>;

class PhysicalOperatorProvider;
using PhysicalOperatorProviderPtr = std::shared_ptr<PhysicalOperatorProvider>;

class GeneratableOperatorProvider;
using GeneratableOperatorProviderPtr = std::shared_ptr<GeneratableOperatorProvider>;

class LowerPhysicalToGeneratableOperators;
using LowerPhysicalToGeneratableOperatorsPtr = std::shared_ptr<LowerPhysicalToGeneratableOperators>;

class LowerToExecutableQueryPlanPhase;
using LowerToExecutableQueryPlanPhasePtr = std::shared_ptr<LowerToExecutableQueryPlanPhase>;

class PipelineQueryPlan;
using PipelineQueryPlanPtr = std::shared_ptr<PipelineQueryPlan>;

class AddScanAndEmitPhase;
using AddScanAndEmitPhasePtr = std::shared_ptr<AddScanAndEmitPhase>;

class BufferOptimizationPhase;
using BufferOptimizationPhasePtr = std::shared_ptr<BufferOptimizationPhase>;

class PredicationOptimizationPhase;
using PredicationOptimizationPhasePtr = std::shared_ptr<PredicationOptimizationPhase>;

class PipeliningPhase;
using PipeliningPhasePtr = std::shared_ptr<PipeliningPhase>;

class OperatorFusionPolicy;
using OperatorFusionPolicyPtr = std::shared_ptr<OperatorFusionPolicy>;

class DataSinkProvider;
using DataSinkProviderPtr = std::shared_ptr<DataSinkProvider>;

class DefaultDataSourceProvider;
using DataSourceProviderPtr = std::shared_ptr<DefaultDataSourceProvider>;
namespace Phases {

class PhaseFactory;
using PhaseFactoryPtr = std::shared_ptr<PhaseFactory>;

}// namespace Phases

namespace PhysicalOperators {
class PhysicalOperator;
using PhysicalOperatorPtr = std::shared_ptr<PhysicalOperator>;

class PhysicalCountMinBuildOperator;
class PhysicalHyperLogLogBuildOperator;
}// namespace PhysicalOperators

}// namespace QueryCompilation

}// namespace NES

#endif// NES_RUNTIME_INCLUDE_QUERYCOMPILER_QUERYCOMPILERFORWARDDECLARATION_HPP_
