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

#ifndef NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_CONVERTLOGICALTOPHYSICALSINK_HPP_
#define NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_CONVERTLOGICALTOPHYSICALSINK_HPP_

#include <Identifiers/Identifiers.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>
namespace NES {

class Schema;
using SchemaPtr = std::shared_ptr<Schema>;

/**
 * @brief This class is responsible for creating the physical sink from Logical sink description
 */
class ConvertLogicalToPhysicalSink {

  public:
    /**
     * @brief This method is responsible for creating the physical sink from logical sink descriptor
     * @param sinkDescriptor: logical sink descriptor
     * @param nodeEngine: the running node engine where the sink is deployed
     * @param querySubPlanId: the id of the owning subplan
     * @return Data sink pointer representing the physical sink
     */
    static DataSinkPtr createDataSink(OperatorId operatorId,
                                      const SinkDescriptorPtr& sinkDescriptor,
                                      const SchemaPtr& schema,
                                      const Runtime::NodeEnginePtr& nodeEngine,
                                      const QueryCompilation::PipelineQueryPlanPtr& pipelineQueryPlan,
                                      size_t numOfProducers);

  private:
    ConvertLogicalToPhysicalSink() = default;
};

}// namespace NES

#endif// NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_CONVERTLOGICALTOPHYSICALSINK_HPP_
