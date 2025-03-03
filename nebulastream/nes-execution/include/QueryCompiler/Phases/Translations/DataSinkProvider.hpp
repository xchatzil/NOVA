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

#ifndef NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_DATASINKPROVIDER_HPP_
#define NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_DATASINKPROVIDER_HPP_

#include <Identifiers/Identifiers.hpp>
#include <QueryCompiler/QueryCompilerForwardDeclaration.hpp>
#include <Runtime/RuntimeForwardRefs.hpp>

namespace NES::QueryCompilation {
/**
 * @brief Provider to transform a sink descriptor to executable DataSink
 */
class DataSinkProvider {
  public:
    /**
     * @brief Factory method for creating a DataSinkProvider
     * @return DataSinkProvider
     */
    static DataSinkProviderPtr create();

    /**
     * @brief Lowers a sink descriptor to a executable data sink.
     * @param operatorId id of this sink
     * @param sinkDescriptor the sink descriptor
     * @param schema the schema of the sink
     * @param nodeEngine the node engine
     * @param querySubPlanId
     * @return DataSinkPtr
     */
    virtual DataSinkPtr lower(OperatorId sinkId,
                              SinkDescriptorPtr sinkDescriptor,
                              SchemaPtr schema,
                              Runtime::NodeEnginePtr nodeEngine,
                              const QueryCompilation::PipelineQueryPlanPtr& querySubPlan,
                              size_t numOfProducers);

    virtual ~DataSinkProvider() = default;
};
}// namespace NES::QueryCompilation

#endif// NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_DATASINKPROVIDER_HPP_
