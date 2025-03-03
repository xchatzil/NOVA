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

#ifndef NES_COORDINATOR_INCLUDE_SERVICES_PLACEMENTAMENDMENT_PLACEMENTAMENDMENTINSTANCE_HPP_
#define NES_COORDINATOR_INCLUDE_SERVICES_PLACEMENTAMENDMENT_PLACEMENTAMENDMENTINSTANCE_HPP_

#include <Util/RequestType.hpp>
#include <future>
#include <memory>

namespace NES {

class GlobalQueryPlan;
using GlobalQueryPlanPtr = std::shared_ptr<GlobalQueryPlan>;

class SharedQueryPlan;
using SharedQueryPlanPtr = std::shared_ptr<SharedQueryPlan>;

class DeploymentPhase;
using DeploymentPhasePtr = std::shared_ptr<DeploymentPhase>;

class ReconfigurationMarker;
using ReconfigurationMarkerPtr = std::shared_ptr<ReconfigurationMarker>;

namespace Configurations {
class CoordinatorConfiguration;
using CoordinatorConfigurationPtr = std::shared_ptr<CoordinatorConfiguration>;
}// namespace Configurations

class Topology;
using TopologyPtr = std::shared_ptr<Topology>;

namespace Optimizer {

class GlobalExecutionPlan;
using GlobalExecutionPlanPtr = std::shared_ptr<GlobalExecutionPlan>;

class TypeInferencePhase;
using TypeInferencePhasePtr = std::shared_ptr<TypeInferencePhase>;

class PlacementAmendmentInstance;
using PlacementAmendmentInstancePtr = std::shared_ptr<PlacementAmendmentInstance>;

class PlacementAmendmentHandler;
struct DeploymentUnit;

/**
 * @brief class representing the placement amendment instance
 */
class PlacementAmendmentInstance {
  public:
    /** Create placement amendment instance
     * @brief
     * @param sharedQueryPlan
     * @param globalExecutionPlan
     * @param topology
     * @param typeInferencePhase
     * @param coordinatorConfiguration
     * @param deploymentPhase
     * @return a shared pointer to the placement amendment instance
     */
    static PlacementAmendmentInstancePtr create(SharedQueryPlanPtr sharedQueryPlan,
                                                Optimizer::GlobalExecutionPlanPtr globalExecutionPlan,
                                                TopologyPtr topology,
                                                TypeInferencePhasePtr typeInferencePhase,
                                                Configurations::CoordinatorConfigurationPtr coordinatorConfiguration,
                                                DeploymentPhasePtr deploymentPhase);

    PlacementAmendmentInstance(SharedQueryPlanPtr sharedQueryPlan,
                               Optimizer::GlobalExecutionPlanPtr globalExecutionPlan,
                               TopologyPtr topology,
                               TypeInferencePhasePtr typeInferencePhase,
                               Configurations::CoordinatorConfigurationPtr coordinatorConfiguration,
                               DeploymentPhasePtr deploymentPhase);

    /**
     * @brief Get promise to check if the amendment instance was processed
     * @return
     */
    std::future<bool> getFuture();

    /**
     * @warning only used by the mocks for testing purposes
     */
    void setPromise(bool promise);

  protected:
    /**
     * @brief Perform the placement amendment
     * @return true if success else false
     */
    void execute();

  private:
    /**
     * @brief Compute reconfiguration marker for reconfiguring the plan
     * @param deploymentUnit: the deployment unit using which the marker needs to be computed
     * @return pointer to the ReconfigurationMarker
     */
    ReconfigurationMarkerPtr computeReconfigurationMarker(DeploymentUnit& deploymentUnit);

    SharedQueryPlanPtr sharedQueryPlan;
    Optimizer::GlobalExecutionPlanPtr globalExecutionPlan;
    TopologyPtr topology;
    TypeInferencePhasePtr typeInferencePhase;
    Configurations::CoordinatorConfigurationPtr coordinatorConfiguration;
    DeploymentPhasePtr deploymentPhase;
    std::promise<bool> completionPromise;

    //declare as friend to allow accessing execute method
    friend PlacementAmendmentHandler;
};
}// namespace Optimizer
}// namespace NES
#endif// NES_COORDINATOR_INCLUDE_SERVICES_PLACEMENTAMENDMENT_PLACEMENTAMENDMENTINSTANCE_HPP_
