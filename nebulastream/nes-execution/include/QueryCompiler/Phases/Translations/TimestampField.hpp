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

#ifndef NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_TIMESTAMPFIELD_HPP_
#define NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_TIMESTAMPFIELD_HPP_
#include <API/TimeUnit.hpp>
#include <Execution/Expressions/ReadFieldExpression.hpp>
#include <Execution/Operators/Streaming/TimeFunction.hpp>
#include <cstdint>
#include <ostream>
#include <string>
#include <utility>

namespace NES::QueryCompilation {
/**
 * A TimestampField is a wrapper around a FieldName and a Unit of time.
 * This enforces fields carrying time values to be evaluated with respect to a specific timeunit.
 */
class TimestampField {
    enum TimeFunctionType {
        EVENT_TIME,
        INGESTION_TIME,
    };

  public:
    friend std::ostream& operator<<(std::ostream& os, const TimestampField& obj);
    /**
     * The multiplier is the value which converts from the underlying time value to milliseconds.
     * E.g. the multiplier for a timestampfield of seconds is 1000
     * @return Unit Multiplier
     */
    [[nodiscard]] Windowing::TimeUnit getUnit() const;

    /**
     * Name of the field
     * @return reference to the field name
     */
    [[nodiscard]] const std::string& getName() const;

    [[nodiscard]] const TimeFunctionType& getTimeFunctionType() const;

    /**
     * Builds the TimeFunction
     * @return reference to the field name
     */
    [[nodiscard]] Runtime::Execution::Operators::TimeFunctionPtr toTimeFunction() const;
    static TimestampField IngestionTime();
    static TimestampField EventTime(std::string fieldName, Windowing::TimeUnit tm);

  private:
    std::string fieldName;
    Windowing::TimeUnit unit;
    TimeFunctionType timeFunctionType;
    TimestampField(std::string fieldName, Windowing::TimeUnit unit, TimeFunctionType timeFunctionType);
};
}// namespace NES::QueryCompilation

#endif// NES_EXECUTION_INCLUDE_QUERYCOMPILER_PHASES_TRANSLATIONS_TIMESTAMPFIELD_HPP_
