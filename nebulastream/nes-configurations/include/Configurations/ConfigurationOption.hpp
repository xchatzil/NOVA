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

#ifndef NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_CONFIGURATIONOPTION_HPP_
#define NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_CONFIGURATIONOPTION_HPP_

#include "Configurations/ConfigurationsNames.hpp"
#include "Util/GatheringMode.hpp"
#include "Util/Logger/Logger.hpp"
#include "Util/magicenum/magic_enum.hpp"
#include "Util/yaml/Yaml.hpp"
#include <any>
#include <memory>
#include <sstream>
#include <string>
#include <typeinfo>
#include <utility>

namespace NES::Configurations {

/**
 * @brief Template for a ConfigurationOption object
 * @tparam T template parameter, depends on ConfigOptions
 */
template<class T>
class ConfigurationOption {
  public:
    static std::shared_ptr<ConfigurationOption> create(std::string name, T value, std::string description) {
        return std::make_shared<ConfigurationOption>(ConfigurationOption(name, value, description));
    };

    /**
     * @brief converts a ConfigurationOption Object into human readable format
     * @return string representation of the config
     */
    std::string toString() {
        std::stringstream ss;
        ss << "Name: " << name << "\n";
        ss << "Description: " << description << "\n";
        ss << "Value: " << value << "\n";
        ss << "Default Value: " << defaultValue << "\n";
        return ss.str();
    }

    /**
     * @brief converts config object to human readable form, only prints name and current value
     * @return Name: current Value of config object
     */
    std::string toStringNameCurrentValue() {
        std::stringstream ss;
        ss << name << ": " << value << "\n";
        return ss.str();
    }

    /**
     * @brief converts config object to human readable form, only prints name and current value
     * @return Name: current Value of config object
     */
    std::string toStringNameCurrentValueEnum() {
        std::stringstream ss;
        ss << name << ": " << magic_enum::enum_name(value) << "\n";
        return ss.str();
    }

    /**
     * @brief converts the value of this object into a string
     * @return string of the value of this object
     */
    [[nodiscard]] std::string getValueAsString() const { return std::to_string(value); };

    /**
     * @brief get the name of the ConfigurationOption Object
     * @return name of the config
     */
    std::string getName() { return name; }

    /**
     * @brief get the value of the ConfigurationOption Object
     * @return the value of the config if not set then default value
     */
    [[nodiscard]] T getValue() const { return value; };

    /**
     * @brief sets the value
     * @param value: the value to be used
     */
    void setValue(T value) { this->value = value; }

    /**
     * @brief sets the value if it is defined
     * @param yamlNode: the yaml field name to be used
     */
    void setValueIfDefined(Yaml::Node yamlNode) {
        if (!yamlNode.IsNone()) {
            this->value = yamlNode.As<T>();
        }
    }

    /**
     * @brief get the description of this parameter
     * @return description of the config
     */
    [[nodiscard]] std::string getDescription() const { return description; };

    /**
     * @brief get the default value of this parameter
     * @return: the default value
     */
    [[nodiscard]] T getDefaultValue() const { return defaultValue; };

    /**
     * @brief perform equality check between two config options
     * @param other: other config option
     * @return true if equal else return false
     */
    bool equals(const std::any& other) {
        if (this == other) {
            return true;
        }
        if (other.has_value() && other.type() == typeid(ConfigurationOption)) {
            auto that = (ConfigurationOption<T>) other;
            return this->name == that.name && this->description == that.description && this->value == that.value
                && this->defaultValue == that.defaultValue;
        }
        return false;
    };

    /**
     * @brief converts a string to the appropriate InputFormat enum value and sets it
     * @param inputFormat
     */
    void setInputFormatEnum(std::string inputFormat) {
        if (inputFormat == "CSV") {
            this->value = InputFormat::CSV;
        } else if (inputFormat == "JSON") {
            this->value = InputFormat::JSON;
        } else if (inputFormat == "NES_BINARY") {
            this->value = InputFormat::NES_BINARY;
        } else {
            NES_ERROR("InputFormatEnum: value unknown.");
        }
    }

    /**
     * @brief converts a string to the appropriate TCPDecideMessageSize enum value and sets it
     * @param decideMessageSize
     */
    void setTCPDecideMessageSizeEnum(std::string decideMessageSize) {
        if (decideMessageSize == "TUPLE_SEPARATOR") {
            this->value = TCPDecideMessageSize::TUPLE_SEPARATOR;
        } else if (decideMessageSize == "USER_SPECIFIED_BUFFER_SIZE") {
            this->value = TCPDecideMessageSize::USER_SPECIFIED_BUFFER_SIZE;
        } else if (decideMessageSize == "BUFFER_SIZE_FROM_SOCKET") {
            this->value = TCPDecideMessageSize::BUFFER_SIZE_FROM_SOCKET;
        } else {
            NES_ERROR("TCPDecideMessageSizeEnum: value unknown.");
        }
    }

  private:
    /**
     * @brief Constructs a ConfigurationOption<T> object
     * @param name the name of the object
     * @param value the value of the object
     * @param description default value of the object
     */
    explicit ConfigurationOption(std::string name, T value, std::string description)
        : name(std::move(name)), description(std::move(description)), value(value), defaultValue(value) {}

    ConfigurationOption(std::string name, T value, T defaultValue, std::string description)
        : name(std::move(name)), description(std::move(description)), value(value), defaultValue(defaultValue) {}

    std::string name;
    std::string description;
    T value;
    T defaultValue;
};

using IntConfigOption = std::shared_ptr<ConfigurationOption<uint32_t>>;
using LongConfigOption = std::shared_ptr<ConfigurationOption<uint64_t>>;
using StringConfigOption = std::shared_ptr<ConfigurationOption<std::string>>;
using BoolConfigOption = std::shared_ptr<ConfigurationOption<bool>>;
using FloatConfigOption = std::shared_ptr<ConfigurationOption<float>>;
using CharConfigOption = std::shared_ptr<ConfigurationOption<char>>;
using InputFormatConfigOption = std::shared_ptr<ConfigurationOption<InputFormat>>;
using GatheringModeConfigOption = std::shared_ptr<ConfigurationOption<GatheringMode>>;
using TCPDecideMessageSizeConfigOption = std::shared_ptr<ConfigurationOption<TCPDecideMessageSize>>;

}// namespace NES::Configurations

#endif// NES_CONFIGURATIONS_INCLUDE_CONFIGURATIONS_CONFIGURATIONOPTION_HPP_
