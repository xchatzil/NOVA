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

#ifndef NES_COMMON_INCLUDE_PRECOMPILEDHEADERS_HPP_
#define NES_COMMON_INCLUDE_PRECOMPILEDHEADERS_HPP_
// This is a list of often used and expensive header libraries that we want to precompile

// Commonly-used first-party headers that do not often change (e.g., logging, basic components)
#include <Util/Logger/Logger.hpp>
#include <Util/Logger/impl/NesLogger.hpp>

// Commonly used Standard Library or third-party headers
#include <algorithm>
#include <chrono>
#include <filesystem>
#include <gtest/gtest.h>
#include <iostream>
#include <memory>
#include <string>
#include <unordered_map>
#include <vector>

#endif// NES_COMMON_INCLUDE_PRECOMPILEDHEADERS_HPP_
