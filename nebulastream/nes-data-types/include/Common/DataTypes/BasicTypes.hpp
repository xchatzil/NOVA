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

#ifndef NES_DATA_TYPES_INCLUDE_COMMON_DATATYPES_BASICTYPES_HPP_
#define NES_DATA_TYPES_INCLUDE_COMMON_DATATYPES_BASICTYPES_HPP_

#include <stdint.h>

namespace NES {

enum class BasicType : uint8_t {
    INT8,
    UINT8,
    INT16,
    UINT16,
    INT32,
    UINT32,
    INT64,
    FLOAT32,
    UINT64,
    FLOAT64,
    BOOLEAN,
    CHAR,
};

}

#endif// NES_DATA_TYPES_INCLUDE_COMMON_DATATYPES_BASICTYPES_HPP_
