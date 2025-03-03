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

#include <Runtime/TaggedPointer.hpp>
#include <Runtime/detail/TupleBufferImpl.hpp>
#include <Util/Logger/Logger.hpp>

namespace NES {

template<typename T>
TaggedPointer<T>::TaggedPointer(T* ptr, uint16_t tag) {
    reset(ptr, tag);
}

template<typename T>
TaggedPointer<T>& TaggedPointer<T>::operator=(T* ptr) {
    reset(ptr);
    return *this;
}

template<typename T>
TaggedPointer<T>::operator bool() const {
    return get() != nullptr;
}

template<typename T>
void TaggedPointer<T>::reset(T* ptr, uint16_t tag) {
    uintptr_t pointer = reinterpret_cast<uintptr_t>(ptr);
    NES_ASSERT(!(pointer >> 48), "invalid pointer");
    pointer |= static_cast<uintptr_t>(tag) << 48;
    data = pointer;
}

// explicit instantiation of tagged ptr
template class TaggedPointer<Runtime::detail::BufferControlBlock>;

}// namespace NES
