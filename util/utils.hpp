/*
  Copyright (c) 2014-2016 DataStax

  Licensed under the Apache License, Version 2.0 (the "License");
  you may not use this file except in compliance with the License.
  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
*/

#ifndef __CASS_COMMON_HPP_INCLUDED__
#define __CASS_COMMON_HPP_INCLUDED__

#include "macros.hpp"

#include <stddef.h>
#include <stdint.h>
#include <string>
#include <string.h>
#include <vector>
#include <mutex>

#if defined(_MSC_VER)
#include <intrin.h>
#endif

namespace ig {

    class BufferPiece;
    class Value;

    template<class From, class To>
    #if _MSC_VER && !__INTEL_COMPILER
    class IsConvertible : public std::is_convertible<From, To> {
    #else
    class IsConvertible {
      private:
        typedef char Yes;
        typedef struct { char not_used[2]; } No;

        struct Helper {
          static Yes test(To);
          static No test(...);
          static From& check();
        };

      public:
        static const bool value = sizeof(Helper::test(Helper::check())) == sizeof(Yes);
    #endif
    };

    int32_t get_pid();

} // namespace ig

#endif
