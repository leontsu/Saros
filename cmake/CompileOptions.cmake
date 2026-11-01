set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -fno-omit-frame-pointer")

if (ENABLE_TSAN)
    # ThreadSanitizer replaces AddressSanitizer.
    set(sanitizers "thread")
else ()
    set(sanitizers "address")
    if (ENABLE_UB_SANITIZER)
        # Requires an instrumented libstdc++.
        set(sanitizers "${sanitizers},undefined")
    endif ()
    if (CMAKE_CXX_COMPILER_ID STREQUAL "GNU")
        # do nothing for gcc
    elseif (CMAKE_CXX_COMPILER_ID MATCHES "^(Clang|AppleClang)$")
        set(sanitizers "${sanitizers},nullability")
    else ()
        message(FATAL_ERROR "unsupported compiler ${CMAKE_CXX_COMPILER_ID}")
    endif ()
endif ()

if (ENABLE_SANITIZER OR ENABLE_TSAN)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fsanitize=${sanitizers}")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-sanitize=alignment")
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-sanitize-recover=${sanitizers}")
endif ()
