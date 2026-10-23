cmake_minimum_required(VERSION 3.24)
project(vulture-ui VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_AUTOMOC ON)

# --- The vulture.v1 contract for C++ (protoc + grpc_cpp_plugin from conan tool_requires) ---
# Without grpc/protobuf (for example, no conan install) the vulture-proto-cpp target is disabled.
find_package(Protobuf CONFIG QUIET)
find_package(gRPC CONFIG QUIET)
find_program(VULTURE_PROTOC protoc)
find_program(VULTURE_GRPC_CPP_PLUGIN grpc_cpp_plugin)

if(Protobuf_FOUND AND gRPC_FOUND AND VULTURE_PROTOC AND VULTURE_GRPC_CPP_PLUGIN)
    set(VULTURE_PROTO_ROOT "${CMAKE_CURRENT_SOURCE_DIR}/../proto")
    set(VULTURE_PROTO_GEN "${CMAKE_CURRENT_BINARY_DIR}/gen")
    file(MAKE_DIRECTORY "${VULTURE_PROTO_GEN}")
    # Files with services also produce *.grpc.pb.*; common.proto has messages only.
    set(VULTURE_PROTO_MSG_ONLY common)
    set(VULTURE_PROTO_SERVICES stream history control)
    set(VULTURE_PROTO_SRCS "")
    foreach(name ${VULTURE_PROTO_MSG_ONLY} ${VULTURE_PROTO_SERVICES})
        set(base "${VULTURE_PROTO_GEN}/vulture/v1/${name}")
        set(outs "${base}.pb.cc" "${base}.pb.h")
        set(grpc_args "")
        if(name IN_LIST VULTURE_PROTO_SERVICES)
            list(APPEND outs "${base}.grpc.pb.cc" "${base}.grpc.pb.h")
            set(grpc_args "--grpc_out=${VULTURE_PROTO_GEN}"
                          "--plugin=protoc-gen-grpc=${VULTURE_GRPC_CPP_PLUGIN}")
        endif()
        add_custom_command(
            OUTPUT ${outs}
            COMMAND "${VULTURE_PROTOC}" -I "${VULTURE_PROTO_ROOT}"
                    "--cpp_out=${VULTURE_PROTO_GEN}" ${grpc_args}
                    "${VULTURE_PROTO_ROOT}/vulture/v1/${name}.proto"
            DEPENDS "${VULTURE_PROTO_ROOT}/vulture/v1/${name}.proto"
            COMMENT "protoc vulture/v1/${name}.proto"
            VERBATIM)
        list(APPEND VULTURE_PROTO_SRCS ${outs})
    endforeach()
    add_library(vulture-proto-cpp STATIC ${VULTURE_PROTO_SRCS})
    target_include_directories(vulture-proto-cpp PUBLIC "${VULTURE_PROTO_GEN}")
    target_link_libraries(vulture-proto-cpp PUBLIC gRPC::grpc++ protobuf::libprotobuf)
else()
    message(WARNING "protobuf/gRPC/protoc/grpc_cpp_plugin not found: target vulture-proto-cpp is disabled (run conan install)")
endif()

# If Qt6 is not found and the option is off, configuration succeeds without the vulture-ui target.
option(VULTURE_UI_REQUIRE_QT "Fail configuration if Qt6 is not found" OFF)

if(VULTURE_UI_REQUIRE_QT)
    find_package(Qt6 REQUIRED COMPONENTS Widgets)
else()
    find_package(Qt6 QUIET COMPONENTS Widgets)
endif()

if(NOT Qt6_FOUND)
    message(WARNING "Qt6 not found: target vulture-ui is disabled")
    return()
endif()

qt_add_executable(vulture-ui src/main.cpp)
target_link_libraries(vulture-ui PRIVATE Qt6::Widgets)

if(TARGET vulture-proto-cpp)
    target_link_libraries(vulture-ui PRIVATE vulture-proto-cpp)
endif()
