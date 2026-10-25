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
    find_package(Qt6 REQUIRED COMPONENTS Widgets Network)
else()
    find_package(Qt6 QUIET COMPONENTS Widgets Network)
endif()

if(NOT Qt6_FOUND)
    message(WARNING "Qt6 not found: target vulture-ui is disabled")
    return()
endif()

if(NOT TARGET vulture-proto-cpp)
    message(WARNING "vulture-proto-cpp is disabled: target vulture-ui is disabled (run conan install)")
    return()
endif()

# --- Vendored QCustomPlot (GPLv3), warnings silenced for this directory only ---
add_library(vulture-qcustomplot STATIC third_party/qcustomplot/qcustomplot.cpp third_party/qcustomplot/qcustomplot.h)
set_target_properties(vulture-qcustomplot PROPERTIES AUTOMOC ON)
target_include_directories(vulture-qcustomplot SYSTEM PUBLIC third_party/qcustomplot)
# QT_NO_PRINTER: the bundled header then does not need Qt PrintSupport (see third_party/qcustomplot/README.md).
target_compile_definitions(vulture-qcustomplot PUBLIC QT_NO_PRINTER)
target_link_libraries(vulture-qcustomplot PUBLIC Qt6::Widgets)
target_compile_options(vulture-qcustomplot PRIVATE
    $<$<CXX_COMPILER_ID:MSVC>:/w>
    $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-w>)

# --- Application core: everything except main(), so the tests can link it ---
set(VULTURE_UI_CORE_SOURCES
    src/util/RingBuffer.h
    src/util/Format.h src/util/Format.cpp
    src/util/Backoff.h src/util/Backoff.cpp
    src/util/SemanticColors.h src/util/SemanticColors.cpp
    src/net/ReplayCursor.h src/net/ReplayCursor.cpp
    src/model/OiSeries.h src/model/OiSeries.cpp
    src/model/IngestQueue.h src/model/IngestQueue.cpp
    src/chart/Timeframe.h
    src/chart/Klines.h src/chart/Klines.cpp
    src/chart/PriceSource.h src/chart/PriceSource.cpp
    src/chart/ChartModel.h src/chart/ChartModel.cpp
    src/chart/ChartController.h src/chart/ChartController.cpp
    src/chart/ChartView.h
    src/chart/QcpChartView.h src/chart/QcpChartView.cpp
    src/ui/TableHeader.h src/ui/TableHeader.cpp
    src/ui/ChartPanel.h src/ui/ChartPanel.cpp
    src/model/FeedEvent.h
    src/model/DisplayOptions.h
    src/model/FeedStore.h src/model/FeedStore.cpp
    src/model/FeedModel.h src/model/FeedModel.cpp
    src/model/SymbolModel.h src/model/SymbolModel.cpp
    src/model/Favorites.h src/model/Favorites.cpp
    src/model/SessionStats.h src/model/SessionStats.cpp
    src/model/LiveData.h src/model/LiveData.cpp
    src/model/CsvExport.h src/model/CsvExport.cpp
    src/net/ConnectionConfig.h src/net/ConnectionConfig.cpp
    src/net/ConnectionStatus.h
    src/net/ErrorText.h src/net/ErrorText.cpp
    src/net/StreamClient.h src/net/StreamClient.cpp
    src/app/Settings.h src/app/Settings.cpp
    src/app/LargeLiquidationNotifier.h src/app/LargeLiquidationNotifier.cpp
    src/ui/StatusIndicator.h src/ui/StatusIndicator.cpp
    src/ui/ConnectionDialog.h src/ui/ConnectionDialog.cpp
    src/ui/PreferencesDialog.h src/ui/PreferencesDialog.cpp
    src/ui/SymbolsPanel.h src/ui/SymbolsPanel.cpp
    src/ui/FeedPanel.h src/ui/FeedPanel.cpp
    src/ui/SummaryPanel.h src/ui/SummaryPanel.cpp
    src/ui/ExchangeTab.h src/ui/ExchangeTab.cpp
    src/ui/AdminPanel.h src/ui/AdminPanel.cpp
    src/ui/MainWindow.h src/ui/MainWindow.cpp
)
add_library(vulture-ui-core STATIC ${VULTURE_UI_CORE_SOURCES})
target_include_directories(vulture-ui-core PUBLIC src)
target_compile_definitions(vulture-ui-core PUBLIC VULTURE_UI_VERSION="${PROJECT_VERSION}")
target_link_libraries(vulture-ui-core PUBLIC Qt6::Widgets Qt6::Network vulture-proto-cpp vulture-qcustomplot)
target_compile_options(vulture-ui-core PRIVATE
    $<$<CXX_COMPILER_ID:MSVC>:/W4>
    $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-Wall -Wextra>)

qt_add_executable(vulture-ui src/main.cpp)
target_link_libraries(vulture-ui PRIVATE vulture-ui-core)
target_compile_options(vulture-ui PRIVATE
    $<$<CXX_COMPILER_ID:MSVC>:/W4>
    $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-Wall -Wextra>)
set_target_properties(vulture-ui PROPERTIES WIN32_EXECUTABLE ON MACOSX_BUNDLE OFF)

# --- Tests (Qt Test + CTest); built only when Qt Test is available ---
include(CTest)
if(BUILD_TESTING)
    find_package(Qt6 QUIET OPTIONAL_COMPONENTS Test)
    if(TARGET Qt6::Test)
        add_subdirectory(tests)
    else()
        message(WARNING "Qt6::Test not found: tests are disabled")
    endif()
endif()
