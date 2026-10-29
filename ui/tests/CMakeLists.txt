# Qt Test targets. Everything runs without a display (QT_QPA_PLATFORM=offscreen is set on each test).
add_library(vulture-test-support STATIC FakeStreamServer.cpp FakeStreamServer.h TestCerts.h)
target_include_directories(vulture-test-support PUBLIC ${CMAKE_CURRENT_SOURCE_DIR})
target_link_libraries(vulture-test-support PUBLIC vulture-proto-cpp)

function(vulture_add_test name)
    add_executable(${name} ${name}.cpp)
    target_link_libraries(${name} PRIVATE vulture-ui-core vulture-test-support Qt6::Test)
    target_compile_options(${name} PRIVATE
        $<$<CXX_COMPILER_ID:MSVC>:/W4>
        $<$<NOT:$<CXX_COMPILER_ID:MSVC>>:-Wall -Wextra>)
    add_test(NAME ${name} COMMAND ${name})
    set_tests_properties(${name} PROPERTIES ENVIRONMENT "QT_QPA_PLATFORM=offscreen" TIMEOUT 120)
endfunction()

vulture_add_test(tst_Util)
vulture_add_test(tst_Models)
vulture_add_test(tst_App)
vulture_add_test(tst_StreamClient)
vulture_add_test(tst_Columns)
vulture_add_test(tst_Chart)
target_compile_definitions(tst_Chart PRIVATE VULTURE_FIXTURES_DIR="${CMAKE_CURRENT_SOURCE_DIR}/fixtures")
vulture_add_test(tst_History)
vulture_add_test(tst_Smoke)
vulture_add_test(tst_Perf)
set_tests_properties(tst_Perf PROPERTIES LABELS "perf" TIMEOUT 240)
