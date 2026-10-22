cmake_minimum_required(VERSION 3.24)
project(vulture-ui VERSION 0.1.0 LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_AUTOMOC ON)

# Если Qt6 не найден и опция выключена, конфигурация проходит без цели vulture-ui.
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
