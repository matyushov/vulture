from conan import ConanFile
from conan.tools.cmake import CMake, CMakeToolchain, CMakeDeps, cmake_layout


class VultureUiConan(ConanFile):
    name = "vulture-ui"
    version = "0.1.0"
    settings = "os", "compiler", "build_type", "arch"
    # require_qt=True: конфигурация падает, если Qt6 не найден (так собирает CI).
    options = {"require_qt": [True, False]}
    default_options = {
        "require_qt": False,
        # Лишние плагины grpc не нужны (долгая сборка, больше бинарников в Nexus);
        # остаётся cpp_plugin для генерации C++ из proto/. Паттерн действует и на
        # tool_requires. Опции влияют на package_id: менять только на ветке vendor/*.
        "grpc/*:shared": False,
        "grpc/*:cpp_plugin": True,
        "grpc/*:csharp_plugin": False,
        "grpc/*:node_plugin": False,
        "grpc/*:objective_c_plugin": False,
        "grpc/*:php_plugin": False,
        "grpc/*:python_plugin": False,
        "grpc/*:ruby_plugin": False,
    }

    # Заготовка: gRPC-клиент в UI будет на grpc++ из conan (решение 2026-10-22).
    # Версии уточняются в спайке S-2; удалённый репозиторий (Nexus) подключается
    # конфигурацией conan на машине/в CI, а не этим файлом.
    def requirements(self):
        self.requires("grpc/1.84.0")
        self.requires("protobuf/6.33.5")

    # protoc и grpc_cpp_plugin для генерации C++ из proto/ (см. CMakeLists.txt).
    # Попадают в PATH через conanbuild-окружение (VirtualBuildEnv).
    def build_requirements(self):
        self.tool_requires("protobuf/6.33.5")
        self.tool_requires("grpc/1.84.0")

    def layout(self):
        cmake_layout(self)

    def generate(self):
        tc = CMakeToolchain(self)
        tc.cache_variables["VULTURE_UI_REQUIRE_QT"] = "ON" if self.options.require_qt else "OFF"
        tc.generate()
        CMakeDeps(self).generate()

    def build(self):
        cmake = CMake(self)
        cmake.configure()
        cmake.build()
