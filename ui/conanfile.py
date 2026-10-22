from conan import ConanFile
from conan.tools.cmake import CMakeToolchain, CMakeDeps, cmake_layout


class VultureUiConan(ConanFile):
    name = "vulture-ui"
    version = "0.1.0"
    settings = "os", "compiler", "build_type", "arch"

    # Заготовка: gRPC-клиент в UI будет на grpc++ из conan (решение 2026-10-22).
    # Версии уточняются в спайке S-2; удалённый репозиторий (Nexus) подключается
    # конфигурацией conan на машине/в CI, а не этим файлом.
    def requirements(self):
        self.requires("grpc/1.84.0")
        self.requires("protobuf/6.33.5")

    def layout(self):
        cmake_layout(self)

    def generate(self):
        CMakeToolchain(self).generate()
        CMakeDeps(self).generate()
