from conan import ConanFile
from conan.tools.cmake import CMake, CMakeToolchain, CMakeDeps, cmake_layout


class VultureUiConan(ConanFile):
    name = "vulture-ui"
    version = "0.1.0"
    settings = "os", "compiler", "build_type", "arch"
    # require_qt=True: configuration fails if Qt6 is not found (this is how CI builds).
    options = {"require_qt": [True, False]}
    default_options = {
        "require_qt": False,
        # Extra grpc plugins are not needed (long build, more binaries in Nexus);
        # cpp_plugin remains for generating C++ from proto/. The pattern also applies to
        # tool_requires. Options affect package_id: change them only on a vendor/* branch.
        "grpc/*:shared": False,
        "grpc/*:cpp_plugin": True,
        "grpc/*:csharp_plugin": False,
        "grpc/*:node_plugin": False,
        "grpc/*:objective_c_plugin": False,
        "grpc/*:php_plugin": False,
        "grpc/*:python_plugin": False,
        "grpc/*:ruby_plugin": False,
    }

    # Placeholder: the gRPC client in the UI will use grpc++ from conan (decision of 2026-10-22).
    # Versions are refined in spike S-2; the remote repository (Nexus) is attached
    # through conan configuration on the machine/in CI, not by this file.
    def requirements(self):
        self.requires("grpc/1.84.0")
        self.requires("protobuf/6.33.5")

    # protoc and grpc_cpp_plugin for generating C++ from proto/ (see CMakeLists.txt).
    # They get onto PATH through the conanbuild environment (VirtualBuildEnv).
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
