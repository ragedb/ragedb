#define CATCH_CONFIG_RUNNER
#include <catch2/catch.hpp>
#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>
#include <iostream>

int main(int argc, char** argv) {
    seastar::app_template app;
    int result = 0;
    try {
        char* dummy_argv[] = { argv[0], nullptr };
        app.run(1, dummy_argv, [&result, argc, argv] {
            return seastar::async([&result, argc, argv] {
                Catch::Session session;
                result = session.run(argc, argv);
            });
        });
    } catch (const std::exception& e) {
        std::cerr << "Failed to start Seastar / Catch2 session: " << e.what() << "\n";
        return -1;
    }
    return result;
}
