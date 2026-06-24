#define CATCH_CONFIG_RUNNER
#include <catch2/catch.hpp>
#include <seastar/core/app-template.hh>
#include <seastar/core/thread.hh>
#include <iostream>

namespace ragedb::gql {
    void clear_registries();
}

struct TestCleanupListener : Catch::TestEventListenerBase {
    using TestEventListenerBase::TestEventListenerBase;

    void testCaseStarting(Catch::TestCaseInfo const& testInfo) override {
        std::cout << "DEBUG: testCaseStarting: " << testInfo.name << std::endl;
        ragedb::gql::clear_registries();
    }

    void testCaseEnded(Catch::TestCaseStats const& testCaseStats) override {
        std::cout << "DEBUG: testCaseEnded: " << testCaseStats.testInfo.name << std::endl;
        ragedb::gql::clear_registries();
    }
};
CATCH_REGISTER_LISTENER(TestCleanupListener)

int main(int argc, char** argv) {
    seastar::app_template app;
    int result = 0;
    try {
        char* dummy_argv[] = { argv[0], const_cast<char*>("-c"), const_cast<char*>("1"), const_cast<char*>("-m"), const_cast<char*>("1024M"), nullptr };
        app.run(5, dummy_argv, [&result, argc, argv] {
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
