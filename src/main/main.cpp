#include <seastar/core/app-template.hh>
#include <seastar/core/reactor.hh>
#include <iostream>

int main(int argc, char** argv) {
    seastar::app_template app;
    try {
        app.run(argc, argv, [] {
            std::cout << "Hello world!\n";
            std::cout << "This server has " << seastar::smp::count << " cores.\n";
            return seastar::make_ready_future<>();
        });
    } catch (...) {
        std::cerr << "Failed to start RageDB: "
                  << std::current_exception() << "\n";
        return 1;
    }
    return 0;
}