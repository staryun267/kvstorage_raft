#include <iostream>

#include "clerk.h"
#include "util.h"

int main()
{
    Clerk client;
    client.Init("test.conf");
    auto start = now();
    client.Put("X", "1");
    client.Put("A", "252");
    client.Put("C", "563635");
    client.Put("G", "87878");
    client.Put("B", "00000000");
    client.Put("Y", "14765386");
    client.Put("N", "114514");
    client.Put("M", "69699696");
    std::cout << client.Get("X") << std::endl;
    std::cout << client.Get("A") << std::endl;
    std::cout << client.Get("C") << std::endl;
    std::cout << client.Get("G") << std::endl;
    std::cout << client.Get("B") << std::endl;
    std::cout << client.Get("Y") << std::endl;
    std::cout << client.Get("N") << std::endl;
    std::cout << client.Get("M") << std::endl;
    return 0;
}