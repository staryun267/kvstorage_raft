#include <iostream>
#include "raft.h"
#include "KvServer.h"
#include <unistd.h>
#include <iostream>
#include <random>

//命令参数：raft节点数量 + 配置文件名
void ShowArgsHelp()
{
    std::cout << "format: command -n <RaftNodeNum> -f <ConfigFileName>" << std::endl;
}

int main(int argc, char **argv)
{
    if(argc < 2)
    {
        ShowArgsHelp();
        exit(EXIT_FAILURE);
    }

    int c = 0;
    int nodeNum = 0;
    std::string configFileName;
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_int_distribution<> dis(10000, 29999);
    unsigned short startPort = dis(gen);

    while((c = getopt(argc, argv, "n:f:")) != -1)
    {
        switch (c)
        {
        case 'n':
            nodeNum = atoi(optarg);
            break;
        case 'f':
            configFileName = optarg;
            break;
        default:
            ShowArgsHelp();
            exit(EXIT_FAILURE);
        }
    }

    std::ofstream file(configFileName, std::ios::out | std::ios::trunc);
    if(file.is_open())
    {
        file.close();
        std::cout << "配置文件生成成功" << std::endl;
    }
    else
    {
        std::cout << "文件无法打开" << std::endl;
        exit(EXIT_FAILURE);
    }

    for(int i = 0; i < nodeNum; i++)
    {
        short port = startPort + static_cast<short>(i);
        std::cout << "start to create raftkv node:" << i << "    port:" << port << " pid:" << getpid() << std::endl;
    
        pid_t pid = fork();     //创建子进程

        if(pid == 0)    //子进程
        {
            auto kvServer = new KvServer(i, 10 * 1024 * 1024, configFileName, port);
            pause();
        }
        else if(pid > 0)    //父进程
        {
            sleep(1);
        }
        else
        {
            std::cerr << "进程创建失败" << std::endl;
            exit(EXIT_FAILURE);
        }
    }
    pause();
    return 0;
}