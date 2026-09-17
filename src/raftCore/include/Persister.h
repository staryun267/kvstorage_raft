#pragma once

#include <fstream>
#include <mutex>

class Persister
{
public:
    void Save(std::string raftState, std::string snapshot);

    std::string ReadSnapshot();

    void SaveRaftState(const std::string& data);

    long long RaftStateSize();

    std::string ReadRaftState();

    explicit Persister(int me);

    ~Persister();
private:
    void clearRaftState();
    void clearSnapshot();
    void clearRaftStateAndSnapshot();
private:
    std::mutex m_mtx;
    std::string m_raftState;
    std::string m_snapshot;

    //raftState文件名
    const std::string m_raftStateFileName;
    //snapshot文件名
    const std::string m_snapshotFileName;

    std::ofstream m_raftStateOutStream;
    std::ofstream m_snapshotOutStream;

    long long m_raftStateSize;
};