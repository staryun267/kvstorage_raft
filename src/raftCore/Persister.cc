#include "Persister.h"
#include "util.h"

void Persister::Save(const std::string raftState, const std::string snapshot)
{
    std::lock_guard<std::mutex> lock(m_mtx);
    clearRaftStateAndSnapshot();

    //将数据写入文件
    m_raftStateOutStream << raftState;
    m_snapshotOutStream << snapshot;

}

std::string Persister::ReadSnapshot()
{
    std::lock_guard<std::mutex> lock(m_mtx);
    if(m_snapshotOutStream.is_open())
    {
        m_snapshotOutStream.close();
    }

    DEFER{
        m_snapshotOutStream.open(m_snapshotFileName);
    };

    std::fstream ifs(m_snapshotFileName, std::ios_base::in);
    if(!ifs.good())
    {
        return "";
    }

    std::string snapshot;
    ifs >> snapshot;
    ifs.close();
    return snapshot;
}

void Persister::SaveRaftState(const std::string &data)
{


    std::lock_guard<std::mutex> lock(m_mtx);

    // std::cout << "=== SaveRaftState ===" << std::endl;
    // std::cout << "File: " << m_raftStateFileName << std::endl;
    // std::cout << "Data size: " << data.size() << std::endl;
    // std::cout << "Data content (first 50 chars): " << data.substr(0, 50) << std::endl;

    clearRaftState();

    // std::cout << "After clearRaftState, stream is_open: " << m_raftStateOutStream.is_open() << std::endl;
    // std::cout << "Stream good: " << m_raftStateOutStream.good() << std::endl;
    // std::cout << "Stream fail: " << m_raftStateOutStream.fail() << std::endl;

    //std::cout << "----------------------------------------+++++++++++++++++++++++++++++++-----------------------------------------" << std::endl;
    //std::cout << "Writing to: " << std::filesystem::absolute(m_raftStateFileName) << std::endl;
    //std::cout << "----------------------------------------+++++++++++++++++++++++++++++++-----------------------------------------" << std::endl;
    
    m_raftStateOutStream << data;
    m_raftStateSize += data.size();
}

long long Persister::RaftStateSize()
{
    std::lock_guard<std::mutex> lock(m_mtx);

    return m_raftStateSize;
}

std::string Persister::ReadRaftState()
{
    std::lock_guard<std::mutex> lock(m_mtx);

    std::fstream ifs(m_raftStateFileName, std::ios_base::in);
    if(!ifs.good())
    {
        return "";
    }


    std::string raftState;
    ifs >> raftState;
    ifs.close();
    return raftState;
}

Persister::Persister(const int me)
    : m_raftStateFileName("raftstatePersister" + std::to_string(me) + ".txt"),
      m_snapshotFileName("snapshot" + std::to_string(me) + ".txt"),
      m_raftStateSize(0)
{
    bool fileOpenFlag = true;
    std::fstream file(m_raftStateFileName, std::ios::out | std::ios::trunc);
    if(file.is_open())
    {
        file.close();
    }
    else
    {
        fileOpenFlag = false;
    }

    file = std::fstream(m_snapshotFileName, std::ios::out | std::ios::trunc);
    if(file.is_open())
    {
        file.close();
    }
    else
    {
        fileOpenFlag = false;
    }

    if(!fileOpenFlag)
    {
        DPrint("[func-Persister::Persister] file open error");
    }
    m_raftStateOutStream.open(m_raftStateFileName);
    m_snapshotOutStream.open(m_snapshotFileName); 
}

Persister::~Persister()
{
    if(m_raftStateOutStream.is_open())
    {
        m_raftStateOutStream.close();
    }

    if(m_snapshotOutStream.is_open())
    {
        m_snapshotOutStream.close();
    }
}

void Persister::clearRaftState()
{
    m_raftStateSize = 0;
    if(m_raftStateOutStream.is_open())
    {
        m_raftStateOutStream.close();
    }

    //重新打开，并清空文件内容
    m_raftStateOutStream.open(m_raftStateFileName, std::ios::out | std::ios::trunc);
    //std::cout << "clearRaftState: opened " << m_raftStateFileName << ", is_open: " << m_raftStateOutStream.is_open() << std::endl;
}

void Persister::clearSnapshot()
{
    if(m_snapshotOutStream.is_open())
    {
        m_snapshotOutStream.close();
    }

    m_snapshotOutStream.open(m_snapshotFileName, std::ios::out | std::ios::trunc);
}

void Persister::clearRaftStateAndSnapshot()
{
    clearRaftState();
    clearSnapshot();
}