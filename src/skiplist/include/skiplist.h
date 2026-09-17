#include <iostream>
#include <cstdlib>
#include <cmath>
#include <cstring>
#include <mutex>
#include <fstream>

#define STORE_FILE "store/dumpFile"

static std::string delimiter = ":";

//node
template <typename K, typename V>
class Node
{
public:
    Node(){}

    Node(K k, V v, int);

    ~Node();

    K getK() const;

    V getV() const;

    void setV(V);

    Node<K, V> **forward;

    int level;
private:
    K key;
    V value;
};

//节点的构造函数
template <typename K, typename V>
Node<K, V>::Node(K k, V v, int level)
{
    this->key = k;
    this->value = v;
    this->level = level;

    //节点数组，存储当前节点的所有指向的节点的指针
    this->forward = new Node<K, V>*[level + 1];

    memset(this->forward, 0, sizeof(Node<K, V>*) * (level + 1));
}

template <typename K, typename V>
Node<K, V>::~Node()
{
    delete []forward;
}

template <typename K, typename V>
K Node<K, V>::getK() const
{
    return this->key;
}

template <typename K, typename V>
V Node<K, V>::getV() const
{
    return this->value;
}

template <typename K, typename V>
void Node<K, V>::setV(V v)
{
    this->value = v;
}

template <typename K, typename V>
class SkipListDump
{
public:
    friend class boost::serialization::access;

    template <class Archive>
    void serialize(Archive &ar, const unsigned int version)
    {
        ar &keyDumpVt_;
        ar &valDumpVt_;
    }
    std::vector<K> keyDumpVt_;
    std::vector<V> valDumpVt_;

public:
    void insert(const Node<K, V> &node);
};

//skiplist
template <typename K, typename V>
class SkipList
{
public:
    SkipList(int);
    ~SkipList();

    int get_random_level();

    Node<K, V>* create_node(K, V, int);

    int insert_element(K, V);
    void insert_set_element(K &, V &);
    void display_list();
    bool search_element(K, V &);
    void delete_element(K);
    std::string dump_file();
    void load_file(const std::string &dumpStr);

    //递归删除节点
    void clear(Node<K, V>*);
    int size();

private:
    void get_key_value_from_string(const std::string& str, std::string* key, std::string* value);
    bool is_valid_string(const std::string& str);
private:
    //跳表最大的层级
    int _max_level;

    //跳表当前的层级
    int _skip_list_level;

    //头节点指针
    Node<K, V>* _header;

    //文件操作
    std::ofstream _file_writer;
    std::ifstream _file_reader;

    //跳表中的元素数量
    int _element_count;

    std::mutex mtx;
};

//创建节点
template <typename K, typename V>
Node<K, V>* SkipList<K, V>::create_node(K k, V v, int level)
{
    Node<K, V>* n = new Node<K, V>(k, v, level);
    return n;
}

//插入节点
template <typename K, typename V>
int SkipList<K, V>::insert_element(K key, V value)
{
    mtx.lock();

    Node<K, V>* current = this->_header;

    //存放插入节点前一个位置的node的array
    Node<K, V>* update[_max_level + 1];
    memset(update, 0, sizeof(Node<K, V>*) * (_max_level + 1));

    //寻找需要更新的节点并放入update中
    for(int i = this->_skip_list_level; i >= 0; i--)
    {
        while(current->forward[i] != nullptr && current->forward[i]->getK() < key)
        {
            current = current->forward[i];
        }
        update[i] = current;
    }

    current = current->forward[0];

    //检查要插入的key是否已经存在于跳表中
    //在跳表中
    if(current != nullptr && current->getK() == key)
    {
        //std::cout << "key: " << key << "exists!" << std::endl;
        mtx.unlock();
        return 1;
    }

    //不在跳表中
    if(current == nullptr || current->getK() != key)
    {
        //获取随机层级
        int random_level = get_random_level();

        //随机层级大于当前层级，则需要在update中添加多出来的头节点(大于当前所处层级的说明还未进行过节点的插入，只有头节点)
        if(random_level > _skip_list_level)
        {
            for(int i = _skip_list_level + 1; i < random_level + 1; i++)
            {
                update[i] = _header;
            }

            _skip_list_level = random_level;
        }

        //为需要插入的数据创建节点
        Node<K, V>* insert_node = create_node(key, value, random_level);

        //插入
        for(int i = 0; i <= random_level; i++)
        {
            insert_node->forward[i] = update[i]->forward[i];
            update[i]->forward[i] = insert_node;
        }

        //std::cout << "successfully insert key: " << key << "value" << value << std::endl;
        _element_count++;
    }
    mtx.unlock();

    return 0;
}

//插入元素，存在则修改
template <typename K, typename V>
void SkipList<K, V>::insert_set_element(K &key, V &val)
{
    V oldValue;
    if(search_element(key, oldValue))
    {
        delete_element(key);
    }
    insert_element(key, val);
}

//display skiplist
template <typename K, typename V>
void SkipList<K, V>::display_list()
{
    //std::cout << "\n*******SkipList********" << "\n";
    for(int i = 0; i <= _skip_list_level; i++)
    {
        Node<K, V>* node = this->_header->forward[i];
        //std::cout << "level" << i << ": ";
        while(node != nullptr)
        {
            //std::cout << node->getK() << ":" << node->getV() << "; ";
            node = node->forward[i];
        }
        //std::cout << std::endl;
    }
}

//dump date to file
template <typename K, typename V>
std::string SkipList<K, V>::dump_file()
{
    //std::cout << "dump_file..............." << std::endl;
    //_file_writer.open(STORE_FILE);

    Node<K, V>* node = this->_header->forward[0];
    SkipListDump<K, V> dumper;

    while(node != nullptr)
    {
        dumper.insert(*node);
        //_file_writer << node->getK() << ":" << node->getV() << "\n";
        //std::cout << node->getK() << ":" << node->getV() << ";\n";
        node = node->forward[0];
    }

    std::stringstream ss;
    boost::archive::text_oarchive oa(ss);
    oa << dumper;
    //_file_writer.flush();
    //_file_writer.close();

    return ss.str();
}

//load date from disk
template <typename K, typename V>
void SkipList<K, V>::load_file(const std::string &dumpStr)
{
    // _file_reader.open(STORE_FILE);
    // std::string line;
    // std::string* key = new std::string();
    // std::string* value = new std::string();

    // while(getline(_file_reader, line))
    // {
    //     get_key_value_from_string(line, key, value);
    //     if(key->empty() || value->empty())
    //     {
    //         continue;
    //     }

    //     insert_element(stoi(*key), *value);
    //     std::cout << "key: " << *key << "value: " << *value << std::endl;
    // }

    // delete key;
    // delete value;
    // _file_reader.close();

    if(dumpStr.empty())
    {
        return;
    }
    SkipListDump<K, V> dumper;
    std::stringstream iss(dumpStr);
    boost::archive::text_iarchive ia(iss);
    ia >> dumper;
    for(int i = 0; i < dumper.keyDumpVt_.size(); i++)
    {
        insert_element(dumper.keyDumpVt_[i], dumper.valDumpVt_[i]);
    }
}

//获取跳表中的元素个数
template <typename K, typename V>
int SkipList<K, V>::size()
{
    return _element_count;
}

//根据字符串获取key和value
template <typename K, typename V>
void SkipList<K, V>::get_key_value_from_string(const std::string& str, std::string* key, std::string* value)
{
    if(!is_valid_string(str))
    {
        return;
    }

    *key = str.substr(0, str.find(delimiter));
    *value = str.substr(str.find(delimiter) + 1, str.size());
}

//判断字符串格式是否合法
template <typename K, typename V>
bool SkipList<K, V>::is_valid_string(const std::string& str)
{
    if(str.empty())
    {
        return false;
    }

    if(str.find(delimiter) == std::string::npos)
    {
        return false;
    }

    return true;
}

//delete element from skiplist
template <typename K, typename V>
void SkipList<K, V>::delete_element(K key)
{
    mtx.lock();

    Node<K, V>* current = this->_header;
    Node<K, V>* update[_max_level + 1];

    for(int i = _skip_list_level; i >= 0; i--)
    {
        while(current->forward[i] != nullptr && current->forward[i]->getK() < key)
        {
            current = current->forward[i];
        }

        update[i] = current;
    }

    current = current->forward[0];

    //删除操作
    if(current != nullptr && current->getK() == key)
    {
        for(int i = 0; i <= _skip_list_level; i++)
        {
            if(update[i]->forward[i] != current)
            {
                break;
            }

            update[i]->forward[i] = current->forward[i];
        }

        // 删除空的层级
        while (_skip_list_level > 0 && _header->forward[_skip_list_level] == 0)
        {
            _skip_list_level--;
        }

        //std::cout << "successfully delete key: " << key << std::endl;
        delete current;
        _element_count--;
    }
    mtx.unlock();
    return;
}

//查找元素
template <typename K, typename V>
bool SkipList<K, V>::search_element(K key, V &value)
{
    //std::cout << "search element..............." << std::endl;

    Node<K, V>* current = _header;

    for(int i = _skip_list_level; i >= 0; i--)
    {
        while(current->forward[i] != nullptr && current->forward[i]->getK() < key)
        {
            current = current->forward[i];
        }
    }

    current = current->forward[0];

    if(current != nullptr && current->getK() == key)
    {
        value = current->getV();
        //std::cout << "find key: " << key << "value: " << current->getV() << std::endl;
        return true;
    }

    //std::cout << "Not find key: " << key << std::endl;
    return false;
}

//跳表构造函数
template <typename K, typename V>
SkipList<K, V>::SkipList(int max_level)
{
    this->_max_level = max_level;
    this->_skip_list_level = 0;
    this->_element_count = 0;

    K k;
    V v;

    this->_header = new Node<K, V>(k, v, _max_level);
}

//跳表析构函数
template <typename K, typename V>
SkipList<K, V>::~SkipList()
{
    if(_file_reader.is_open())
    {
        _file_reader.close();
    }

    if(_file_writer.is_open())
    {
        _file_writer.close();
    }

    //递归删除跳表节点
    if(_header->forward[0] != nullptr)
    {
        clear(_header->forward[0]);
    }
    delete(_header);
}

template <typename K, typename V>
void SkipList<K, V>::clear(Node<K, V>* cur)
{
    if(cur->forward[0] != nullptr)
    {
        clear(cur->forward[0]);
    }
    delete(cur);
}

//获取随机层级
template <typename K, typename V>
int SkipList<K, V>::get_random_level()
{
    int k = 1;
    while(rand() % 2)
    {
        k++;
    }

    return (k < _max_level) ? k : _max_level;
}

template <typename K, typename V>
void SkipListDump<K, V>::insert(const Node<K, V> &node)
{
    keyDumpVt_.emplace_back(node.getK());
    valDumpVt_.emplace_back(node.getV());
}