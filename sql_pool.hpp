#ifndef SQL_POOL_HPP
#define SQL_POOL_HPP
#include <condition_variable>
#include <memory>
#include <mutex>
#include <queue>

#include "jsdb.hpp"

enum SQL_POOL_TYPE { postgresql_POOL_TYPE, clickhouse_POOL_TYPE };

class SQL_Connection {
 public:
  SQL_Connection(const std::string &db_name, SQL_POOL_TYPE pool_type)
      : m_pool_type(pool_type) {
    if (m_pool_type == postgresql_POOL_TYPE) {
      m_pg_db = std::make_shared<JSPGDB>(db_name);
    } else if (m_pool_type == clickhouse_POOL_TYPE) {
      m_ch_db = std::make_shared<JSCHDB>(db_name);
    }
  }

  std::shared_ptr<JSPGDB> get_pg_db() { return m_pg_db; }

  std::shared_ptr<JSCHDB> get_ch_db() { return m_ch_db; }

  bool check_connection_is_alive() {
    if (m_pool_type == postgresql_POOL_TYPE) {
      return m_pg_db->is_alive();
    } else if (m_pool_type == clickhouse_POOL_TYPE) {
      return true;
    }

    return false;
  }

  SQL_POOL_TYPE get_pool_type() { return m_pool_type; }

 private:
  std::shared_ptr<JSPGDB> m_pg_db = nullptr;
  std::shared_ptr<JSCHDB> m_ch_db = nullptr;
  SQL_POOL_TYPE m_pool_type;
};
class SQL_POOL {
 public:
  explicit SQL_POOL(const std::string &db_name, SQL_POOL_TYPE pool_type,
                    int pool_size) {
    for (int i = 0; i < pool_size; ++i) {
      auto ptr = std::make_shared<SQL_Connection>(db_name, pool_type);
      m_pool.push(ptr);
    }
  }

  std::shared_ptr<SQL_Connection> get_connection() {
    std::unique_lock<std::mutex> locking(m_mutex);
    while (m_pool.empty()) {
      std::cout << "pool size empty " << std::endl;
      m_condition.wait(locking);
    }
    std::shared_ptr<SQL_Connection> conn;
    while (true) {
      conn = m_pool.front();
      m_pool.pop();
      if (conn->check_connection_is_alive()) {
        break;
      } else {
        std::cout << "conn is not alive! recreate connection" << std::endl;
        auto pool_type = conn->get_pool_type();
        if (pool_type == postgresql_POOL_TYPE) {
          auto db_name = conn->get_pg_db()->get_dbname();
          conn = std::make_shared<SQL_Connection>(db_name, pool_type);
          break;
        } else if (pool_type == clickhouse_POOL_TYPE) {
          auto db_name = conn->get_ch_db()->get_dbname();
          conn = std::make_shared<SQL_Connection>(db_name, pool_type);
          break;
        }
      }
    }
    return conn;
  }

  void put_connection(std::shared_ptr<SQL_Connection> conn) {
    std::unique_lock<std::mutex> locking(m_mutex);
    if (conn->get_pool_type() == postgresql_POOL_TYPE) {
      conn->get_pg_db()->reset_first();
    }
    m_pool.push(conn);
    m_condition.notify_one();
  }

 private:
  std::condition_variable m_condition;
  std::queue<std::shared_ptr<SQL_Connection>> m_pool;
  std::mutex m_mutex;
};

class map_pool {
 public:
  void add_map_pool(const std::string db_name, SQL_POOL_TYPE sql_pool_type,
                    int pool_size) {
    auto sql_pool =
        std::make_shared<SQL_POOL>(db_name, sql_pool_type, pool_size);
    m_dbname_pool[db_name] = sql_pool;
  }

  std::shared_ptr<SQL_POOL> get_pool(const std::string &db_name) {
    if (m_dbname_pool.find(db_name) != m_dbname_pool.end()) {
      return m_dbname_pool[db_name];
    } else {
      std::cout << "error not have this db_name" << db_name << std::endl;
      exit(0);
    }
  }

 private:
  std::map<std::string, std::shared_ptr<SQL_POOL>> m_dbname_pool;
};

#endif