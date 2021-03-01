#include "jsdb.hpp"
#include "sql_pool.hpp"
int main() {
  map_pool m_map_sql_pool;
  m_map_sql_pool.add_map_pool("monitor", postgresql_POOL_TYPE, 5);
  m_map_sql_pool.add_map_pool("ticks_timescale_full", clickhouse_POOL_TYPE, 5);
  {
    //   m_map_sql_pool.add_map_pool("kbars2", 2);
    auto monitor_pool = m_map_sql_pool.get_pool("monitor");
    std::string sql_command = "SELECT * FROM trading_day;";
    for (int k = 0; k <= 1000; ++k) {
      // JSPGDB db("monitor");
      int j = 0;
      auto connection = monitor_pool->get_connection();
      std::string test;
      while (true) {
        std::unordered_map<int, std::string> result;
        bool haveData = connection->get_pg_db()->Query(sql_command, result);
        //   bool haveData = db.Query(sql_command, result);
        if (haveData) {
          test = result[0];
          ++j;
        } else {
          break;
        }
      }
      std::cout << "k" << k << "j" << j << std::endl;
      monitor_pool->put_connection(connection);
    }
  }

  //------------------------------------------------------------------
  {
    auto ticks_pool = m_map_sql_pool.get_pool("ticks_timescale_full");
    std::string sql_command = "SELECT * FROM rb limit 10000;";
    for (int k = 0; k <= 1000; ++k) {
      // JSPGDB db("monitor");
      int j = 0;
      auto connection = ticks_pool->get_connection();
      auto client = connection->get_ch_db()->get_ch();
      client->Select(sql_command, [&](const clickhouse::Block &block) {
        //--------------------------------------------------------------------------
        for (size_t i = 0; i < block.GetRowCount(); ++i) {
          auto abc = block[5]->As<clickhouse::ColumnFloat64>()->At(i);
          ++j;
        }
      });

      std::cout << "k" << k << "j" << j << std::endl;
      ticks_pool->put_connection(connection);
    }
  }
  return 0;
}