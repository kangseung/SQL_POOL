//
// Created by jiangsheng on 2020/11/13.
//

#ifndef JSFAST2020_JSDB_HPP
#define JSFAST2020_JSDB_HPP

#include <iostream>
#include <string>
#include <unordered_map>

#include "clickhouse/client.h"
#include "libpq/libpq-fe.h"

struct DBPassWord {
  std::string IPaddress = "127.0.0.1";
  std::string port = "5432";

  std::string pg_account = "postgres";
  std::string pg_password = "password";

  std::string ch_account = "default";
  std::string ch_password = "password";
};

class JSPGQuery {
 public:
  explicit JSPGQuery() : m_res(nullptr) {}

  void reset_first() { m_first = true; }

  bool get_data(const std::string &command, PGconn *connect,
                std::unordered_map<std::string, std::string> &result) {
    if (m_first) {
      // init query
      int result = PQsendQuery(connect, command.c_str());
      PQsetSingleRowMode(connect);
      m_first = false;
    }

    m_res = PQgetResult(connect);

    int nFields = PQnfields(m_res);
    int rows = PQntuples(m_res);
    for (int i = 0; i < nFields; i++) {
      if (rows == 1) {
        result[PQfname(m_res, i)] = PQgetvalue(m_res, rows - 1, i);
      }
    }

    PQclear(m_res);

    bool have_data = !(rows == 0 || nFields == 0);
    if (!have_data) {
      // init query
      m_res = PQgetResult(connect);
    }

    return have_data;
  }

  bool get_data(const std::string &command, PGconn *connect,
                std::unordered_map<int, std::string> &result) {
    if (m_first) {
      // init query
      int result = PQsendQuery(connect, command.c_str());
      PQsetSingleRowMode(connect);
      m_first = false;
    }

    m_res = PQgetResult(connect);

    int nFields = PQnfields(m_res);
    int rows = PQntuples(m_res);
    for (int i = 0; i < nFields; i++) {
      if (rows == 1) {
        result[i] = PQgetvalue(m_res, rows - 1, i);
      }
    }

    PQclear(m_res);

    bool have_data = !(rows == 0 || nFields == 0);
    if (!have_data) {
      // init query
      m_res = PQgetResult(connect);
    }

    return have_data;
  }

  ~JSPGQuery() {}

 private:
  PGresult *m_res;
  bool m_first = true;
};

class JSPGEXERES {
 public:
  JSPGEXERES(const std::string &command, PGconn *connect) {
    m_res = PQexec(connect, command.c_str());
    if (PQresultStatus(m_res) != PGRES_COMMAND_OK) {
      fprintf(stderr, "DECLARE CURSOR failed: %s", PQerrorMessage(connect));
      m_exeRes = false;
    }
    m_exeRes = true;
  }

  bool exeResult() const { return m_exeRes; }

  ~JSPGEXERES() { PQclear(m_res); }

 private:
  bool m_exeRes;
  PGresult *m_res;
};

class JSPGDB {
 public:
  JSPGDB(const std::string &database_name) : m_dbname(database_name) {
    m_connect =
        PQsetdbLogin(get_ipaddress().c_str(), get_port().c_str(), "", "",
                     database_name.c_str(), get_pg_account().c_str(),
                     get_pg_password().c_str());
    if (PQstatus(m_connect) != CONNECTION_OK) {
      std::cout << "pgsql connect error" << std::endl;
      exit(0);
    }
  }

  ~JSPGDB() {
    if (m_connect != nullptr) {
      PQfinish(m_connect);
    }
  }

  bool is_alive() {
    if (PQstatus(m_connect) == CONNECTION_OK) {
      return true;
    } else {
      return false;
    }
  }

  bool execute(const std::string &command) {
    auto pg_res = JSPGEXERES(command, m_connect);
    return pg_res.exeResult();
  }

  bool Query(const std::string &command,
             std::unordered_map<std::string, std::string> &result) {
    return m_jspgquery.get_data(command, m_connect, result);
  }

  bool Query(const std::string &command,
             std::unordered_map<int, std::string> &result) {
    return m_jspgquery.get_data(command, m_connect, result);
  }

  inline std::string get_dbname() const { return m_dbname; }

  void reset_first() { m_jspgquery.reset_first(); }

 private:
  std::string m_dbname;

  PGconn *m_connect = nullptr;

  inline std::string get_pg_account() const { return m_password.pg_account; }

  inline std::string get_pg_password() const { return m_password.pg_password; }

  inline std::string get_ipaddress() const { return m_password.IPaddress; }

  inline std::string get_port() const { return m_password.port; }

  DBPassWord m_password;

  JSPGQuery m_jspgquery;
};

class JSCHDB {
 public:
  explicit JSCHDB(const std::string &db_name) : m_dbname(db_name) {
    auto c = clickhouse::ClientOptions();
    c.SetHost("127.0.0.1");
    c.SetDefaultDatabase(db_name);
    c.SetUser(get_ch_account());
    c.SetPassword(get_ch_password());
    c.SetPort(9000);
    m_client = std::make_shared<clickhouse::Client>(c);
  }

  std::shared_ptr<clickhouse::Client> get_ch() { return m_client; }

  std::string get_dbname() { return m_dbname; }

 private:
  std::string m_dbname;

  inline std::string get_ch_account() const { return m_password.ch_account; }

  inline std::string get_ch_password() const { return m_password.ch_password; }

  std::shared_ptr<clickhouse::Client> m_client;

  DBPassWord m_password;
};

#endif  // JSFAST2020_JSDB_HPP
