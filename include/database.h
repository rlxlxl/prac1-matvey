#ifndef DATABASE_H
#define DATABASE_H

#include "config.h"
#include "sql_parser.h"
#include "file_manager.h"
#include <string>
#include <vector>
#include <map>

class Database {
private:
    DatabaseConfig config;
    std::string schemaName;
    
    bool evaluateCondition(const Condition& cond, 
                          const std::map<std::string, std::vector<std::string>>& rowData,
                          const std::map<std::string, std::vector<std::string>>& tableHeaders);
    
    bool evaluateConditions(const std::vector<Condition>& conditions,
                           const std::map<std::string, std::vector<std::string>>& rowData,
                           const std::map<std::string, std::vector<std::string>>& tableHeaders);
    
    std::vector<std::string> getTableHeader(const std::string& tablePath, const std::string& tableName);
    
public:
    Database(const DatabaseConfig& config);
    
    void initialize();
    std::vector<std::vector<std::string>> executeSelect(const SelectQuery& query);
    void executeInsert(const InsertQuery& query);
    void executeDelete(const DeleteQuery& query);
};

#endif

