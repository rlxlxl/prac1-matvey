#ifndef SQL_PARSER_H
#define SQL_PARSER_H

#include <string>
#include <vector>
#include <memory>

enum class QueryType {
    SELECT,
    INSERT,
    DELETE,
    UNKNOWN
};

struct SelectColumn {
    std::string tableName;
    std::string columnName;
};

struct Condition {
    std::string leftTable;
    std::string leftColumn;
    std::string operator_;
    std::string rightTable;
    std::string rightColumn;
    std::string rightValue; // For literal values
    bool isLiteral;
    
    std::string logicalOp; // "AND" or "OR"
};

struct SelectQuery {
    std::vector<SelectColumn> columns;
    std::vector<std::string> tables;
    std::vector<Condition> conditions;
};

struct InsertQuery {
    std::string tableName;
    std::vector<std::string> values;
};

struct DeleteQuery {
    std::string tableName;
    std::vector<Condition> conditions;
};

class SQLParser {
public:
    static QueryType parseQueryType(const std::string& query);
    static SelectQuery parseSelect(const std::string& query);
    static InsertQuery parseInsert(const std::string& query);
    static DeleteQuery parseDelete(const std::string& query);
    
private:
    static std::vector<std::string> tokenize(const std::string& query);
    static std::string trim(const std::string& str);
    static std::string removeQuotes(const std::string& str);
    static Condition parseCondition(const std::vector<std::string>& tokens, size_t& pos);
};

#endif

