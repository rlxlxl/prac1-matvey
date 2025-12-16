#include "sql_parser.h"
#include <sstream>
#include <algorithm>
#include <cctype>

QueryType SQLParser::parseQueryType(const std::string& query) {
    std::string upperQuery = query;
    std::transform(upperQuery.begin(), upperQuery.end(), upperQuery.begin(), ::toupper);
    
    if (upperQuery.find("SELECT") == 0) {
        return QueryType::SELECT;
    } else if (upperQuery.find("INSERT") == 0) {
        return QueryType::INSERT;
    } else if (upperQuery.find("DELETE") == 0) {
        return QueryType::DELETE;
    }
    
    return QueryType::UNKNOWN;
}

std::vector<std::string> SQLParser::tokenize(const std::string& query) {
    std::vector<std::string> tokens;
    std::string current;
    bool inQuotes = false;
    
    for (size_t i = 0; i < query.length(); ++i) {
        char c = query[i];
        
        if (c == '\'') {
            inQuotes = !inQuotes;
            current += c;
        } else if (inQuotes) {
            current += c;
        } else if (std::isspace(c) || c == ',' || c == '(' || c == ')' || c == '=') {
            if (!current.empty()) {
                tokens.push_back(current);
                current.clear();
            }
            if (c == ',' || c == '(' || c == ')' || c == '=') {
                tokens.push_back(std::string(1, c));
            }
        } else {
            current += c;
        }
    }
    
    if (!current.empty()) {
        tokens.push_back(current);
    }
    
    return tokens;
}

std::string SQLParser::trim(const std::string& str) {
    size_t first = str.find_first_not_of(" \t\n\r");
    if (first == std::string::npos) return "";
    size_t last = str.find_last_not_of(" \t\n\r");
    return str.substr(first, last - first + 1);
}

std::string SQLParser::removeQuotes(const std::string& str) {
    std::string result = trim(str);
    if (result.length() >= 2 && result[0] == '\'' && result[result.length() - 1] == '\'') {
        return result.substr(1, result.length() - 2);
    }
    return result;
}

static std::string toUpper(const std::string& str) {
    std::string result = str;
    std::transform(result.begin(), result.end(), result.begin(), ::toupper);
    return result;
}

Condition SQLParser::parseCondition(const std::vector<std::string>& tokens, size_t& pos) {
    Condition cond;
    cond.logicalOp = "";
    
    // Left side: table.column
    if (pos < tokens.size()) {
        std::string left = tokens[pos++];
        size_t dotPos = left.find('.');
        if (dotPos != std::string::npos) {
            cond.leftTable = left.substr(0, dotPos);
            cond.leftColumn = left.substr(dotPos + 1);
        }
    }
    
    // Operator (=)
    if (pos < tokens.size() && tokens[pos] == "=") {
        cond.operator_ = tokens[pos++];
    }
    
    // Right side
    if (pos < tokens.size()) {
        std::string right = tokens[pos];
        
        if (right[0] == '\'') {
            // Literal value
            cond.isLiteral = true;
            cond.rightValue = removeQuotes(right);
            pos++;
        } else {
            // Column reference
            cond.isLiteral = false;
            size_t dotPos = right.find('.');
            if (dotPos != std::string::npos) {
                cond.rightTable = right.substr(0, dotPos);
                cond.rightColumn = right.substr(dotPos + 1);
            }
            pos++;
        }
    }
    
    // Check for AND/OR
    if (pos < tokens.size()) {
        std::string upper = toUpper(tokens[pos]);
        if (upper == "AND" || upper == "OR") {
            cond.logicalOp = upper;
            pos++;
        }
    }
    
    return cond;
}

SelectQuery SQLParser::parseSelect(const std::string& query) {
    SelectQuery selectQuery;
    auto tokens = tokenize(query);
    
    size_t pos = 1; // Skip SELECT
    
    // Parse columns
    while (pos < tokens.size() && toUpper(tokens[pos]) != "FROM") {
        if (tokens[pos] != "," && tokens[pos] != " ") {
            std::string col = tokens[pos];
            size_t dotPos = col.find('.');
            if (dotPos != std::string::npos) {
                SelectColumn selectCol;
                selectCol.tableName = col.substr(0, dotPos);
                selectCol.columnName = col.substr(dotPos + 1);
                selectQuery.columns.push_back(selectCol);
            }
        }
        pos++;
    }
    
    // Skip FROM
    if (pos < tokens.size() && toUpper(tokens[pos]) == "FROM") {
        pos++;
    }
    
    // Parse tables
    while (pos < tokens.size() && toUpper(tokens[pos]) != "WHERE") {
        if (tokens[pos] != "," && tokens[pos] != " ") {
            selectQuery.tables.push_back(tokens[pos]);
        }
        pos++;
    }
    
    // Parse WHERE conditions
    if (pos < tokens.size() && toUpper(tokens[pos]) == "WHERE") {
        pos++;
        while (pos < tokens.size()) {
            Condition cond = parseCondition(tokens, pos);
            selectQuery.conditions.push_back(cond);
            
            if (cond.logicalOp.empty()) {
                break;
            }
        }
    }
    
    return selectQuery;
}

InsertQuery SQLParser::parseInsert(const std::string& query) {
    InsertQuery insertQuery;
    auto tokens = tokenize(query);
    
    size_t pos = 1; // Skip INSERT
    
    // Skip INTO
    if (pos < tokens.size() && toUpper(tokens[pos]) == "INTO") {
        pos++;
    }
    
    // Get table name
    if (pos < tokens.size()) {
        insertQuery.tableName = tokens[pos++];
    }
    
    // Skip VALUES
    if (pos < tokens.size() && toUpper(tokens[pos]) == "VALUES") {
        pos++;
    }
    
    // Skip (
    if (pos < tokens.size() && tokens[pos] == "(") {
        pos++;
    }
    
    // Parse values
    while (pos < tokens.size() && tokens[pos] != ")") {
        if (tokens[pos] != "," && tokens[pos] != " ") {
            insertQuery.values.push_back(removeQuotes(tokens[pos]));
        }
        pos++;
    }
    
    return insertQuery;
}

DeleteQuery SQLParser::parseDelete(const std::string& query) {
    DeleteQuery deleteQuery;
    auto tokens = tokenize(query);
    
    size_t pos = 1; // Skip DELETE
    
    // Skip FROM
    if (pos < tokens.size() && toUpper(tokens[pos]) == "FROM") {
        pos++;
    }
    
    // Get table name
    if (pos < tokens.size()) {
        deleteQuery.tableName = tokens[pos++];
    }
    
    // Parse WHERE conditions
    if (pos < tokens.size() && toUpper(tokens[pos]) == "WHERE") {
        pos++;
        while (pos < tokens.size()) {
            Condition cond = parseCondition(tokens, pos);
            deleteQuery.conditions.push_back(cond);
            
            if (cond.logicalOp.empty()) {
                break;
            }
        }
    }
    
    return deleteQuery;
}

