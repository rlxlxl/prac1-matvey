#include "database.h"
#include <algorithm>
#include <iostream>
#include <sstream>
#include <functional>
#include <fstream>

Database::Database(const DatabaseConfig& config) : config(config) {
    schemaName = config.name;
}

void Database::initialize() {
    FileManager::initializeDatabase(schemaName, config.structure);
}

std::vector<std::string> Database::getTableHeader(const std::string& tablePath, const std::string& /*tableName*/) {
    auto files = FileManager::getCSVFiles(tablePath);
    if (files.empty()) {
        return {};
    }
    
    std::ifstream file(files[0]);
    if (!file.is_open()) {
        return {};
    }
    
    std::string headerLine;
    std::getline(file, headerLine);
    file.close();
    
    std::vector<std::string> header;
    std::stringstream ss(headerLine);
    std::string cell;
    
    while (std::getline(ss, cell, ',')) {
        header.push_back(cell);
    }
    
    return header;
}

bool Database::evaluateCondition(const Condition& cond, 
                                 const std::map<std::string, std::vector<std::string>>& rowData,
                                 const std::map<std::string, std::vector<std::string>>& tableHeaders) {
    std::string leftValue;
    std::string rightValue;
    
    // Get left value
    if (rowData.find(cond.leftTable) != rowData.end() && 
        tableHeaders.find(cond.leftTable) != tableHeaders.end()) {
        const auto& row = rowData.at(cond.leftTable);
        const auto& header = tableHeaders.at(cond.leftTable);
        
        auto it = std::find(header.begin(), header.end(), cond.leftColumn);
        if (it != header.end()) {
            size_t index = std::distance(header.begin(), it);
            if (index < row.size()) {
                leftValue = row[index];
            }
        }
    }
    
    // Get right value
    if (cond.isLiteral) {
        rightValue = cond.rightValue;
    } else {
        if (rowData.find(cond.rightTable) != rowData.end() && 
            tableHeaders.find(cond.rightTable) != tableHeaders.end()) {
            const auto& row = rowData.at(cond.rightTable);
            const auto& header = tableHeaders.at(cond.rightTable);
            
            auto it = std::find(header.begin(), header.end(), cond.rightColumn);
            if (it != header.end()) {
                size_t index = std::distance(header.begin(), it);
                if (index < row.size()) {
                    rightValue = row[index];
                }
            }
        }
    }
    
    return leftValue == rightValue;
}

bool Database::evaluateConditions(const std::vector<Condition>& conditions,
                                   const std::map<std::string, std::vector<std::string>>& rowData,
                                   const std::map<std::string, std::vector<std::string>>& tableHeaders) {
    if (conditions.empty()) {
        return true;
    }
    
    bool result = evaluateCondition(conditions[0], rowData, tableHeaders);
    
    for (size_t i = 1; i < conditions.size(); ++i) {
        bool condResult = evaluateCondition(conditions[i], rowData, tableHeaders);
        
        if (conditions[i-1].logicalOp == "AND") {
            result = result && condResult;
        } else if (conditions[i-1].logicalOp == "OR") {
            result = result || condResult;
        }
    }
    
    return result;
}

std::vector<std::vector<std::string>> Database::executeSelect(const SelectQuery& query) {
    std::vector<std::vector<std::string>> result;
    
    if (query.tables.empty()) {
        return result;
    }
    
    // Get headers for all tables
    std::map<std::string, std::vector<std::string>> tableHeaders;
    std::map<std::string, std::string> tablePaths;
    
    for (const auto& tableName : query.tables) {
        std::string tablePath = FileManager::getTablePath(schemaName, tableName);
        tablePaths[tableName] = tablePath;
        tableHeaders[tableName] = getTableHeader(tablePath, tableName);
    }
    
    // Recursive function to generate cartesian product
    std::function<void(size_t, std::map<std::string, std::vector<std::string>>)> 
        generateProduct = [&](size_t tableIndex, 
                              std::map<std::string, std::vector<std::string>> currentRows) {
        if (tableIndex >= query.tables.size()) {
            // Check conditions
            if (evaluateConditions(query.conditions, currentRows, tableHeaders)) {
                // Build result row
                std::vector<std::string> resultRow;
                for (const auto& col : query.columns) {
                    if (currentRows.find(col.tableName) != currentRows.end() &&
                        tableHeaders.find(col.tableName) != tableHeaders.end()) {
                        const auto& row = currentRows[col.tableName];
                        const auto& header = tableHeaders[col.tableName];
                        
                        auto it = std::find(header.begin(), header.end(), col.columnName);
                        if (it != header.end()) {
                            size_t index = std::distance(header.begin(), it);
                            if (index < row.size()) {
                                resultRow.push_back(row[index]);
                            } else {
                                resultRow.push_back("");
                            }
                        } else {
                            resultRow.push_back("");
                        }
                    } else {
                        resultRow.push_back("");
                    }
                }
                result.push_back(resultRow);
            }
            return;
        }
        
        // Process current table
        std::string tableName = query.tables[tableIndex];
        std::string tablePath = tablePaths[tableName];
        auto files = FileManager::getCSVFiles(tablePath);
        
        for (const auto& file : files) {
            auto rows = FileManager::readCSVFile(file);
            for (const auto& row : rows) {
                std::map<std::string, std::vector<std::string>> newRows = currentRows;
                newRows[tableName] = row;
                generateProduct(tableIndex + 1, newRows);
            }
        }
    };
    
    generateProduct(0, {});
    
    return result;
}

void Database::executeInsert(const InsertQuery& query) {
    std::string tablePath = FileManager::getTablePath(schemaName, query.tableName);
    
    // Lock table
    if (!FileManager::lockTable(tablePath, query.tableName)) {
        throw std::runtime_error("Table " + query.tableName + " is locked");
    }
    
    try {
        // Get next PK
        int nextPK = FileManager::readPKSequence(tablePath, query.tableName) + 1;
        
        // Get table structure
        auto header = getTableHeader(tablePath, query.tableName);
        if (header.empty()) {
            FileManager::unlockTable(tablePath, query.tableName);
            throw std::runtime_error("Cannot read table structure");
        }
        
        // Count data columns (excluding PK)
        size_t dataColumnCount = header.size() - 1;
        if (query.values.size() != dataColumnCount) {
            FileManager::unlockTable(tablePath, query.tableName);
            throw std::runtime_error("Column count mismatch");
        }
        
        // Find file to insert into
        auto files = FileManager::getCSVFiles(tablePath);
        std::string targetFile;
        
        if (files.empty()) {
            targetFile = tablePath + "/1.csv";
        } else {
            // Check last file
            std::string lastFile = files.back();
            int rowCount = FileManager::getRowCount(lastFile);
            
            if (rowCount < config.tuples_limit) {
                targetFile = lastFile;
            } else {
                // Create new file
                int nextFileNum = FileManager::getNextFileNumber(tablePath);
                targetFile = tablePath + "/" + std::to_string(nextFileNum) + ".csv";
                
                // Create file with header
                std::ofstream newFile(targetFile);
                if (newFile.is_open()) {
                    for (size_t i = 0; i < header.size(); ++i) {
                        newFile << header[i];
                        if (i < header.size() - 1) newFile << ",";
                    }
                    newFile << "\n";
                    newFile.close();
                }
            }
        }
        
        // Build row: PK + values
        std::vector<std::string> row;
        row.push_back(std::to_string(nextPK));
        row.insert(row.end(), query.values.begin(), query.values.end());
        
        // Append row
        FileManager::appendToCSVFile(targetFile, row);
        
        // Update PK sequence
        FileManager::writePKSequence(tablePath, query.tableName, nextPK);
        
    } catch (...) {
        FileManager::unlockTable(tablePath, query.tableName);
        throw;
    }
    
    // Unlock table
    FileManager::unlockTable(tablePath, query.tableName);
}

void Database::executeDelete(const DeleteQuery& query) {
    std::string tablePath = FileManager::getTablePath(schemaName, query.tableName);
    
    // Lock table
    if (!FileManager::lockTable(tablePath, query.tableName)) {
        throw std::runtime_error("Table " + query.tableName + " is locked");
    }
    
    try {
        auto header = getTableHeader(tablePath, query.tableName);
        if (header.empty()) {
            FileManager::unlockTable(tablePath, query.tableName);
            return;
        }
        
        std::map<std::string, std::vector<std::string>> tableHeaders;
        tableHeaders[query.tableName] = header;
        
        auto files = FileManager::getCSVFiles(tablePath);
        
        for (const auto& file : files) {
            auto rows = FileManager::readCSVFile(file);
            std::vector<std::vector<std::string>> newRows;
            
            for (const auto& row : rows) {
                std::map<std::string, std::vector<std::string>> rowData;
                rowData[query.tableName] = row;
                
                if (!evaluateConditions(query.conditions, rowData, tableHeaders)) {
                    newRows.push_back(row);
                }
            }
            
            // Rewrite file
            FileManager::writeCSVFile(file, header, newRows);
        }
        
    } catch (...) {
        FileManager::unlockTable(tablePath, query.tableName);
        throw;
    }
    
    // Unlock table
    FileManager::unlockTable(tablePath, query.tableName);
}

