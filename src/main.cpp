#include <iostream>
#include <string>
#include <algorithm>
#include "config.h"
#include "database.h"
#include "sql_parser.h"

void printResults(const std::vector<std::vector<std::string>>& results) {
    for (const auto& row : results) {
        for (size_t i = 0; i < row.size(); ++i) {
            std::cout << row[i];
            if (i < row.size() - 1) {
                std::cout << ",";
            }
        }
        std::cout << std::endl;
    }
}

int main() {
    try {
        // Загрузка конфигурации
        DatabaseConfig config = DatabaseConfig::loadFromFile("schema.json");
        
        // Инициализация базы данных
        Database db(config);
        db.initialize();
        
        std::cout << "Database initialized. Enter SQL queries (or 'exit' to quit):" << std::endl;
        
        std::string query;
        while (true) {
            std::cout << "> ";
            std::getline(std::cin, query);
            
            if (query.empty()) {
                continue;
            }
            
            // Преобразование в верхний регистр для парсинга
            std::string upperQuery = query;
            std::transform(upperQuery.begin(), upperQuery.end(), upperQuery.begin(), ::toupper);
            
            if (query == "exit" || query == "EXIT" || query == "quit" || query == "QUIT") {
                break;
            }
            
            QueryType type = SQLParser::parseQueryType(query);
            
            try {
                switch (type) {
                    case QueryType::SELECT: {
                        SelectQuery selectQuery = SQLParser::parseSelect(query);
                        auto results = db.executeSelect(selectQuery);
                        printResults(results);
                        break;
                    }
                    case QueryType::INSERT: {
                        InsertQuery insertQuery = SQLParser::parseInsert(query);
                        db.executeInsert(insertQuery);
                        std::cout << "Row inserted successfully." << std::endl;
                        break;
                    }
                    case QueryType::DELETE: {
                        DeleteQuery deleteQuery = SQLParser::parseDelete(query);
                        db.executeDelete(deleteQuery);
                        std::cout << "Rows deleted successfully." << std::endl;
                        break;
                    }
                    default:
                        std::cout << "Unknown query type." << std::endl;
                        break;
                }
            } catch (const std::exception& e) {
                std::cerr << "Error: " << e.what() << std::endl;
            }
        }
        
    } catch (const std::exception& e) {
        std::cerr << "Fatal error: " << e.what() << std::endl;
        return 1;
    }
    
    return 0;
}

