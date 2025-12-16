#include "config.h"
#include <fstream>
#include <sstream>
#include <iostream>
#include <algorithm>

DatabaseConfig DatabaseConfig::loadFromFile(const std::string& filename) {
    DatabaseConfig config;
    std::ifstream file(filename);
    
    if (!file.is_open()) {
        throw std::runtime_error("Cannot open schema.json");
    }
    
    std::string line;
    std::string content;
    while (std::getline(file, line)) {
        content += line;
    }
    file.close();
    
    // Simple JSON parser for our specific format
    // Remove whitespace
    content.erase(std::remove_if(content.begin(), content.end(), 
        [](char c) { return std::isspace(c); }), content.end());
    
    // Extract name
    size_t namePos = content.find("\"name\":\"");
    if (namePos != std::string::npos) {
        namePos += 8;
        size_t nameEnd = content.find("\"", namePos);
        config.name = content.substr(namePos, nameEnd - namePos);
    }
    
    // Extract tuples_limit
    size_t limitPos = content.find("\"tuples_limit\":");
    if (limitPos != std::string::npos) {
        limitPos += 15;
        size_t limitEnd = content.find_first_of(",}", limitPos);
        config.tuples_limit = std::stoi(content.substr(limitPos, limitEnd - limitPos));
    }
    
    // Extract structure
    size_t structPos = content.find("\"structure\":{");
    if (structPos != std::string::npos) {
        structPos += 13;
        size_t structEnd = content.find_last_of("}");
        
        std::string structContent = content.substr(structPos, structEnd - structPos);
        
        size_t pos = 0;
        while (pos < structContent.length()) {
            // Find table name
            size_t tableStart = structContent.find("\"", pos);
            if (tableStart == std::string::npos) break;
            tableStart += 1;
            size_t tableEnd = structContent.find("\"", tableStart);
            std::string tableName = structContent.substr(tableStart, tableEnd - tableStart);
            
            // Find columns array
            size_t arrayStart = structContent.find("[", tableEnd);
            if (arrayStart == std::string::npos) break;
            size_t arrayEnd = structContent.find("]", arrayStart);
            
            std::string arrayContent = structContent.substr(arrayStart + 1, arrayEnd - arrayStart - 1);
            
            std::vector<std::string> columns;
            size_t colPos = 0;
            while (colPos < arrayContent.length()) {
                size_t colStart = arrayContent.find("\"", colPos);
                if (colStart == std::string::npos) break;
                colStart += 1;
                size_t colEnd = arrayContent.find("\"", colStart);
                std::string colName = arrayContent.substr(colStart, colEnd - colStart);
                columns.push_back(colName);
                colPos = colEnd + 1;
            }
            
            config.structure[tableName] = columns;
            pos = arrayEnd + 1;
        }
    }
    
    return config;
}

