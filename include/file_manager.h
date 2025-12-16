#ifndef FILE_MANAGER_H
#define FILE_MANAGER_H

#include <string>
#include <vector>
#include <fstream>
#include <map>

class FileManager {
public:
    static void initializeDatabase(const std::string& schemaName, 
                                   const std::map<std::string, std::vector<std::string>>& structure);
    
    static std::string getTablePath(const std::string& schemaName, const std::string& tableName);
    static std::vector<std::string> getCSVFiles(const std::string& tablePath);
    static int getNextFileNumber(const std::string& tablePath);
    
    static std::vector<std::vector<std::string>> readCSVFile(const std::string& filepath);
    static void writeCSVFile(const std::string& filepath, 
                            const std::vector<std::string>& header,
                            const std::vector<std::vector<std::string>>& rows);
    static void appendToCSVFile(const std::string& filepath, 
                               const std::vector<std::string>& row);
    
    static int getRowCount(const std::string& filepath);
    
    static bool lockTable(const std::string& tablePath, const std::string& tableName);
    static void unlockTable(const std::string& tablePath, const std::string& tableName);
    static bool isTableLocked(const std::string& tablePath, const std::string& tableName);
    
    static int readPKSequence(const std::string& tablePath, const std::string& tableName);
    static void writePKSequence(const std::string& tablePath, const std::string& tableName, int pk);
};

#endif

