#include "file_manager.h"
#include <filesystem>
#include <sstream>
#include <algorithm>
#include <iostream>
#include <fstream>
#include <map>

namespace fs = std::filesystem;

void FileManager::initializeDatabase(const std::string& schemaName, 
                                     const std::map<std::string, std::vector<std::string>>& structure) {
    // Создание директории схемы
    fs::create_directories(schemaName);
    
    for (const auto& [tableName, columns] : structure) {
        std::string tablePath = schemaName + "/" + tableName;
        fs::create_directories(tablePath);
        
        // Создание первого CSV файла с заголовком (только если не существует)
        std::vector<std::string> header = columns;
        header.insert(header.begin(), tableName + "_pk"); // Добавление колонки первичного ключа в начало
        
        std::string csvFile = tablePath + "/1.csv";
        if (!fs::exists(csvFile)) {
            std::ofstream file(csvFile);
            if (file.is_open()) {
                for (size_t i = 0; i < header.size(); ++i) {
                    file << header[i];
                    if (i < header.size() - 1) file << ",";
                }
                file << "\n";
                file.close();
            }
        }
        
        // Инициализация файла последовательности первичных ключей (только если не существует)
        std::string pkFile = tablePath + "/" + tableName + "_pk_sequence";
        if (!fs::exists(pkFile)) {
            std::ofstream pkStream(pkFile);
            if (pkStream.is_open()) {
                pkStream << "0";
                pkStream.close();
            }
        }
    }
}

std::string FileManager::getTablePath(const std::string& schemaName, const std::string& tableName) {
    return schemaName + "/" + tableName;
}

std::vector<std::string> FileManager::getCSVFiles(const std::string& tablePath) {
    std::vector<std::string> files;
    
    if (!fs::exists(tablePath)) {
        return files;
    }
    
    for (const auto& entry : fs::directory_iterator(tablePath)) {
        if (entry.is_regular_file()) {
            std::string filename = entry.path().filename().string();
            if (filename.find(".csv") != std::string::npos && 
                filename.find("_") == std::string::npos) { // Исключение файлов блокировки и последовательности
                files.push_back(entry.path().string());
            }
        }
    }
    
    // Сортировка файлов по номеру
    std::sort(files.begin(), files.end(), [](const std::string& a, const std::string& b) {
        std::string numA = fs::path(a).stem().string();
        std::string numB = fs::path(b).stem().string();
        return std::stoi(numA) < std::stoi(numB);
    });
    
    return files;
}

int FileManager::getNextFileNumber(const std::string& tablePath) {
    auto files = getCSVFiles(tablePath);
    if (files.empty()) return 1;
    
    std::string lastFile = files.back();
    std::string numStr = fs::path(lastFile).stem().string();
    return std::stoi(numStr) + 1;
}

std::vector<std::vector<std::string>> FileManager::readCSVFile(const std::string& filepath) {
    std::vector<std::vector<std::string>> result;
    std::ifstream file(filepath);
    
    if (!file.is_open()) {
        return result;
    }
    
    std::string line;
    bool isFirstLine = true;
    
    while (std::getline(file, line)) {
        if (isFirstLine) {
            isFirstLine = false;
            continue; // Пропуск заголовка
        }
        
        if (line.empty()) continue;
        
        std::vector<std::string> row;
        std::stringstream ss(line);
        std::string cell;
        
        while (std::getline(ss, cell, ',')) {
            row.push_back(cell);
        }
        
        if (!row.empty()) {
            result.push_back(row);
        }
    }
    
    file.close();
    return result;
}

void FileManager::writeCSVFile(const std::string& filepath, 
                               const std::vector<std::string>& header,
                               const std::vector<std::vector<std::string>>& rows) {
    std::ofstream file(filepath);
    
    if (!file.is_open()) {
        throw std::runtime_error("Cannot write to file: " + filepath);
    }
    
    // Запись заголовка
    for (size_t i = 0; i < header.size(); ++i) {
        file << header[i];
        if (i < header.size() - 1) file << ",";
    }
    file << "\n";
    
    // Запись строк
    for (const auto& row : rows) {
        for (size_t i = 0; i < row.size(); ++i) {
            file << row[i];
            if (i < row.size() - 1) file << ",";
        }
        file << "\n";
    }
    
    file.close();
}

void FileManager::appendToCSVFile(const std::string& filepath, 
                                  const std::vector<std::string>& row) {
    std::ofstream file(filepath, std::ios::app);
    
    if (!file.is_open()) {
        throw std::runtime_error("Cannot append to file: " + filepath);
    }
    
    for (size_t i = 0; i < row.size(); ++i) {
        file << row[i];
        if (i < row.size() - 1) file << ",";
    }
    file << "\n";
    
    file.close();
}

int FileManager::getRowCount(const std::string& filepath) {
    std::ifstream file(filepath);
    if (!file.is_open()) return 0;
    
    int count = 0;
    std::string line;
    bool isFirstLine = true;
    
    while (std::getline(file, line)) {
        if (isFirstLine) {
            isFirstLine = false;
            continue;
        }
        if (!line.empty()) {
            count++;
        }
    }
    
    file.close();
    return count;
}

bool FileManager::lockTable(const std::string& tablePath, const std::string& tableName) {
    std::string lockFile = tablePath + "/" + tableName + "_lock";
    
    if (fs::exists(lockFile)) {
        return false; // Таблица уже заблокирована
    }
    
    std::ofstream file(lockFile);
    if (file.is_open()) {
        file << "locked";
        file.close();
        return true;
    }
    
    return false;
}

void FileManager::unlockTable(const std::string& tablePath, const std::string& tableName) {
    std::string lockFile = tablePath + "/" + tableName + "_lock";
    if (fs::exists(lockFile)) {
        fs::remove(lockFile);
    }
}

bool FileManager::isTableLocked(const std::string& tablePath, const std::string& tableName) {
    std::string lockFile = tablePath + "/" + tableName + "_lock";
    return fs::exists(lockFile);
}

int FileManager::readPKSequence(const std::string& tablePath, const std::string& tableName) {
    std::string pkFile = tablePath + "/" + tableName + "_pk_sequence";
    
    if (!fs::exists(pkFile)) {
        return 0;
    }
    
    std::ifstream file(pkFile);
    if (!file.is_open()) {
        return 0;
    }
    
    int pk;
    file >> pk;
    file.close();
    
    return pk;
}

void FileManager::writePKSequence(const std::string& tablePath, const std::string& tableName, int pk) {
    std::string pkFile = tablePath + "/" + tableName + "_pk_sequence";
    
    std::ofstream file(pkFile);
    if (file.is_open()) {
        file << pk;
        file.close();
    }
}

