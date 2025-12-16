#ifndef CONFIG_H
#define CONFIG_H

#include <string>
#include <vector>
#include <map>

struct DatabaseConfig {
    std::string name;
    int tuples_limit;
    std::map<std::string, std::vector<std::string>> structure;
    
    static DatabaseConfig loadFromFile(const std::string& filename);
};

#endif

