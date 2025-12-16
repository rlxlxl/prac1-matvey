CXX = g++
CXXFLAGS = -std=c++17 -Wall -Wextra
TARGET = dbms
SRCDIR = src
INCDIR = include
SOURCES = $(wildcard $(SRCDIR)/*.cpp)
OBJECTS = $(SOURCES:$(SRCDIR)/%.cpp=%.o)

# Добавляем путь к заголовочным файлам
INCLUDES = -I$(INCDIR)

all: $(TARGET)

$(TARGET): $(OBJECTS)
	$(CXX) $(CXXFLAGS) $(INCLUDES) -o $(TARGET) $(OBJECTS)

%.o: $(SRCDIR)/%.cpp
	$(CXX) $(CXXFLAGS) $(INCLUDES) -c $< -o $@

clean:
	rm -f $(OBJECTS) $(TARGET)

.PHONY: all clean
