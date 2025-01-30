#include <iostream>
#include <fstream>
#include <vector>
#include <string>
#include <algorithm>
#include <unordered_map>
#include <memory>
#include <iomanip>

using namespace std;

class RelationalDatabase {
private:
    vector<vector<string>> data;
    vector<string> columns;

    vector<string> parseCSVLine(const string& line) {
        vector<string> tokens;
        size_t start = 0;
        bool inQuotes = false;
        
        for (size_t i = 0; i < line.length(); ++i) {
            if (line[i] == '"') inQuotes = !inQuotes;
            else if (line[i] == ',' && !inQuotes) {
                tokens.push_back(cleanToken(line.substr(start, i - start)));
                start = i + 1;
            }
        }
        tokens.push_back(cleanToken(line.substr(start)));
        return tokens;
    }

    string cleanToken(string token) {
        // Remove surrounding quotes and trim whitespace
        if (!token.empty() && token.front() == '"') 
            token.erase(0, 1);
        if (!token.empty() && token.back() == '"') 
            token.pop_back();
        
        // Trim whitespace
        token.erase(token.begin(), find_if(token.begin(), token.end(), [](int ch) {
            return !isspace(ch);
        }));
        token.erase(find_if(token.rbegin(), token.rend(), [](int ch) {
            return !isspace(ch);
        }).base(), token.end());
        
        return token;
    }

public:
    void loadCSV(const string& filename) {
        ifstream file(filename);
        if (!file) throw runtime_error("Failed to open: " + filename);

        string header;
        if (getline(file, header)) {
            columns = parseCSVLine(header);
        }

        string line;
        while (getline(file, line)) {
            auto parsed = parseCSVLine(line);
            if (parsed.size() != columns.size()) {
                throw runtime_error("CSV format error in " + filename);
            }
            data.push_back(parsed);
        }
    }

    RelationalDatabase select(const string& column, const string& value) const {
        RelationalDatabase result;
        result.columns = columns;
        
        int colIndex = distance(columns.begin(), 
            find(columns.begin(), columns.end(), column));
        
        for (const auto& row : data) {
            if (row[colIndex] == value) {
                result.data.push_back(row);
            }
        }
        return result;
    }

    RelationalDatabase project(const vector<string>& cols) const {
        RelationalDatabase result;
        vector<int> indices;
        
        for (const auto& col : cols) {
            auto it = find(columns.begin(), columns.end(), col);
            if (it == columns.end()) throw invalid_argument("Invalid column: " + col);
            indices.push_back(distance(columns.begin(), it));
            result.columns.push_back(col);
        }
        
        for (const auto& row : data) {
            vector<string> projectedRow;
            for (int idx : indices) {
                projectedRow.push_back(row[idx]);
            }
            result.data.push_back(projectedRow);
        }
        return result;
    }

    RelationalDatabase join(const RelationalDatabase& other, const string& joinCol) const {
        RelationalDatabase result;
        const int thisCol = getColumnIndex(joinCol);
        const int otherCol = other.getColumnIndex(joinCol);

        // Build hash map
        unordered_map<string, vector<vector<string>>> otherMap;
        for (const auto& row : other.data) {
            otherMap[row[otherCol]].push_back(row);
        }

        // Merge schemas
        result.columns = columns;
        for (const auto& col : other.columns) {
            if (find(columns.begin(), columns.end(), col) == columns.end()) {
                result.columns.push_back(col);
            }
        }

        // Perform join
        for (const auto& row : data) {
            const string& key = row[thisCol];
            if (otherMap.find(key) != otherMap.end()) {
                for (const auto& otherRow : otherMap[key]) {
                    vector<string> merged = row;
                    for (size_t i = 0; i < otherRow.size(); ++i) {
                        if (i != static_cast<size_t>(otherCol)) {
                            merged.push_back(otherRow[i]);
                        }
                    }
                    result.data.push_back(merged);
                }
            }
        }
        return result;
    }

    void print() const {
        // Header
        for (const auto& col : columns) {
            cout << left << setw(20) << col;
        }
        cout << "\n" << string(20 * columns.size(), '-') << "\n";

        // Data
        for (const auto& row : data) {
            for (const auto& cell : row) {
                cout << left << setw(20) << cell;
            }
            cout << "\n";
        }
        cout << endl;
    }

private:
    int getColumnIndex(const string& col) const {
        auto it = find(columns.begin(), columns.end(), col);
        if (it == columns.end()) throw invalid_argument("Column not found: " + col);
        return distance(columns.begin(), it);
    }
};

int main() {
    try {
        RelationalDatabase buyers, suppliers;

        buyers.loadCSV("data/buyers.csv");
        suppliers.loadCSV("data/suppliers.csv");

        cout << "=== BUYERS TABLE ===\n";
        buyers.print();

        cout << "=== SUPPLIERS IN DEPT 23 ===\n";
        auto dept23 = suppliers.select("Dept", "23");
        dept23.print();

        cout << "=== JOINED DATA ===\n";
        auto combined = buyers.join(suppliers, "PartNo");
        combined.print();

    } catch (const exception& e) {
        cerr << "Error: " << e.what() << endl;
        return 1;
    }
    return 0;
}