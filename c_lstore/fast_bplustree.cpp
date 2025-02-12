// lstore/fast_bplustree.cpp
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>
#include <vector>
#include <string>
#include <algorithm>
#include <map>
#include <stdexcept>

namespace py = pybind11;

// A minimal in-memory B+ tree implemented using a sorted vector.
class BPlusTree {
public:
    int order;       // Unused in this minimal version, but kept for interface compatibility.
    int cache_size;  // Unused here.
    std::vector<std::pair<int, std::string>> data;  // Sorted container of key-value pairs.

    // Constructor: index_file is ignored in this in-memory version.
    BPlusTree(const std::string &index_file, int order = 75, int cache_size = 10000)
        : order(order), cache_size(cache_size) { }

    // Insert a single key-value pair.
    void insert(int key, const std::string &value) {
        auto it = std::lower_bound(data.begin(), data.end(), key,
            [](const std::pair<int, std::string>& p, const int &k) {
                return p.first < k;
            });
        if (it != data.end() && it->first == key) {
            it->second = value;
        } else {
            data.insert(it, {key, value});
        }
    }

    // Batch insert sorted key-value pairs.
    // If the tree is nonempty, the first new key must be strictly greater than the current maximum.
    void batch_insert(const std::vector<std::pair<int, std::string>> &pairs) {
        if (!data.empty() && !pairs.empty()) {
            int current_max = data.back().first;
            if (pairs[0].first <= current_max) {
                throw std::runtime_error("Keys to batch insert must be sorted and bigger than keys currently in the tree");
            }
        }
        data.insert(data.end(), pairs.begin(), pairs.end());
    }

    // Get the value for a given key (throws if not found).
    std::string get(int key) const {
        auto it = std::lower_bound(data.begin(), data.end(), key,
            [](const std::pair<int, std::string>& p, const int &k) {
                return p.first < k;
            });
        if (it != data.end() && it->first == key) {
            return it->second;
        }
        throw std::runtime_error("Key not found");
    }

    // Return a map of key-value pairs for keys in [start_key, stop_key).
    std::map<int, std::string> range_query(int start_key, int stop_key) const {
        std::map<int, std::string> result;
        auto it = std::lower_bound(data.begin(), data.end(), start_key,
            [](const std::pair<int, std::string>& p, const int &k) {
                return p.first < k;
            });
        while (it != data.end() && it->first < stop_key) {
            result[it->first] = it->second;
            ++it;
        }
        return result;
    }

    // Return the number of keys stored.
    size_t size() const {
        return data.size();
    }
};

PYBIND11_MODULE(fast_bplustree, m) {
    m.doc() = "A minimal in-memory B+ tree implemented in C++";

    py::class_<BPlusTree>(m, "BPlusTree")
        .def(py::init<const std::string &, int, int>(),
             py::arg("index_file"),
             py::arg("order") = 75,
             py::arg("cache_size") = 10000)
        .def("insert", &BPlusTree::insert, "Insert a key-value pair")
        .def("batch_insert", &BPlusTree::batch_insert, "Batch insert sorted key-value pairs")
        .def("get", &BPlusTree::get, "Get the value for a key")
        .def("size", &BPlusTree::size, "Return the number of keys stored")
        // Allow syntax: tree[key] = value
        .def("__setitem__", [](BPlusTree &self, int key, const std::string &value) {
            self.insert(key, value);
        })
        // Allow syntax: value = tree[key]  OR  dict = tree[slice]
        .def("__getitem__", [&] (BPlusTree &self, py::object key) -> py::object {
            if (py::isinstance<py::int_>(key)) {
                int k = key.cast<int>();
                return py::str(self.get(k));
            } else if (py::isinstance<py::slice>(key)) {
                py::slice s(key);
                py::object start_obj = s.attr("start");
                py::object stop_obj = s.attr("stop");
                if (start_obj.is_none() || stop_obj.is_none()) {
                    throw std::runtime_error("Slice must have start and stop");
                }
                int start_key = start_obj.cast<int>();
                int stop_key = stop_obj.cast<int>();
                auto result_map = self.range_query(start_key, stop_key);
                py::dict result;
                for (const auto &p : result_map) {
                    result[py::int_(p.first)] = py::str(p.second);
                }
                return result;
            } else {
                throw std::runtime_error("Invalid key type, must be int or slice");
            }
        }, py::arg("key"));
}
