// module_page.cpp
#include <pybind11/pybind11.h>
#include "page.h"

namespace py = pybind11;

PYBIND11_MODULE(page, m) {  // Note: the module name is "page"
    m.doc() = "C++ implementation of Page for L-Store";
    py::class_<Page>(m, "Page")
        .def(py::init<>())
        .def("__repr__", &Page::repr)
        .def("has_capacity", &Page::has_capacity)
        .def("write", &Page::write, "Append a record and return its index (or -1 if full)")
        .def("overwrite_rid", &Page::overwrite_rid, "Overwrite the 'rid' attribute at a given index")
        .def("read_all", &Page::read_all, "Return all records")
        .def("read_index", &Page::read_index, "Return the record at the specified index");
}
