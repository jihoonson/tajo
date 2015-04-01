// Copyright (c) Corporation for National Research Initiatives
package org.apache.tajo.org.python.core;

import org.python.expose.ExposedType;

import java.io.Serializable;

/**
 * A class representing the singleton Ellipsis <code>...</code> object.
 */
@ExposedType(name = "ellipsis", base = PyObject.class, isBaseType = false)
public class PyEllipsis extends PySingleton implements Serializable {

    public static final PyType TYPE = PyType.fromClass(PyEllipsis.class);

    PyEllipsis() {
        super("Ellipsis");
    }

    private Object writeReplace() {
        return new Py.SingletonResolver("Ellipsis");
    }
}
