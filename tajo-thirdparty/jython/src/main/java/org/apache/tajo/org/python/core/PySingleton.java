// Copyright (c) Corporation for National Research Initiatives
package org.apache.tajo.org.python.core;

public class PySingleton extends PyObject
{
    private String name;

    public PySingleton(String name) {
        this.name = name;
    }

    public String toString() {
        return name;
    }
}
