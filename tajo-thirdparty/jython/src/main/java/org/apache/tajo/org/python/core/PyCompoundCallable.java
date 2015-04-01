// Copyright (c) Corporation for National Research Initiatives
package org.apache.tajo.org.python.core;

import org.python.util.Generic;

import java.util.List;

public class PyCompoundCallable extends PyObject {

    private List<PyObject> callables = Generic.list();

    private PySystemState systemState = Py.getSystemState();

    public void append(PyObject callable) {
        callables.add(callable);
    }

    public void clear() {
        callables.clear();
    }

    public PyObject __call__(PyObject[] args, String[] keywords) {
        // Set the system state to handle callbacks from java threads
        Py.setSystemState(systemState);
        for (PyObject callable : callables) {
            callable.__call__(args, keywords);
        }
        return Py.None;
    }

    public String toString() {
        return "<CompoundCallable with " + callables.size() + " callables>";
    }
}
