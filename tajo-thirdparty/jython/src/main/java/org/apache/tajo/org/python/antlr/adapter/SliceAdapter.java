package org.apache.tajo.org.python.antlr.adapter;

import org.apache.tajo.org.python.antlr.base.slice;
import org.apache.tajo.org.python.core.Py;
import org.apache.tajo.org.python.core.PyObject;

import java.util.ArrayList;
import java.util.List;

public class SliceAdapter implements AstAdapter {

    public Object py2ast(PyObject o) {
        if (o instanceof slice) {
            return o;
        }
        return null;
    }

    public PyObject ast2py(Object o) {
        if (o == null) {
            return Py.None;
        }
        return (PyObject)o;
    }

    public List iter2ast(PyObject iter) {
        List<slice> slices = new ArrayList<slice>();
        if (iter != Py.None) {
            for(Object o : (Iterable)iter) {
                slices.add((slice)py2ast((PyObject)o));
            }
        }
        return slices;
    }
}
