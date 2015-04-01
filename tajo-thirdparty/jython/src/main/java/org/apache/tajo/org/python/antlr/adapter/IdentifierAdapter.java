package org.apache.tajo.org.python.antlr.adapter;

import org.apache.tajo.org.python.core.Py;
import org.apache.tajo.org.python.core.PyObject;
import org.apache.tajo.org.python.core.PyString;

import java.util.ArrayList;
import java.util.List;

public class IdentifierAdapter implements AstAdapter {

    public Object py2ast(PyObject o) {
        if (o == null || o == Py.None) {
            return null;
        }
        return o.toString();
    }

    public PyObject ast2py(Object o) {
        if (o == null) {
            return Py.None;
        }
        return new PyString(o.toString());
    }

    public List iter2ast(PyObject iter) {
        List<String> identifiers = new ArrayList<String>();
        if (iter != Py.None) {
            for(Object o : (Iterable)iter) {
                identifiers.add((String)py2ast((PyObject)o));
            }
        }
        return identifiers;
    }
}
