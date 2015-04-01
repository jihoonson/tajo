package org.apache.tajo.org.python.antlr.adapter;

import org.apache.tajo.org.python.antlr.ast.cmpopType;
import org.apache.tajo.org.python.antlr.op.*;
import org.apache.tajo.org.python.core.Py;
import org.apache.tajo.org.python.core.PyObject;

import java.util.ArrayList;
import java.util.List;

public class CmpopAdapter implements AstAdapter {

    public Object py2ast(PyObject o) {
        if (o != Py.None) {
            switch ((o).asInt()) {
                case 1:
                    return cmpopType.Eq;
                case 2:
                    return cmpopType.NotEq;
                case 3:
                    return cmpopType.Lt;
                case 4:
                    return cmpopType.LtE;
                case 5:
                    return cmpopType.Gt;
                case 6:
                    return cmpopType.GtE;
                case 7:
                    return cmpopType.Is;
                case 8:
                    return cmpopType.IsNot;
                case 9:
                    return cmpopType.In;
                case 10:
                    return cmpopType.NotIn;
                default:
                    return cmpopType.UNDEFINED;
            }
        }
        return cmpopType.UNDEFINED;
    }

    public PyObject ast2py(Object o) {
        if (o == null) {
            return Py.None;
        }
        switch ((cmpopType)o) {
            case Eq:
                return new Eq();
            case NotEq:
                return new NotEq();
            case Lt:
                return new Lt();
            case LtE:
                return new LtE();
            case Gt:
                return new Gt();
            case GtE:
                return new GtE();
            case Is:
                return new Is();
            case IsNot:
                return new IsNot();
            case In:
                return new In();
            case NotIn:
                return new NotIn();
            default:
                return Py.None;
        }
    }

    public List iter2ast(PyObject iter) {
        List<cmpopType> cmpops = new ArrayList<cmpopType>();
        if (iter != Py.None) {
            for(Object o : (Iterable)iter) {
                cmpops.add((cmpopType)py2ast((PyObject)o));
            }
        }
        return cmpops;
    }
}
