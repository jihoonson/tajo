// Autogenerated AST node
package org.apache.tajo.org.python.antlr.ast;

import org.antlr.runtime.Token;
import org.apache.tajo.org.python.antlr.AST;
import org.apache.tajo.org.python.antlr.PythonTree;
import org.apache.tajo.org.python.antlr.adapter.AstAdapters;
import org.apache.tajo.org.python.antlr.base.expr;
import org.apache.tajo.org.python.antlr.base.stmt;
import org.apache.tajo.org.python.core.*;
import org.apache.tajo.org.python.expose.*;

import java.util.ArrayList;

@ExposedType(name = "_ast.Assign", base = AST.class)
public class Assign extends stmt {
public static final PyType TYPE = PyType.fromClass(Assign.class);
    private java.util.List<expr> targets;
    public java.util.List<expr> getInternalTargets() {
        return targets;
    }
    @ExposedGet(name = "targets")
    public PyObject getTargets() {
        return new AstList(targets, AstAdapters.exprAdapter);
    }
    @ExposedSet(name = "targets")
    public void setTargets(PyObject targets) {
        this.targets = AstAdapters.py2exprList(targets);
    }

    private expr value;
    public expr getInternalValue() {
        return value;
    }
    @ExposedGet(name = "value")
    public PyObject getValue() {
        return value;
    }
    @ExposedSet(name = "value")
    public void setValue(PyObject value) {
        this.value = AstAdapters.py2expr(value);
    }


    private final static PyString[] fields =
    new PyString[] {new PyString("targets"), new PyString("value")};
    @ExposedGet(name = "_fields")
    public PyString[] get_fields() { return fields; }

    private final static PyString[] attributes =
    new PyString[] {new PyString("lineno"), new PyString("col_offset")};
    @ExposedGet(name = "_attributes")
    public PyString[] get_attributes() { return attributes; }

    public Assign(PyType subType) {
        super(subType);
    }
    public Assign() {
        this(TYPE);
    }
    @ExposedNew
    @ExposedMethod
    public void Assign___init__(PyObject[] args, String[] keywords) {
        ArgParser ap = new ArgParser("Assign", args, keywords, new String[]
            {"targets", "value", "lineno", "col_offset"}, 2, true);
        setTargets(ap.getPyObject(0, Py.None));
        setValue(ap.getPyObject(1, Py.None));
        int lin = ap.getInt(2, -1);
        if (lin != -1) {
            setLineno(lin);
        }

        int col = ap.getInt(3, -1);
        if (col != -1) {
            setLineno(col);
        }

    }

    public Assign(PyObject targets, PyObject value) {
        setTargets(targets);
        setValue(value);
    }

    public Assign(Token token, java.util.List<expr> targets, expr value) {
        super(token);
        this.targets = targets;
        if (targets == null) {
            this.targets = new ArrayList<expr>();
        }
        for(PythonTree t : this.targets) {
            addChild(t);
        }
        this.value = value;
        addChild(value);
    }

    public Assign(Integer ttype, Token token, java.util.List<expr> targets, expr value) {
        super(ttype, token);
        this.targets = targets;
        if (targets == null) {
            this.targets = new ArrayList<expr>();
        }
        for(PythonTree t : this.targets) {
            addChild(t);
        }
        this.value = value;
        addChild(value);
    }

    public Assign(PythonTree tree, java.util.List<expr> targets, expr value) {
        super(tree);
        this.targets = targets;
        if (targets == null) {
            this.targets = new ArrayList<expr>();
        }
        for(PythonTree t : this.targets) {
            addChild(t);
        }
        this.value = value;
        addChild(value);
    }

    @ExposedGet(name = "repr")
    public String toString() {
        return "Assign";
    }

    public String toStringTree() {
        StringBuffer sb = new StringBuffer("Assign(");
        sb.append("targets=");
        sb.append(dumpThis(targets));
        sb.append(",");
        sb.append("value=");
        sb.append(dumpThis(value));
        sb.append(",");
        sb.append(")");
        return sb.toString();
    }

    public <R> R accept(VisitorIF<R> visitor) throws Exception {
        return visitor.visitAssign(this);
    }

    public void traverse(VisitorIF<?> visitor) throws Exception {
        if (targets != null) {
            for (PythonTree t : targets) {
                if (t != null)
                    t.accept(visitor);
            }
        }
        if (value != null)
            value.accept(visitor);
    }

    private int lineno = -1;
    @ExposedGet(name = "lineno")
    public int getLineno() {
        if (lineno != -1) {
            return lineno;
        }
        return getLine();
    }

    @ExposedSet(name = "lineno")
    public void setLineno(int num) {
        lineno = num;
    }

    private int col_offset = -1;
    @ExposedGet(name = "col_offset")
    public int getCol_offset() {
        if (col_offset != -1) {
            return col_offset;
        }
        return getCharPositionInLine();
    }

    @ExposedSet(name = "col_offset")
    public void setCol_offset(int num) {
        col_offset = num;
    }

}
