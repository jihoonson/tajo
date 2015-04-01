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

@ExposedType(name = "_ast.Delete", base = AST.class)
public class Delete extends stmt {
public static final PyType TYPE = PyType.fromClass(Delete.class);
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


    private final static PyString[] fields =
    new PyString[] {new PyString("targets")};
    @ExposedGet(name = "_fields")
    public PyString[] get_fields() { return fields; }

    private final static PyString[] attributes =
    new PyString[] {new PyString("lineno"), new PyString("col_offset")};
    @ExposedGet(name = "_attributes")
    public PyString[] get_attributes() { return attributes; }

    public Delete(PyType subType) {
        super(subType);
    }
    public Delete() {
        this(TYPE);
    }
    @ExposedNew
    @ExposedMethod
    public void Delete___init__(PyObject[] args, String[] keywords) {
        ArgParser ap = new ArgParser("Delete", args, keywords, new String[]
            {"targets", "lineno", "col_offset"}, 1, true);
        setTargets(ap.getPyObject(0, Py.None));
        int lin = ap.getInt(1, -1);
        if (lin != -1) {
            setLineno(lin);
        }

        int col = ap.getInt(2, -1);
        if (col != -1) {
            setLineno(col);
        }

    }

    public Delete(PyObject targets) {
        setTargets(targets);
    }

    public Delete(Token token, java.util.List<expr> targets) {
        super(token);
        this.targets = targets;
        if (targets == null) {
            this.targets = new ArrayList<expr>();
        }
        for(PythonTree t : this.targets) {
            addChild(t);
        }
    }

    public Delete(Integer ttype, Token token, java.util.List<expr> targets) {
        super(ttype, token);
        this.targets = targets;
        if (targets == null) {
            this.targets = new ArrayList<expr>();
        }
        for(PythonTree t : this.targets) {
            addChild(t);
        }
    }

    public Delete(PythonTree tree, java.util.List<expr> targets) {
        super(tree);
        this.targets = targets;
        if (targets == null) {
            this.targets = new ArrayList<expr>();
        }
        for(PythonTree t : this.targets) {
            addChild(t);
        }
    }

    @ExposedGet(name = "repr")
    public String toString() {
        return "Delete";
    }

    public String toStringTree() {
        StringBuffer sb = new StringBuffer("Delete(");
        sb.append("targets=");
        sb.append(dumpThis(targets));
        sb.append(",");
        sb.append(")");
        return sb.toString();
    }

    public <R> R accept(VisitorIF<R> visitor) throws Exception {
        return visitor.visitDelete(this);
    }

    public void traverse(VisitorIF<?> visitor) throws Exception {
        if (targets != null) {
            for (PythonTree t : targets) {
                if (t != null)
                    t.accept(visitor);
            }
        }
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
