// Autogenerated AST node
package org.apache.tajo.org.python.antlr.ast;

import org.antlr.runtime.Token;
import org.apache.tajo.org.python.antlr.AST;
import org.apache.tajo.org.python.antlr.PythonTree;
import org.apache.tajo.org.python.antlr.adapter.AstAdapters;
import org.apache.tajo.org.python.antlr.base.expr;
import org.apache.tajo.org.python.core.*;
import org.apache.tajo.org.python.expose.*;

@ExposedType(name = "_ast.Lambda", base = AST.class)
public class Lambda extends expr {
public static final PyType TYPE = PyType.fromClass(Lambda.class);
    private arguments args;
    public arguments getInternalArgs() {
        return args;
    }
    @ExposedGet(name = "args")
    public PyObject getArgs() {
        return args;
    }
    @ExposedSet(name = "args")
    public void setArgs(PyObject args) {
        this.args = AstAdapters.py2arguments(args);
    }

    private expr body;
    public expr getInternalBody() {
        return body;
    }
    @ExposedGet(name = "body")
    public PyObject getBody() {
        return body;
    }
    @ExposedSet(name = "body")
    public void setBody(PyObject body) {
        this.body = AstAdapters.py2expr(body);
    }


    private final static PyString[] fields =
    new PyString[] {new PyString("args"), new PyString("body")};
    @ExposedGet(name = "_fields")
    public PyString[] get_fields() { return fields; }

    private final static PyString[] attributes =
    new PyString[] {new PyString("lineno"), new PyString("col_offset")};
    @ExposedGet(name = "_attributes")
    public PyString[] get_attributes() { return attributes; }

    public Lambda(PyType subType) {
        super(subType);
    }
    public Lambda() {
        this(TYPE);
    }
    @ExposedNew
    @ExposedMethod
    public void Lambda___init__(PyObject[] args, String[] keywords) {
        ArgParser ap = new ArgParser("Lambda", args, keywords, new String[]
            {"args", "body", "lineno", "col_offset"}, 2, true);
        setArgs(ap.getPyObject(0, Py.None));
        setBody(ap.getPyObject(1, Py.None));
        int lin = ap.getInt(2, -1);
        if (lin != -1) {
            setLineno(lin);
        }

        int col = ap.getInt(3, -1);
        if (col != -1) {
            setLineno(col);
        }

    }

    public Lambda(PyObject args, PyObject body) {
        setArgs(args);
        setBody(body);
    }

    public Lambda(Token token, arguments args, expr body) {
        super(token);
        this.args = args;
        this.body = body;
        addChild(body);
    }

    public Lambda(Integer ttype, Token token, arguments args, expr body) {
        super(ttype, token);
        this.args = args;
        this.body = body;
        addChild(body);
    }

    public Lambda(PythonTree tree, arguments args, expr body) {
        super(tree);
        this.args = args;
        this.body = body;
        addChild(body);
    }

    @ExposedGet(name = "repr")
    public String toString() {
        return "Lambda";
    }

    public String toStringTree() {
        StringBuffer sb = new StringBuffer("Lambda(");
        sb.append("args=");
        sb.append(dumpThis(args));
        sb.append(",");
        sb.append("body=");
        sb.append(dumpThis(body));
        sb.append(",");
        sb.append(")");
        return sb.toString();
    }

    public <R> R accept(VisitorIF<R> visitor) throws Exception {
        return visitor.visitLambda(this);
    }

    public void traverse(VisitorIF<?> visitor) throws Exception {
        if (args != null)
            args.accept(visitor);
        if (body != null)
            body.accept(visitor);
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
