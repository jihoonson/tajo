// Copyright (c) Corporation for National Research Initiatives

package org.apache.tajo.org.python.compiler;

import org.apache.tajo.org.python.antlr.ParseException;
import org.apache.tajo.org.python.antlr.Visitor;
import org.apache.tajo.org.python.antlr.ast.*;
import org.apache.tajo.org.python.antlr.base.expr;
import org.apache.tajo.org.python.antlr.base.stmt;

import java.util.ArrayList;
import java.util.List;

public class ArgListCompiler extends Visitor
{
    public boolean arglist, keywordlist;
    public List<expr> defaults;
    public List<String> names;
    public List<String> fpnames;
    public List<stmt> init_code;

    public ArgListCompiler() {
        arglist = keywordlist = false;
        defaults = null;
        names = new ArrayList<String>();
        fpnames = new ArrayList<String>();
        init_code = new ArrayList<stmt>();
    }

    public void reset() {
        arglist = keywordlist = false;
        defaults = null;
        names.clear();
        init_code.clear();
    }

    public void appendInitCode(Suite node) {
        node.getInternalBody().addAll(0, init_code);
    }

    public List<expr> getDefaults() {
        return defaults;
    }

    public void visitArgs(arguments args) throws Exception {
        for (int i = 0; i < args.getInternalArgs().size(); i++) {
            String name = (String) visit(args.getInternalArgs().get(i));
            names.add(name);
            if (args.getInternalArgs().get(i) instanceof Tuple) {
                List<expr> targets = new ArrayList<expr>();
                targets.add(args.getInternalArgs().get(i));
                Assign ass = new Assign(args.getInternalArgs().get(i),
                    targets,
                    new Name(args.getInternalArgs().get(i), name, expr_contextType.Load));
                init_code.add(ass);
            }
        }
        if (args.getInternalVararg() != null) {
            arglist = true;
            names.add(args.getInternalVararg());
        }
        if (args.getInternalKwarg() != null) {
            keywordlist = true;
            names.add(args.getInternalKwarg());
        }
        
        defaults = args.getInternalDefaults();
        for (int i = 0; i < defaults.size(); i++) {
            if (defaults.get(i) == null)
                throw new ParseException(
                    "non-default argument follows default argument",
                    args.getInternalArgs().get(args.getInternalArgs().size() - defaults.size() + i));
        }
    }

    @Override
    public Object visitName(Name node) throws Exception {
        //FIXME: do we need Store and Param, or just Param?
        if (node.getInternalCtx() != expr_contextType.Store && node.getInternalCtx() != expr_contextType.Param) {
            return null;
        } 

        if (fpnames.contains(node.getInternalId())) {
            throw new ParseException("duplicate argument name found: " +
                                     node.getInternalId(), node);
        }
        fpnames.add(node.getInternalId());
        return node.getInternalId();
    }

    @Override
    public Object visitTuple(Tuple node) throws Exception {
        StringBuffer name = new StringBuffer("(");
        int n = node.getInternalElts().size();
        for (int i = 0; i < n-1; i++) {
            name.append(visit(node.getInternalElts().get(i)));
            name.append(", ");
        }
        name.append(visit(node.getInternalElts().get(n - 1)));
        name.append(")");
        return name.toString();
    }
}
