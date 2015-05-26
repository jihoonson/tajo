/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.plan.util;

import org.apache.tajo.algebra.*;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.algebra.BaseAlgebraVisitor;
import org.apache.tajo.plan.visitor.SimpleAlgebraVisitor;

import java.util.HashSet;
import java.util.Set;
import java.util.Stack;

public class ExprTreeUtil {

  static class TypeFinderContext {
    Set<Expr> set = new HashSet<Expr>();
    OpType targetType;
    boolean findTopOnly;

    TypeFinderContext(OpType type) {
      this(type, false);
    }

    TypeFinderContext(OpType type, boolean findTopOnly) {
      this.targetType = type;
      this.findTopOnly = findTopOnly;
    }
  }

  static class ParentFinderContext {
    Expr target;
    Expr found;

    ParentFinderContext(Expr target) {
      this.target = target;
    }
  }

  public static <T extends Expr> Set<T> finds(Expr expr, OpType type) {
    TypeFinderContext context = new TypeFinderContext(type);
    TypeFinder finder = new TypeFinder();
    try {
      finder.visit(context, new Stack<Expr>(), expr);
    } catch (PlanningException e) {
      throw new RuntimeException(e);
    }
    return (Set<T>) context.set;
  }

  public static <T extends Expr> T findTopExpr(Expr expr, OpType type) {
    TypeFinderContext context = new TypeFinderContext(type, true);
    TypeFinder finder = new TypeFinder();
    try {
      finder.visit(context, new Stack<Expr>(), expr);
    } catch (PlanningException e) {
      throw new RuntimeException(e);
    }
    if (context.set.size() > 0) {
      return (T) context.set.iterator().next();
    } else {
      return null;
    }
  }

  public static <T extends Expr> T findParent(Expr root, Expr target) {
    ParentFinderContext context = new ParentFinderContext(target);
    ExprFinder finder = new ExprFinder();
    try {
      finder.visit(context, new Stack<Expr>(), root);
    } catch (PlanningException e) {
      throw new RuntimeException(e);
    }
    return (T) context.found;
  }

  static class TypeFinder extends SimpleAlgebraVisitor<ExprTreeUtil.TypeFinderContext, Object> {

    @Override
    public Object visit(TypeFinderContext ctx, Stack<Expr> stack, Expr expr) throws PlanningException {
      if (ctx.targetType == expr.getType()) {
        ctx.set.add(expr);
        if (ctx.findTopOnly) {
          return null;
        }
      }

      if (expr instanceof UnaryOperator) {
        preHook(ctx, stack, expr);
        visitUnaryOperator(ctx, stack, (UnaryOperator) expr);
        postHook(ctx, stack, expr, null);
      } else if (expr instanceof BinaryOperator) {
        preHook(ctx, stack, expr);
        visitBinaryOperator(ctx, stack, (BinaryOperator) expr);
        postHook(ctx, stack, expr, null);
      } else {
        super.visit(ctx, stack, expr);
      }

//    if (ctx.targetType == expr.getType()) {
//      ctx.set.add(expr);
//    }

      return null;
    }
  }

  static class ExprFinder extends BaseAlgebraVisitor<ParentFinderContext, Object> {

    @Override
    public void preHook(ParentFinderContext ctx, Stack<Expr> stack, Expr expr) throws PlanningException {
      if (expr instanceof UnaryOperator) {
        UnaryOperator unary = (UnaryOperator) expr;
        if (unary.getChild().identical(ctx.target)) {
          ctx.found = unary;
        }
      } else if (expr instanceof BinaryOperator) {
        BinaryOperator binary = (BinaryOperator) expr;
        if (binary.getLeft().identical(ctx.target) ||
            binary.getRight().identical(ctx.target)) {
          ctx.found = binary;
        }
      } else if (expr instanceof TablePrimarySubQuery) {
        TablePrimarySubQuery subQuery = (TablePrimarySubQuery) expr;
        if (subQuery.getSubQuery().identical(ctx.target)) {
          ctx.found = subQuery;
        }
    }
//    @Override
//    public Object visit(ParentFinderContext ctx, Stack<Expr> stack, Expr expr) throws PlanningException {
//
//      if (expr instanceof UnaryOperator) {
//        UnaryOperator unary = (UnaryOperator) expr;
//        if (unary.getChild().equals(ctx.target)) {
//          ctx.found = unary;
//          return null;
//        }
//        preHook(ctx, stack, expr);
//        visitUnaryOperator(ctx, stack, unary);
//        postHook(ctx, stack, expr, null);
//      } else if (expr instanceof BinaryOperator) {
//        BinaryOperator binary = (BinaryOperator) expr;
//        if (binary.getLeft().equals(ctx.target) ||
//            binary.getRight().equals(ctx.target)) {
//          ctx.found = binary;
//          return null;
//        }
//        preHook(ctx, stack, expr);
//        visitBinaryOperator(ctx, stack, binary);
//        postHook(ctx, stack, expr, null);
//      } else if (expr instanceof TablePrimarySubQuery) {
//        TablePrimarySubQuery subQuery = (TablePrimarySubQuery) expr;
//        if (subQuery.getSubQuery().equals(ctx.target)) {
//          ctx.found = subQuery;
//          return null;
//        }
//        preHook(ctx, stack, expr);
//        visitTableSubQuery(ctx, stack, subQuery);
//        postHook(ctx, stack, expr, null);
//      } else {
//        super.visit(ctx, stack, expr);
//      }
//
//      return null;
    }
  }
}
