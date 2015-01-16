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

import org.apache.tajo.algebra.BinaryOperator;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.algebra.OpType;
import org.apache.tajo.algebra.UnaryOperator;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.visitor.SimpleAlgebraVisitor;

import java.util.HashSet;
import java.util.Set;
import java.util.Stack;

public class ExprFinder extends SimpleAlgebraVisitor<ExprFinder.Context, Object> {

  static class Context {
    Set<Expr> set = new HashSet<Expr>();
    OpType targetType;
    boolean findTopOnly;

    Context(OpType type) {
      this(type, false);
    }

    Context(OpType type, boolean findTopOnly) {
      this.targetType = type;
      this.findTopOnly = findTopOnly;
    }
  }

  public static <T extends Expr> Set<T> finds(Expr expr, OpType type) {
    Context context = new Context(type);
    ExprFinder finder = new ExprFinder();
    Stack<Expr> stack = new Stack<Expr>();
    stack.push(expr);
    try {
      finder.visit(context, new Stack<Expr>(), expr);
    } catch (PlanningException e) {
      throw new RuntimeException(e);
    }
    stack.pop();
    return (Set<T>) context.set;
  }

  public static <T extends Expr> T findTopExpr(Expr expr, OpType type) {
    Context context = new Context(type, true);
    ExprFinder finder = new ExprFinder();
    Stack<Expr> stack = new Stack<Expr>();
    stack.push(expr);
    try {
      finder.visit(context, new Stack<Expr>(), expr);
    } catch (PlanningException e) {
      throw new RuntimeException(e);
    }
    stack.pop();
    if (context.set.size() > 0) {
      return (T) context.set.iterator().next();
    } else {
      return null;
    }
  }

  public Object visit(Context ctx, Stack<Expr> stack, Expr expr) throws PlanningException {
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
