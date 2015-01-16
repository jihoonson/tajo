/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.tajo.plan.rewrite;

import com.google.common.base.Preconditions;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.algebra.*;
import org.apache.tajo.plan.PlanningException;
import org.apache.tajo.plan.algebra.BaseAlgebraVisitor;
import org.apache.tajo.plan.util.ExprFinder;
import org.apache.tajo.util.TUtil;

import java.util.Map;
import java.util.Stack;

public class InSubQueryRewriteRule implements ExpressionRewriteRule {
  private final static String NAME = "InSubQueryRewriter";
  private Rewriter rewriter = new Rewriter();

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public boolean isEligible(OverridableConf queryContext, Expr expr) {
    for (Expr found : ExprFinder.finds(expr, OpType.InPredicate)) {
      InPredicate inPredicate = (InPredicate) found;
      if (inPredicate.getInValue().getType() == OpType.SimpleTableSubQuery) {
        return true;
      }
    }

    return false;
  }

  @Override
  public Expr rewrite(OverridableConf queryContext, Expr expr) throws PlanningException {
    Context context = new Context(queryContext);
    return rewriter.visit(context, new Stack<Expr>(), expr);
  }

  static class Context {
    OverridableConf queryContext;
    boolean needDistinct;
    Map<SimpleTableSubQuery, Selection> replacedMap;

    public Context(OverridableConf queryContext) {
      this.queryContext = queryContext;
      this.needDistinct = false;
      this.replacedMap = TUtil.newHashMap();
    }
  }

  class Rewriter extends BaseAlgebraVisitor<Context, Expr> {

    @Override
    public Expr visitFilter(Context ctx, Stack<Expr> stack, Selection expr) throws PlanningException {
      for (Expr found : ExprFinder.finds(expr.getQual(), OpType.InPredicate)) {
        InPredicate inPredicate = (InPredicate) found;
        if (inPredicate.getInValue().getType() == OpType.SimpleTableSubQuery) {
          Preconditions.checkArgument(inPredicate.getPredicand().getType() == OpType.Column);
          ColumnReferenceExpr predicand = (ColumnReferenceExpr) inPredicate.getPredicand();
          SimpleTableSubQuery subQuery = (SimpleTableSubQuery) inPredicate.getInValue();

          Join join = new Join(inPredicate.isNot() ? JoinType.LEFT_OUTER : JoinType.INNER);
          join.setLeft(expr.getChild());
          join.setRight(subQuery);
          ctx.needDistinct = true;

          // join condition
        }
      }
      super.visitFilter(ctx, stack, expr);
      return null;
    }

    @Override
    public Expr visitProjection(Context ctx, Stack<Expr> stack, Projection expr) throws PlanningException {
      if (ctx.needDistinct) {
        expr.setDistinct();
        ctx.needDistinct = false;
      }
      return super.visitProjection(ctx, stack, expr);
    }
  }
}
