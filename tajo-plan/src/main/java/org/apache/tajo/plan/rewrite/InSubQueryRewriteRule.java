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
import org.apache.tajo.plan.util.ExprTreeUtil;
import org.apache.tajo.util.TUtil;

import java.util.Map;
import java.util.Set;
import java.util.Stack;

public class InSubQueryRewriteRule implements ExpressionRewriteRule {
  private final static String NAME = "InSubQueryRewriter";
  private Preprocessor preprocessor = new Preprocessor();

  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public boolean isEligible(OverridableConf queryContext, Expr expr) {
    for (Expr foundFilter : ExprTreeUtil.finds(expr, OpType.Filter)) {
      Selection selection = (Selection) foundFilter;
      for (Expr foundIn : ExprTreeUtil.finds(selection.getQual(), OpType.InPredicate)) {
        InPredicate inPredicate = (InPredicate) foundIn;
        if (inPredicate.getInValue().getType() == OpType.SimpleTableSubQuery) {
          return true;
        }
      }
    }

    return false;
  }

  @Override
  public Expr rewrite(OverridableConf queryContext, Expr expr) throws PlanningException {
    Context context = new Context(queryContext);
    Expr rewritten;
    try {
      rewritten = (Expr) expr.clone();
    } catch (CloneNotSupportedException e) {
      throw new PlanningException(e);
    }
    preprocessor.visit(context, new Stack<Expr>(), rewritten);

    for (Map.Entry<Selection, Expr> entry : context.replacedMap.entrySet()) {
      Expr parent = ExprTreeUtil.findParent(rewritten, entry.getKey());

      if (parent == null) {
        throw new PlanningException("No such parent who has " + entry.getKey() + " as its child");
      }

      if (parent instanceof UnaryOperator) {
        ((UnaryOperator) parent).setChild(entry.getValue());
      } else if (parent instanceof BinaryOperator) {
        BinaryOperator binary = (BinaryOperator) parent;
        if (binary.getLeft().equals(entry.getKey())) {
          binary.setLeft(entry.getValue());
        } else if (binary.getRight().equals(entry.getKey())) {
          binary.setRight(entry.getValue());
        }
      } else if (parent instanceof TablePrimarySubQuery) {
        TablePrimarySubQuery subQuery = (TablePrimarySubQuery) parent;
        subQuery.setSubquery(entry.getValue());
      } else {
        throw new PlanningException("No such parent who has " + entry.getKey() + " as its child");
      }
    }
    return rewritten;
  }

  static class Context {
    OverridableConf queryContext;
    Map<Selection, Expr> replacedMap;

    public Context(OverridableConf queryContext) {
      this.queryContext = queryContext;
      this.replacedMap = TUtil.newHashMap();
    }
  }

  class Preprocessor extends BaseAlgebraVisitor<Context, Expr> {

    private final static String SUBQUERY_NAME_PREFIX = "SQ_";
    private final static String ALIAS_PREFIX = "generated_";
    private int subQueryNamePostfix = 0;
    private int aliasPostfix = 0;

    private int getSubQueryNamePostfix() {
      return subQueryNamePostfix++;
    }

    private String getNextSubQueryName() {
      return SUBQUERY_NAME_PREFIX + getSubQueryNamePostfix();
    }

//    public Expr postHook(Context ctx, Stack<Expr> stack, Expr expr, Expr current) throws PlanningException {
//      if (current instanceof UnaryOperator) {
//        UnaryOperator unary = (UnaryOperator) current;
//        if (unary.getChild().getType() == OpType.Filter) {
//          if (ctx.replacedMap.containsKey(unary.getChild())) {
//            unary.setChild(ctx.replacedMap.get(unary.getChild()));
//          }
//        }
//      } else if (current instanceof BinaryOperator) {
//        BinaryOperator binary = (BinaryOperator) current;
//        if (binary.getLeft().getType() == OpType.Filter) {
//          if (ctx.replacedMap.containsKey(binary.getLeft())) {
//            binary.setLeft(ctx.replacedMap.get(binary.getLeft()));
//          }
//        } else if (binary.getRight().getType() == OpType.Filter) {
//          if (ctx.replacedMap.containsKey(binary.getRight())) {
//            binary.setRight(ctx.replacedMap.get(binary.getRight()));
//          }
//        }
//      } else if (current instanceof TablePrimarySubQuery) {
//        TablePrimarySubQuery subQuery = (TablePrimarySubQuery) current;
//        if (subQuery.getSubQuery().getType() == OpType.Filter) {
//          if (ctx.replacedMap.containsKey(subQuery.getSubQuery())) {
//            subQuery.setSubquery(ctx.replacedMap.get(subQuery.getSubQuery()));
//          }
//        }
//      }
//      return current;
//    }

    @Override
    public Expr visitFilter(Context ctx, Stack<Expr> stack, Selection expr) throws PlanningException {
      Expr child = super.visitFilter(ctx, stack, expr);

      for (Expr found : ExprTreeUtil.finds(expr.getQual(), OpType.InPredicate)) {
        InPredicate inPredicate = (InPredicate) found;
        if (inPredicate.getInValue().getType() == OpType.SimpleTableSubQuery) {
          Preconditions.checkArgument(inPredicate.getPredicand().getType() == OpType.Column);
          ColumnReferenceExpr predicand = (ColumnReferenceExpr) inPredicate.getPredicand();
          SimpleTableSubQuery subQuery = (SimpleTableSubQuery) inPredicate.getInValue();
          TablePrimarySubQuery primarySubQuery = new TablePrimarySubQuery(getNextSubQueryName(), subQuery.getSubQuery());

          Projection projection = ExprTreeUtil.findTopExpr(primarySubQuery.getSubQuery(), OpType.Projection);
          updateProjection(projection);

          Join join = new Join(inPredicate.isNot() ? JoinType.LEFT_OUTER : JoinType.INNER);
          if (ctx.replacedMap.containsKey(expr)) {
            // This selection has multiple IN subqueries.
            // In this case, the new join must have the join corresponding to the other IN subquery as its child.
            join.setLeft(ctx.replacedMap.get(expr));
//            join.setRight(primarySubQuery);
//
//            join.setQual(buildJoinCondition(primarySubQuery, predicand, projection));
//            ctx.replacedMap.put(expr, join);
          } else {
            if (isRemovable(expr)) {
              join.setLeft(expr.getChild());
            } else {
              removeInQual(expr, inPredicate);
              join.setLeft(expr);
            }
          }

          join.setRight(primarySubQuery);

          // join condition
          join.setQual(buildJoinCondition(primarySubQuery, predicand, projection));

          if (inPredicate.isNot()) {
//            addQual(expr, buildFilterForNotInQuery(predicand));
            Selection newFilter = new Selection(buildFilterForNotInQuery(join.getQual()));
            newFilter.setChild(join);
            ctx.replacedMap.put(expr, newFilter);
          } else {
            ctx.replacedMap.put(expr, join);
          }

//          child = super.visitJoin(ctx, stack, join);
        }
      }
      return child;
    }

    private boolean isRemovable(Selection expr) {
      // if the selection has only one IN subquery qual, it can be removed.
      if (expr.getQual().getType() == OpType.InPredicate) {
        return true;
      } else {
        return false;
      }
    }

    private Selection removeInQual(Selection expr, InPredicate target) throws PlanningException {
      // assume that the target's parent is the binary operator
      BinaryOperator binaryParent = ExprTreeUtil.findParent(expr.getQual(), target);
      if (binaryParent == null) {
        throw new PlanningException("No such parent expression who has " + target + " as its child");
      } else {
        Expr willBeRemained = null;
        if (binaryParent.getLeft().equals(target)) {
          willBeRemained = binaryParent.getRight();
        } else if (binaryParent.getRight().equals(target)) {
          willBeRemained = binaryParent.getLeft();
        } else {
          throw new PlanningException("No such parent expression who has " + target + " as its child");
        }
        // find the grand parent because we should remove the binary parent
        BinaryOperator grandParent = ExprTreeUtil.findParent(expr.getQual(), binaryParent);
        if (grandParent == null) {
          // the binary parent is root
          expr.setQual(willBeRemained);
        } else {
          // connect the grand parent and the remaining predicate
          if (grandParent.getLeft().equals(binaryParent)) {
            grandParent.setLeft(willBeRemained);
          } else if (grandParent.getRight().equals(binaryParent)) {
            grandParent.setRight(willBeRemained);
          } else {
            throw new PlanningException("No such parent expression who has " + binaryParent + " as its child");
          }
        }
      }
      return expr;
    }

    private void updateProjection(Projection projection) {
      // if the child expression does not have any group bys, set distinct
      boolean needDistinct = true;
      Set<Aggregation> aggregations = ExprTreeUtil.finds(projection, OpType.Aggregation);
      if (aggregations.size() == 0) {
        for (NamedExpr namedExpr : projection.getNamedExprs()) {
          if (namedExpr.getExpr().getType() == OpType.CountRowsFunction) {
            needDistinct = false;
          }
        }
        if (needDistinct) {
          projection.setDistinct();
        }
      }

      // if the named expression is not a column reference and does not have an alias, set its alias
      NamedExpr projectExpr = projection.getNamedExprs()[0];
      if (projectExpr.getExpr().getType() != OpType.Column &&
          !projectExpr.hasAlias()) {
        projectExpr.setAlias(generateAlias());
      }
    }

    private BinaryOperator buildJoinCondition(TablePrimarySubQuery subQuery, ColumnReferenceExpr predicand,
                                              Projection projection) {
      NamedExpr projectExpr = projection.getNamedExprs()[0];
      Expr rhs;
      if (projectExpr.getExpr().getType() == OpType.Column) {
        ColumnReferenceExpr projectCol = (ColumnReferenceExpr) projectExpr.getExpr();
        rhs = new ColumnReferenceExpr(subQuery.getName(), projectCol.getName());
      } else {
        rhs = new ColumnReferenceExpr(subQuery.getName(), projectExpr.getAlias());
      }
      BinaryOperator joinCondition = new BinaryOperator(OpType.Equals, predicand, rhs);
      return joinCondition;
    }

    private IsNullPredicate buildFilterForNotInQuery(Expr joinQual) {
      BinaryOperator binaryQual = (BinaryOperator) joinQual;
      return new IsNullPredicate(false, binaryQual.getRight());
    }

    private String generateAlias() {
      return ALIAS_PREFIX + aliasPostfix++;
    }
  }
}
