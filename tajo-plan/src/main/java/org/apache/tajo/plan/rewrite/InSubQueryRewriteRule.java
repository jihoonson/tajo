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
import org.apache.tajo.plan.visitor.SimpleAlgebraVisitor;
import org.apache.tajo.util.Pair;
import org.apache.tajo.util.TUtil;

import java.util.*;

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
          preprocessor.init();
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

    for (Pair<Selection, Expr> entry : context.willBeReplacedExprs) {
      Expr parent = ExprTreeUtil.findParent(rewritten, entry.getFirst());

      if (parent == null) {
        throw new PlanningException("No such parent who has " + entry.getFirst() + " as its child");
      }

      if (parent instanceof UnaryOperator) {
        ((UnaryOperator) parent).setChild(entry.getSecond());
      } else if (parent instanceof BinaryOperator) {
        BinaryOperator binary = (BinaryOperator) parent;
        if (binary.getLeft().identical(entry.getFirst())) {
          binary.setLeft(entry.getSecond());
        } else if (binary.getRight().identical(entry.getFirst())) {
          binary.setRight(entry.getSecond());
        }
      } else if (parent instanceof TablePrimarySubQuery) {
        TablePrimarySubQuery subQuery = (TablePrimarySubQuery) parent;
        subQuery.setSubquery(entry.getSecond());
      } else {
        throw new PlanningException("No such parent who has " + entry.getFirst() + " as its child");
      }
    }

//    for (Map.Entry<Selection, List<InPredicate>> entry : context.willBeRemovedQuals.entrySet()) {
    for (Pair<Selection, List<InPredicate>> entry : context.willBeRemovedQuals) {
      Selection selection = entry.getFirst();
      for (InPredicate qual : entry.getSecond()) {
        removeInQual(selection, qual);
      }
    }

//    for (Projection projection : context.willbeUpdatedProjections) {
//      updateProjection(projection);
//    }
    return rewritten;
  }

  private static Selection removeInQual(Selection expr, InPredicate target) throws PlanningException {
    if (expr.getQual().identical(target)) {
      expr.setQual(null);
    } else {
      // assume that the target's parent is the binary operator
      BinaryOperator binaryParent = ExprTreeUtil.findParent(expr.getQual(), target);
      if (binaryParent == null) {
        throw new PlanningException("No such parent expression who has " + target + " as its child");
      } else {
        Expr willBeRemained = null;
        if (binaryParent.getLeft().identical(target)) {
          willBeRemained = binaryParent.getRight();
        } else if (binaryParent.getRight().identical(target)) {
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
          if (grandParent.getLeft().identical(binaryParent)) {
            grandParent.setLeft(willBeRemained);
          } else if (grandParent.getRight().identical(binaryParent)) {
            grandParent.setRight(willBeRemained);
          } else {
            throw new PlanningException("No such parent expression who has " + binaryParent + " as its child");
          }
        }
      }
    }

    return expr;
  }

  private final static String ALIAS_PREFIX = "generated_";
  private static int aliasPostfix = 0;

  private static void updateProjection(Projection projection) {
    // if the child expression does not have any group bys, set distinct
    boolean needDistinct = true;
    Set<Aggregation> aggregations = ExprTreeUtil.finds(projection, OpType.Aggregation);
    if (aggregations.size() == 0) {
      for (NamedExpr namedExpr : projection.getNamedExprs()) {
        needDistinct =
            ExprTreeUtil.finds(namedExpr.getExpr(), OpType.CountRowsFunction).size() == 0;
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

  private static String generateAlias() {
    return ALIAS_PREFIX + aliasPostfix++;
  }

  static class Context {
    OverridableConf queryContext;
    List<Pair<Selection, Expr>> willBeReplacedExprs;
//    Map<Selection, List<InPredicate>> willBeRemovedQuals;
    List<Pair<Selection, List<InPredicate>>> willBeRemovedQuals;
    List<Projection> willbeUpdatedProjections;

    public Context(OverridableConf queryContext) {
      this.queryContext = queryContext;
      this.willBeReplacedExprs = TUtil.newList();
//      this.willBeRemovedQuals = TUtil.newHashMap();
      this.willBeRemovedQuals = TUtil.newList();
      this.willbeUpdatedProjections = TUtil.newList();
    }
  }

  class Preprocessor extends BaseAlgebraVisitor<Context, Expr> {

    private final static String SUBQUERY_NAME_PREFIX = "SQ_";
    private int subQueryNamePostfix = 0;

    public void init() {
      subQueryNamePostfix = 0;
    }

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
//          if (ctx.replacedMap.isWillBeReplaced(binary.getLeft())) {
//            binary.setLeft(ctx.replacedMap.getWillBeReplaced(binary.getLeft()));
//          }
//        } else if (binary.getRight().getType() == OpType.Filter) {
//          if (ctx.replacedMap.isWillBeReplaced(binary.getRight())) {
//            binary.setRight(ctx.replacedMap.getWillBeReplaced(binary.getRight()));
//          }
//        }
//      } else if (current instanceof TablePrimarySubQuery) {
//        TablePrimarySubQuery subQuery = (TablePrimarySubQuery) current;
//        if (subQuery.getSubQuery().getType() == OpType.Filter) {
//          if (ctx.replacedMap.isWillBeReplaced(subQuery.getSubQuery())) {
//            subQuery.setSubquery(ctx.replacedMap.getWillBeReplaced(subQuery.getSubQuery()));
//          }
//        }
//      }
//      return current;
//    }

    @Override
    public Expr visitFilter(Context ctx, Stack<Expr> stack, Selection expr) throws PlanningException {
      Expr child = super.visitFilter(ctx, stack, expr);

      for (Expr found : ExprTreeUtil.findsInOrder(expr.getQual(), OpType.InPredicate)) {
        InPredicate inPredicate = (InPredicate) found;
        if (inPredicate.getInValue().getType() == OpType.SimpleTableSubQuery) {
          Preconditions.checkArgument(inPredicate.getPredicand().getType() == OpType.Column);
          ColumnReferenceExpr predicand = (ColumnReferenceExpr) inPredicate.getPredicand();
          SimpleTableSubQuery subQuery = (SimpleTableSubQuery) inPredicate.getInValue();
          TablePrimarySubQuery primarySubQuery = new TablePrimarySubQuery(getNextSubQueryName(),
              subQuery.getSubQuery());
//          TUtil.putToNestedList(ctx.willBeRemovedQuals, expr, inPredicate);
          addWillBeRemovedQual(ctx, expr, inPredicate);

          Projection projection = ExprTreeUtil.findTopExpr(primarySubQuery.getSubQuery(), OpType.Projection);
          updateProjection(projection);
//          ctx.willbeUpdatedProjections.add(projection);

          // TODO: change left_outer to left_anti
          Join join = new Join(inPredicate.isNot() ? JoinType.LEFT_OUTER : JoinType.LEFT_SEMI);
          if (isWillBeReplaced(ctx, expr)) {
            // This selection has multiple IN subqueries.
            // In this case, the new join must have the join corresponding to the other IN subquery as its child.
            join.setLeft(getWillBeReplaced(ctx, expr));
//            join.setRight(primarySubQuery);
//
//            join.setQual(buildJoinCondition(primarySubQuery, predicand, projection));
//            ctx.replacedMap.put(expr, join);
          } else {
            if (isRemovable(ctx, expr)) {
              join.setLeft(expr.getChild());
            } else {
//              removeInQual(expr, inPredicate);
              join.setLeft(expr);
            }
          }

          join.setRight(primarySubQuery);

          // join condition
          join.setQual(buildJoinCondition(primarySubQuery, predicand, projection));

          // TODO
          if (inPredicate.isNot()) {
//            addQual(expr, buildFilterForNotInQuery(predicand));
            Selection newFilter = new Selection(buildFilterForNotInQuery(join.getQual()));
            newFilter.setChild(join);
            addWillBeReplaced(ctx, expr, newFilter);
          } else {
            addWillBeReplaced(ctx, expr, join);
          }

//          child = super.visitJoin(ctx, stack, join);
        }
      }
      return child;
    }

    private boolean isWillBeReplaced(Context context, Selection expr) {
      for (Pair<Selection, Expr> pair : context.willBeReplacedExprs) {
        if (pair.getFirst().identical(expr)) {
          return true;
        }
      }
      return false;
    }

    private Expr getWillBeReplaced(Context context, Selection expr) {
      for (Pair<Selection, Expr> pair : context.willBeReplacedExprs) {
        if (pair.getFirst().identical(expr)) {
          return pair.getSecond();
        }
      }
      return null;
    }

    private void addWillBeReplaced(Context context, Selection oldExpr, Expr newExpr) {
      int i = 0;
      for (i = 0; i < context.willBeReplacedExprs.size(); i++) {
        if (context.willBeReplacedExprs.get(i).getFirst().identical(oldExpr)) {
          break;
        }
      }
      if (i < context.willBeReplacedExprs.size()) {
        context.willBeReplacedExprs.remove(i);
      }
      context.willBeReplacedExprs.add(new Pair<Selection, Expr>(oldExpr, newExpr));
    }

    private boolean hasWillBeRemovedQual(Context context, Selection selection) {
      for (Pair<Selection, List<InPredicate>> pair : context.willBeRemovedQuals) {
        if (pair.getFirst().identical(selection)) {
          return true;
        }
      }
      return false;
    }

    private List<InPredicate> getWillBeRemovedQuals(Context context, Selection selection) {
      for (Pair<Selection, List<InPredicate>> pair : context.willBeRemovedQuals) {
        if (pair.getFirst().identical(selection)) {
          return pair.getSecond();
        }
      }
      return null;
    }

    private void addWillBeRemovedQual(Context context, Selection selection, InPredicate qual) {
      Pair<Selection, List<InPredicate>> update = null;
      for (Pair<Selection, List<InPredicate>> pair : context.willBeRemovedQuals) {
        if (pair.getFirst().identical(selection)) {
          update = pair;
        }
      }
      if (update == null) {
        update = new Pair<Selection, List<InPredicate>>(selection, new ArrayList<InPredicate>());
        context.willBeRemovedQuals.add(update);
      }
      update.getSecond().add(qual);
    }

    private boolean isRemovable(Context ctx, Selection expr) throws PlanningException {
      // if the selection has only one IN subquery qual, it can be removed.
      if (expr.getQual().getType() == OpType.InPredicate) {
        return true;
      } else {
//        int removedQualsNum = hasWillBeRemovedQual(ctx, expr) ? getWillBeRemovedQuals(ctx, expr).size() : 0;
        int inPredicateNum = ExprTreeUtil.finds(expr.getQual(), OpType.InPredicate).size();
//        int inPredicateNum = ExprTreeUtil.finds(expr.getQual(), OpType.InPredicate).size();
        int leafQualNum = new LeafQualFinder().find(expr.getQual()).size();
        if (inPredicateNum == leafQualNum) {
          return true;
        } else {
          return false;
        }
      }

//      if (expr.getQual().getType() == OpType.InPredicate) {
//        return true;
//      } else {
//        return false;
//      }
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
  }

  static class LeafQualFinderContext {
    Set<Expr> set = new HashSet<Expr>();
  }

  static class LeafQualFinder extends SimpleAlgebraVisitor<LeafQualFinderContext, Object> {

    public Set<Expr> find(Expr expr) throws PlanningException {
      LeafQualFinderContext context = new LeafQualFinderContext();
      this.visit(context, new Stack<Expr>(), expr);
      return context.set;
    }

    public void preHook(LeafQualFinderContext ctx, Stack<Expr> stack, Expr expr) {
      if (expr instanceof BinaryOperator) {
        BinaryOperator binary = (BinaryOperator) expr;
        if (!(binary.getLeft() instanceof BinaryOperator) &&
            !(binary.getRight() instanceof BinaryOperator)) {
          ctx.set.add(expr);
        }
      }
    }
  }
}
