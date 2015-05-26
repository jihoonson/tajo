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

import org.apache.tajo.OverridableConf;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.plan.PlanningException;

public interface ExpressionRewriteRule {

  /**
   * It returns the rewrite rule name. It will be used for debugging and
   * building a optimization history.
   *
   * @return The rewrite rule name
   */
  String getName();

  /**
   * This method checks if this rewrite rule can be applied to a given query expression.
   *
   * @param expr The expression to be checked
   * @return True if this rule can be applied to a given expression. Otherwise, false.
   */
  boolean isEligible(OverridableConf queryContext, Expr expr);

  /**
   * Updates a query expression and returns an expression rewritten by this rule.
   * It must be guaranteed that the input query expression is not modified even after rewrite.
   * In other words, the rewrite has to modify an plan copied from the input plan.
   *
   * @param expr Input expression. It will not be modified.
   * @return The rewritten expression.
   */
  Expr rewrite(OverridableConf queryContext, Expr expr) throws PlanningException;
}
