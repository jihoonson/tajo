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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.plan.PlanningException;

import java.util.LinkedHashMap;
import java.util.Map;

public class BaseExpressionRewriteEngine implements ExpressionRewriteEngine {
  /** class logger */
  private Log LOG = LogFactory.getLog(BaseExpressionRewriteEngine.class);

  /** a map for expression rewrite rules */
  private Map<String, ExpressionRewriteRule> rewriteRules = new LinkedHashMap<String, ExpressionRewriteRule>();

  /**
   * Add expression rewrite rules to this engine.
   *
   * @param rules Rule classes
   */
  public void addRewriteRule(Iterable<Class<? extends ExpressionRewriteRule>> rules) {
    for (Class<? extends ExpressionRewriteRule> clazz : rules) {
      try {
        ExpressionRewriteRule rule = clazz.newInstance();
        addRewriteRule(rule);
      } catch (Throwable t) {
        throw new RuntimeException(t);
      }
    }
  }

  /**
   * Add an expression rewrite rule to this engine.
   *
   * @param rule The rule to be added to this engine.
   */
  public void addRewriteRule(ExpressionRewriteRule rule) {
    if (!rewriteRules.containsKey(rule.getName())) {
      rewriteRules.put(rule.getName(), rule);
    }
  }

  /**
   * Rewrite an expression with all rewrite rules added to this engine.
   *
   * @param expr The expression to be rewritten with all rewrite rules.
   * @return The rewritten expression.
   */
  @Override
  public Expr rewrite(OverridableConf queryContext, Expr expr) throws PlanningException {
    ExpressionRewriteRule rule;
    for (Map.Entry<String, ExpressionRewriteRule> rewriteRule : rewriteRules.entrySet()) {
      rule = rewriteRule.getValue();
      if (rule.isEligible(queryContext, expr)) {
        expr = rule.rewrite(queryContext, expr);
        if (LOG.isDebugEnabled()) {
          LOG.debug("The rule \"" + rule.getName() + " \" rewrites the expression.");
        }
      }
    }
    return expr;
  }
}
