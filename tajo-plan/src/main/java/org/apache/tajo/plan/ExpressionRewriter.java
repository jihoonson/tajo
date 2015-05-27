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

package org.apache.tajo.plan;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.OverridableConf;
import org.apache.tajo.algebra.Expr;
import org.apache.tajo.conf.TajoConf;
import org.apache.tajo.plan.rewrite.BaseExpressionRewriteEngine;
import org.apache.tajo.plan.rewrite.ExpressionRewriteRuleProvider;
import org.apache.tajo.util.ReflectionUtil;

public class ExpressionRewriter {
  private final static Log LOG = LogFactory.getLog(ExpressionRewriter.class);

  private BaseExpressionRewriteEngine rewriteEngine;

  public ExpressionRewriter(TajoConf conf) {
    Class clazz = conf.getClassVar(TajoConf.ConfVars.EXPRESSION_REWRITE_RULE_PROVIDER_CLASS);
    ExpressionRewriteRuleProvider provider = (ExpressionRewriteRuleProvider) ReflectionUtil.newInstance(clazz, conf);
    rewriteEngine = new BaseExpressionRewriteEngine();
    rewriteEngine.addRewriteRule(provider.getRules());
  }

  public Expr rewrite(OverridableConf context, Expr expr) throws PlanningException {
    return rewriteEngine.rewrite(context, expr);
  }
}
