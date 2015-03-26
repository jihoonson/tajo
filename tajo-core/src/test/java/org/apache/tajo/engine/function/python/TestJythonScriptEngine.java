/*
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

package org.apache.tajo.engine.function.python;

import junit.framework.TestCase;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.tajo.plan.function.OptionalFunctionContext;
import org.apache.tajo.plan.function.python.JythonScriptEngine;
import org.apache.tajo.util.FileUtil;
import org.python.core.PyFunction;

import java.net.URL;

public class TestJythonScriptEngine extends TestCase {
  static final Log LOG = LogFactory.getLog(TestJythonScriptEngine.class);

  public void testGetFunction() throws Exception {
    URL url = FileUtil.getResourcePath("python/test1.py");
    LOG.info("File path: " + url);
    PyFunction function = JythonScriptEngine.getFunction(url.getPath(), "return_one");
    LOG.info(function.getType());
    LOG.info(function.__call__().toString());
  }

  public void testRegisterFunction() throws Exception {
    OptionalFunctionContext context = new OptionalFunctionContext();
    JythonScriptEngine.registerFunctions(context, "python/test1.py", "test");
  }
}