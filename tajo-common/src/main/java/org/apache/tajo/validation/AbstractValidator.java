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

package org.apache.tajo.validation;

import java.util.Collection;

import org.apache.tajo.util.TUtil;

public abstract class AbstractValidator implements Validator {
  
  protected abstract <T> String getErrorMessage(T object);
  protected abstract <T> boolean validateInternal(T object);
  protected abstract Collection<Validator> getDependantValidators();

  @Override
  public <T> Collection<ConstraintViolation> validate(T object) {
    Collection<ConstraintViolation> violations = TUtil.newHashSet();
    
    if (!validateInternal(object)) {
      ConstraintViolation violation = new ConstraintViolation();
      violation.setMessage(getErrorMessage(object));
      violation.setValidatorClazz(this.getClass());
      violations.add(violation);
    }
      
    for (Validator dependantValidator: getDependantValidators()) {
      violations.addAll(dependantValidator.validate(object));
    }
    
    return violations;
  }
  
  @Override
  public <T> void validate(T object, boolean generateThrow) {
    Collection<ConstraintViolation> violations = validate(object);
    
    if (violations.size() > 0) {
      if (generateThrow) {
        throw new ConstraintViolationException(violations);
      }
    }
  }

}
