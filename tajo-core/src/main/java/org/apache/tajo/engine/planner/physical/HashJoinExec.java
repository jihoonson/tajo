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

package org.apache.tajo.engine.planner.physical;

import org.apache.tajo.engine.planner.physical.PhysicalPlanExecutor.ExecEvent;
import org.apache.tajo.engine.planner.physical.PhysicalPlanExecutor.ExecEventType;
import org.apache.tajo.plan.logical.JoinNode;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;
import java.util.Iterator;

public class HashJoinExec extends CommonHashJoinExec<TupleList> {

  public HashJoinExec(TaskAttemptContext context, JoinNode plan, PhysicalExec leftExec,
      PhysicalExec rightExec) {
    super(context, plan, leftExec, rightExec);
  }

  @Override
  protected TupleMap<TupleList> convert(TupleMap<TupleList> hashed, boolean fromCache)
      throws IOException {
    return fromCache ? new TupleMap<>(hashed) : hashed;
  }

  @Override
  public void next() throws IOException {
    super.next();

    if (first) {
      loadRightToHashTable();
    }

//    while (!context.isStopped() && !finished) {
    if (iterator != null && iterator.hasNext()) {
      frameTuple.setRight(iterator.next());
      returnResult(ExecEventType.VALID_RESULT_FOUND, frameTuple);
      return;
//      return projector.eval(frameTuple);
    }

//      Tuple leftTuple = leftChild.next(); // it comes from a disk
    ExecEvent event = context.getDispatcher().getNextResultFrom(leftChild);
    if (hasNextInput(event)) {
      Tuple leftTuple = event.result;
      if (leftFiltered(leftTuple)) {
        returnResult(ExecEventType.MORE_RESULT_AVAILABLE, null);
        return;
      }

      frameTuple.setLeft(leftTuple);

      // getting corresponding right
      Iterable<Tuple> hashed;
      if (!isCrossJoin) {
        hashed = tupleSlots.get(leftKeyExtractor.project(leftTuple));
      } else {
        hashed = tupleSlots.get(null);
      }
      Iterator<Tuple> rightTuples = rightFiltered(hashed);
      if (rightTuples.hasNext()) {
        iterator = rightTuples;
      }
      if (iterator.hasNext()) {
        frameTuple.setRight(iterator.next());
        returnResult(ExecEventType.VALID_RESULT_FOUND, frameTuple);
      } else {
        returnResult(ExecEventType.MORE_RESULT_AVAILABLE, null);
      }
    } else {
      returnResult(ExecEventType.NO_MORE_RESULT, null);
    }
  }
}
