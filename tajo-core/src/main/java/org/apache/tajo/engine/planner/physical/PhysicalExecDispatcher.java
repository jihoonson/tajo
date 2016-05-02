package org.apache.tajo.engine.planner.physical;

import org.apache.tajo.engine.planner.physical.PhysicalPlanExecutor.ExecEvent;

import java.util.LinkedList;
import java.util.Queue;

public class PhysicalExecDispatcher {

  private final Queue<ExecEvent> eventQueue = new LinkedList<>(); // FIFO queue

  public void handle(ExecEvent event) {
    eventQueue.add(event);
  }

  public ExecEvent get() {
    return eventQueue.remove();
  }

  public void clear() {
    eventQueue.clear();
  }
}
