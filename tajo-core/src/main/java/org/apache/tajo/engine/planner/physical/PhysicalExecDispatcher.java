package org.apache.tajo.engine.planner.physical;

import org.apache.tajo.engine.planner.physical.PhysicalPlanExecutor.ExecEvent;
import org.apache.tajo.engine.planner.physical.PhysicalPlanExecutor.ExecEventType;
import org.apache.tajo.exception.TajoInternalError;

import java.io.IOException;
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

  public ExecEvent getNextResultFrom(PhysicalExec exec) {
    while (eventQueue.size() > 0) {
      ExecEvent event = eventQueue.remove();
      if (isValidResultOrStopEvent(event)) {
        return event;
      }
    }

    try {
      ExecEvent event;
      do {
        exec.next();
        event = eventQueue.remove();
      } while (!isValidResultOrStopEvent(event));
      return event;
    } catch (IOException e) {
      throw new TajoInternalError(e);
    }
  }

  public void clear() {
    eventQueue.clear();
  }

  private static boolean isValidResultOrStopEvent(ExecEvent event) {
    return event.type == ExecEventType.VALID_RESULT_FOUND ||
        event.type == ExecEventType.NO_MORE_RESULT ||
        event.type == ExecEventType.INTERRUPT;
  }
}
