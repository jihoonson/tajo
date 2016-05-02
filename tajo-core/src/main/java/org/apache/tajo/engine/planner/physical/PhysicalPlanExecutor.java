package org.apache.tajo.engine.planner.physical;

import org.apache.tajo.annotation.NotNull;
import org.apache.tajo.annotation.Nullable;
import org.apache.tajo.exception.TajoInternalError;
import org.apache.tajo.storage.Tuple;
import org.apache.tajo.worker.TaskAttemptContext;

import java.io.IOException;

public class PhysicalPlanExecutor {

  private enum ExecState {
    NEW,
    INITED,
    RUNNING,
    DONE
  }

  public enum ExecEventType {
    INIT,
    VALID_RESULT_FOUND,
    MORE_RESULT_AVAILABLE,
    NO_MORE_RESULT,
    INTERRUPT,
  }

  public static class ExecEvent {
    ExecEventType type;
    Tuple result;

    public ExecEvent() {}

    public ExecEvent(final @NotNull ExecEventType type, final @Nullable Tuple result) {
      this.set(type, result);
    }

    public void set(final @NotNull ExecEventType type, final @Nullable Tuple result) {
      this.type = type;
      this.result = result;
    }
  }

  private ExecState state = ExecState.NEW;
  private final TaskAttemptContext context;
  private final PhysicalExec exec;
  private final PhysicalExecDispatcher dispatcher;

  public PhysicalPlanExecutor(TaskAttemptContext context, PhysicalExec exec) {
    this.context = context;
    this.exec = exec;
    this.dispatcher = context.getDispatcher();
  }

  public void init() throws IOException {
    exec.init();
    state = ExecState.INITED;
  }

  public void run() {
    if (state != ExecState.INITED) {
      throw new TajoInternalError("Physical exec is not inited");
    }

    state = ExecState.RUNNING;

    while (!context.isStopped()) {
      final ExecEvent event;
      event = dispatcher.get();

      switch (event.type) {
        case VALID_RESULT_FOUND:
          // TODO: evaluate terminate condition
          break;
        case MORE_RESULT_AVAILABLE:
          break;
        case NO_MORE_RESULT:
          context.stop();
          state = ExecState.DONE;
          break;
      }
    }
  }
}
