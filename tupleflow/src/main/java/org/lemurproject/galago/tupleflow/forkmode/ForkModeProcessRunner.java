package org.lemurproject.galago.tupleflow.forkmode;

import java.io.IOException;

/**
 * @author jfoley
 */
public class ForkModeProcessRunner implements Runnable {
  private final String id;
  private final ProcessBuilder command;
  private Integer returnCode = null;
  private long stopTime;

  public ForkModeProcessRunner(String id, ProcessBuilder process) throws IOException {
    this.command = process;
    this.id = id;
  }

  public synchronized boolean isStillRunning() {
    return returnCode == null;
  }

  /**
   * Only valid if not running anymore.
   */
  public synchronized long getStopTime() {
    return stopTime;
  }

  @Override
  public void run() {
    Process process = null;
    try {
      process = command.start();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }

    while (true) {
      try {
        int rc = process.exitValue();
        synchronized (this) {
          returnCode = rc;
          stopTime = System.currentTimeMillis();
        }
        break;
      } catch (IllegalThreadStateException ex) {
        try {
          Thread.sleep(1000L);
        } catch (InterruptedException ignored) {
        }
      }
    }

    // now done, check for .complete and .error:
  }

  public String getName() {
    return id;
  }

  public synchronized int getReturnCode() {
    if(returnCode == null) throw new IllegalStateException("Must check to see if this is done before asking for our return code!");
    return returnCode;
  }
}
