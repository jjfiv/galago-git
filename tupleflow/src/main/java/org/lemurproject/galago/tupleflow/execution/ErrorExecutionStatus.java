// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.tupleflow.execution;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A dummy execution status object that wraps an error.
 * 
 * @author trevor
 */
public class ErrorExecutionStatus implements StageExecutionStatus {
    List<Exception> exceptions;
    String name;

    public ErrorExecutionStatus(String name, Exception e) {
        this.name = name;
        this.exceptions = Collections.singletonList(e);
    }

    public String getName() { return name; }
    public int getBlockedInstances() { return 0; }
    public int getQueuedInstances() { return 0; }
    public int getRunningInstances() { return 0; }
    public int getCompletedInstances() { return 0; }
    public boolean isDone() { return true; }
    public List<Exception> getExceptions() { return exceptions; }
    public List<Double> getRunTimes() { return new ArrayList(); }
}
