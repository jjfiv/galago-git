// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.tupleflow.execution;

import java.util.List;

/**
 *
 * @author trevor
 */
public interface StageExecutionStatus {
    public String getName();
    public int getBlockedInstances();
    public int getQueuedInstances();
    public int getRunningInstances();
    public int getCompletedInstances();

    public boolean isDone();
    public List<Exception> getExceptions();

    // Added by irmarc (9/16/2010)
    public List<Double> getRunTimes();
}
