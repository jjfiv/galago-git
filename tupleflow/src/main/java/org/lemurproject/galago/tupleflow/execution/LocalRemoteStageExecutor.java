// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.tupleflow.execution;

import java.util.ArrayList;
import java.util.concurrent.Future;

/**
 * This executor has no practical use at all.  It's only here to make it easy
 * to test the RemoteExecutor base class.
 *
 * This executor is essentially the same as the LocalStageExecutor in that
 * it runs everything in a single thread, but it serializes the job information
 * and then reads it again.
 *
 * @author trevor
 */
public class LocalRemoteStageExecutor extends RemoteStageExecutor {
    public StageExecutionStatus submit(String stageName, ArrayList<String> jobPaths, String temporary) {
        StageExecutionStatus status = null;

        for (String jobPath : jobPaths) {
            status = new LocalStageExecutor().execute(jobPath);

            if (status.getExceptions().size() > 0) {
                return status;
            }
        }

        return status;
    }

    public void shutdown() {
    }
}
