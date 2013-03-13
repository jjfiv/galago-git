// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow.execution;

import java.io.File;
import java.io.FileOutputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 *
 * @author trevor
 */
public abstract class RemoteStageExecutor implements StageExecutor {

  public abstract StageExecutionStatus submit(String stageName, ArrayList<String> jobPaths, String temporary);

  public abstract void shutdown();

  public StageExecutionStatus execute(StageGroupDescription stage, String temporary) {
    ArrayList<String> jobPaths = new ArrayList<String>();

    try {
      String output = temporary + File.separator + "stdout";
      new File(output).mkdir();
      String stderr = temporary + File.separator + "stderr";
      new File(stderr).mkdir();
      String jobsDirectory = temporary + File.separator + "jobs";
      new File(jobsDirectory).mkdir();
      String stageJobsDirectory = jobsDirectory + File.separator + stage.getName();
      new File(stageJobsDirectory).mkdir();

      List<StageInstanceDescription> instances = stage.getInstances();

      for (int i = 0; i < instances.size(); i++) {
        File instanceJobFile = new File(stageJobsDirectory + File.separator + i);
        File instanceCheckpoint = new File(
                stageJobsDirectory + File.separator + i + ".complete");

        if (instanceCheckpoint.exists()) {
          continue;
        }
        ObjectOutputStream instanceJobStream = new ObjectOutputStream(new FileOutputStream(
                instanceJobFile));
        StageInstanceDescription instance = instances.get(i);
        instanceJobStream.writeObject(instance);
        instanceJobStream.close();

        jobPaths.add(instanceJobFile.toString());
      }
    } catch (Exception e) {
      return new ErrorExecutionStatus(stage.getName(), e);
    }

    return submit(stage.getName(), jobPaths, temporary);
  }
}
