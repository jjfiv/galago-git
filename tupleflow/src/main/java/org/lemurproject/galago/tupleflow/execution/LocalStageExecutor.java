// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow.execution;

import org.lemurproject.galago.tupleflow.ExNihiloSource;
import org.lemurproject.galago.tupleflow.GalagoConf;
import org.xml.sax.SAXException;

import javax.xml.parsers.ParserConfigurationException;
import java.io.*;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;

/**
 *
 * @author trevor
 */
public class LocalStageExecutor implements StageExecutor {

  public static class SequentialExecutionContext implements StageExecutionStatus, Runnable {

    String name;
    List<StageInstanceDescription> instances;
    ArrayList<Exception> exceptions = new ArrayList();
    int queuedInstances = 0;
    int runningInstances = 0;
    int completedInstances = 0;
    boolean done = false;

    public SequentialExecutionContext(String name, List<StageInstanceDescription> instances) {
      this.name = name;
      this.instances = instances;
      this.queuedInstances = instances.size();
    }

    @Override
    public void run() {
      NetworkedCounterManager manager = new NetworkedCounterManager();
      StageInstanceFactory factory = new StageInstanceFactory(manager);
      manager.start();

      for (StageInstanceDescription instance : instances) {
        synchronized (this) {
          runningInstances += 1;
          queuedInstances -= 1;
        }

        try {
          ExNihiloSource source = factory.instantiate(instance);
          source.run();

        } catch (Throwable err) {
          err.printStackTrace();
          addException(new Exception(err));
        }

        synchronized (this) {
          runningInstances -= 1;
          completedInstances += 1;
        }

        if (exceptions.size() > 0) {
          // we have an exception - break and quit cleanly.
          break;
        }
      }
      synchronized (this) {
        manager.stop();
        done = true;
      }
    }

    @Override
    public String getName() {
      return name;
    }

    @Override
    public int getBlockedInstances() {
      return 0;
    }

    @Override
    public synchronized int getQueuedInstances() {
      return queuedInstances;
    }

    @Override
    public synchronized int getRunningInstances() {
      return runningInstances;
    }

    @Override
    public int getCompletedInstances() {
      return completedInstances;
    }

    @Override
    public synchronized boolean isDone() {
      return done;
    }

    @Override
    public synchronized List<Exception> getExceptions() {
      return exceptions;
    }

    @Override
    public synchronized List<Double> getRunTimes() {
      ArrayList<Double> times = new ArrayList();
      // do something
      return times;
    }

    private synchronized void addException(Exception e) {
      exceptions.add(e);
    }
  }

  @Override
  public SequentialExecutionContext execute(StageGroupDescription stage, String temporary) {
    SequentialExecutionContext context =
            new SequentialExecutionContext(stage.getName(), stage.getInstances());
    context.run();
    return context;
  }

  public StageExecutionStatus execute(String descriptionFile) {
    File errorFile = new File(descriptionFile + ".error");
    File completeFile = new File(descriptionFile + ".complete");

    SequentialExecutionContext result = null;

    Logger logger = Logger.getLogger(JobExecutor.class
            .toString());
    StageInstanceDescription stage = null;

    // try to parse the stage description from disk

    try {
      ObjectInputStream stream =
              new ObjectInputStream(new FileInputStream(new File(descriptionFile)));
      stage = (StageInstanceDescription) stream.readObject();
    } catch (Exception e) {
      return new ErrorExecutionStatus("unknown", e);
    }

    // check for a checkpoint file.  If one exists, we quit.
    if (completeFile.exists()) {
      logger.info("Exiting early because a complete checkpoint was found.");
      result = new SequentialExecutionContext(stage.getName(),
              Collections.singletonList(stage));
      return result;
    }

    // get rid of any error checkpoints
    if (errorFile.exists()) {
      errorFile.delete();
    }

    logger.info("Stage create " + descriptionFile + " initialized. Executing.");

    // now. run this job.
    result = new SequentialExecutionContext(stage.getName(),
            Collections.singletonList(stage));

    result.run();

    try {
      if (result.getExceptions().size() > 0) {
        Throwable e = result.getExceptions().get(0);
        BufferedWriter writer = new BufferedWriter(new FileWriter(errorFile));
        writer.write(e.toString());
        writer.close();
      } else {

        // ensure the .complete file exists before terminating -- to avoid spurious errors from slow file systems.
        completeFile.createNewFile();
        int count = 0;
        do {
          if (completeFile.exists()) {
            break;
          }
          try {
            Thread.sleep(1000); // wait 1 second
          } catch (Exception e) {
          } // Don't care about interruption errors
        } while (count++ < 60); // 1 min timeout

      }
    } catch (IOException e) {
      logger.warning("Trouble writing completion/error files: " + errorFile.toString());
      // not much else we can do.
    }
    return result;
  }

  @Override
  public void shutdown() {
  }

  public static void main(String[] args) throws UnsupportedEncodingException, ParserConfigurationException, SAXException, IOException, ClassNotFoundException {
    Logger logger = Logger.getLogger(JobExecutor.class
            .toString());

    String stageDescriptionFile = args[0];

    logger.info(
            "Initializing: " + stageDescriptionFile);

    // This should also trigger the static initializer on GalagoConf which will load the parameters if need be.
    // It also nicely dumps them to a log so you should be able to tell if its using the file you think you're using.
    logger.info(".galago.conf: "+GalagoConf.getAllOptions().toString());

    // Need this to deal with slow lustre filesystem on swarm...
    int count = 0;


    do {
      File descFile = new File(stageDescriptionFile);
      if (descFile.canRead()) {
        break;
      }
      try {
        Thread.sleep(1000); // wait 1 second
      } catch (Exception e) {
      } // Don't care about interruption errors
    } while (count++ < 60000); // 1 min timeout

    StageExecutionStatus context = new LocalStageExecutor().execute(stageDescriptionFile);

    if (context.getExceptions()
            .size() > 0) {
      logger.severe("Exception thrown: " + context.getExceptions().get(0).toString());
      System.exit(1); // force quit on any remaining threads (particularly: NetworkCounter)
    } else {
      logger.info("Local Stage complete");
    }
  }
}
