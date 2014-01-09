// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow.execution;

import java.io.BufferedReader;
import java.net.UnknownHostException;
import org.mortbay.jetty.Server;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.StageGroupDescription.DataPipeRegion;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import org.lemurproject.galago.tupleflow.CompressionType;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * <p>This class is responsible for executing TupleFlow jobs.</p>
 *
 * <p>A job is specified using the TupleFlow Job class. The Job class has an XML
 * form which can be parsed by JobConstructor, but you can create one
 * programmatically as well.</p>
 *
 * <p>Before the job is executed, it is verified. JobExecutor verifies that all
 * the classes references by the Job object actually exist, and that the
 * connections point sensible places. Once it has been verified, the JobExecutor
 * builds an execution plan that will execute the job with as much parallelism
 * as possible while not violate any ordering constraints dictated by stage
 * connections. After the plan is generated, the job is sent to a StageExecutor
 * for the low-level details of execution.</p>
 *
 * <p>TupleFlow has many different kinds of StageExecutors you can use. To get
 * started and to debug your code, use the LocalStageExecutor or
 * ThreadedExecutor. To harness more parallelism, use the SSHStageExecutor or
 * the DRMAAStageExecutor.</p>
 *
 * <p>(12/01/2010, irmarc): Added a bit of code to print out the server url on
 * command. Hit 'space' to see it.</p>
 *
 * @author trevor
 * @author irmarc
 */
public class JobExecutor {

  ErrorStore store;
  Job job;
  String temporaryStorage;
  int defaultHashCount = 10;
  long maximumFileInputs = 200;
  HashMap<String, ConnectionDescription> connections = new HashMap<String, ConnectionDescription>();
  HashMap<String, StageGroupDescription> stages = new HashMap<String, StageGroupDescription>();
  ArrayList<String> stageOrder = new ArrayList<String>();
  HashMap<String, HashSet<String>> stageChildren = new HashMap<String, HashSet<String>>();
  HashMap<String, HashSet<String>> stageParents = new HashMap<String, HashSet<String>>();
  ArrayList<DataPipe> pipes = new ArrayList<DataPipe>();

  public JobExecutor(Job job, String temporaryStorage, ErrorStore store) {
    this.store = store;
    this.temporaryStorage = temporaryStorage;
    this.job = job;
  }

  /**
   * This method tries to combine stages together to reduce overhead.
   *
   * In particular, this method looks for two stages, A and B, where each copy
   * of B takes input from only one copy of stage A. In this case, all the steps
   * from B are moved into stage A, saving a lot of file overhead in
   * transferring tuples from A to B.
   *
   * This method is particularly useful for jobs that are created automatically.
   *
   * @param job The job instance to optimize.
   * @return A new job instance, perhaps with fewer stages.
   */
  public static Job optimize(Job job) {
    // First, figure out what source gets output from the stage, if any.
    HashMap<String, String> outputs = new HashMap<String, String>();
    HashMap<String, String> inputs = new HashMap<String, String>();

    for (Stage stage : job.stages.values()) {
      // Output mapping
      if (stage.steps.size() > 0) {
        Step lastStep = stage.steps.get(stage.steps.size() - 1);
        if (lastStep instanceof OutputStep) {
          OutputStep output = (OutputStep) lastStep;
          outputs.put(stage.name, output.getId());
        }

        Step firstStep = stage.steps.get(0);
        if (firstStep instanceof InputStep) {
          InputStep input = (InputStep) firstStep;
          inputs.put(stage.name, input.getId());
        }
      }
    }

    // Create a mapping from String -> Stage
    HashMap<String, Stage> stages = new HashMap<String, Stage>(job.stages);

    // Now, rip through the connections, and try to find a connection that links
    // one of these inputs to one of the outputs.

    Iterator<Connection> iterator = job.connections.iterator();
    Connection connection;

    innerLoop:
    while (iterator.hasNext()) {
      connection = iterator.next();

      // For simplicity, find just connections with single inputs and outputs and no hashing
      if (connection.outputs.size() < 1) {
        continue;
      }
      if (connection.getHash() != null) {
        continue;
      }
      ConnectionEndPoint connectionInput = connection.input;
      String stageOutputPointId = outputs.get(connectionInput.getStageName());

      // if the input to this connection is not the main output of a stage, skip
      if (!connectionInput.getPointName().equals(stageOutputPointId)) {
        continue;            // make sure all of the outputs are the inputs of stages
      }
      for (ConnectionEndPoint connectionOutput : connection.outputs) {
        String stageInputPointId = inputs.get(connectionOutput.getStageName());

        if (!connectionOutput.getPointName().equals(stageInputPointId)) {
          continue innerLoop;
        }
        if (connectionOutput.getAssignment() == ConnectionAssignmentType.Combined) {
          continue innerLoop;
        }
      }

      // now we've verified that these stages can be combined together.
      Stage source = stages.get(connectionInput.getStageName());

      MultiStep multi = new MultiStep();

      for (ConnectionEndPoint connectionOutput : connection.outputs) {
        Stage destination = stages.get(connectionOutput.getStageName());
        // getting ready: remove the first step, add on to the multi
        int length = destination.steps.size();
        multi.addGroup(new ArrayList<Step>(destination.steps.subList(1, length)));

        renameConnections(job, source, destination);

        // combine dependence information
        source.connections.remove(connectionInput.getPointName());
        destination.connections.remove(connectionOutput.getPointName());
        source.connections.putAll(destination.connections);

        // remove the destination stage
        job.stages.remove(destination);
      }

      source.steps.remove(source.steps.size() - 1);

      // only add a multi step if there were multiple outputs
      if (multi.isSingleton()) {
        source.steps.addAll(multi.singleton());
      } else {
        source.steps.add(multi);
      }

      // remove this connection
      iterator.remove();

      // recurse to remove other connections
      return optimize(job);
    }

    return job;
  }

  public static void renameConnections(Job job, Stage source, Stage destination) {
    // for each connection, rename dest -> source.
    for (Connection connection : job.connections) {
      if (connection.input.getStageName().equals(destination.name)) {
        connection.input.setStageName(source.name);
      }

      for (ConnectionEndPoint output : connection.outputs) {
        if (output.getStageName().equals(destination.name)) {
          output.setStageName(source.name);
        }
      }
    }
  }

  public void prepare() {
    boolean successful = constructAndVerify();

    if (!successful) {
      return;
    }

    // look through each stage to see how many open files it will have.
    // if a particular stage will have a lot of open files, add some
    // intermediate merge stages.
    if (needsMergeStages()) {
      addMergeStages(job);
      constructAndVerify();
    }

    for (DataPipe pipe : pipes) {
      pipe.makeDirectories();
    }
  }

  public void clear() {
    connections.clear();
    stageChildren.clear();
    stageOrder.clear();
    stageParents.clear();
    stages.clear();
    pipes.clear();
  }

  /**
   * Checks to see if any stage has too many inputs.
   *
   * @return true, if there is a stage with more than maximumFileInputs.
   */
  public boolean needsMergeStages() {
    for (StageGroupDescription stage : stages.values()) {
      long totalInputs = 0;

      if (stage.getName().endsWith("mergeStage")) {
        continue;
      }
      for (DataPipeRegion region : stage.inputs.values()) {
        totalInputs += region.fileCount();
      }

      if (totalInputs > this.maximumFileInputs) {
        return true;
      }
    }

    return false;
  }

  /**
   * Find stages that need to open a lot of files for reading when running, and
   * add some intermediate merge stages to reduce problems with open files.
   *
   * @param job
   */
  public void addMergeStages(Job job) {
    // look at each stage in the job
    for (StageGroupDescription stage : stages.values()) {
      long totalInputs = 0;

      // add up every file this stage will need to open
      for (DataPipeRegion region : stage.inputs.values()) {
        totalInputs += region.fileCount();
      }

      // if this stage needs to open too many files, it might crash.
      // therefore, we want to add in an extra merge stage for every input
      // that contains a lot of files.
      if (totalInputs > this.maximumFileInputs) {
        Stage s = stage.getStage();
        ArrayList<Connection> relevantConnections = new ArrayList<Connection>();

        // look for connections that point to this stage and store them
        for (Connection connection : job.connections) {
          for (ConnectionEndPoint point : connection.outputs) {
            if (point.getStageName().equals(s.name)) {
              relevantConnections.add(connection);
            }
          }
        }

        // for each stage we marked in the last loop, find ones where all
        // the inputs come from a single stage, and that stage has a large
        // instanceCount (it generates a lot of files), and make a merge stage.
        for (Connection connection : relevantConnections) {
          ConnectionEndPoint endPoint = connection.input;
          String inputStageName = endPoint.getStageName();
          String inputPointName = endPoint.getPointName();
          StageGroupDescription inputStageDesc = stages.get(inputStageName);

          // if there's no description, that means we just added it
          if (inputStageDesc == null
                  || inputStageName.endsWith("mergeStage")) {
            continue;
          }
          if (inputStageDesc.instanceCount > 1) {
            job.addMergeStage(inputStageName, inputPointName, -1);
          }
        }
      }
    }
  }

  private static class EndPointName implements Comparable<EndPointName> {

    public String stageName;
    public String pointName;
    public ConnectionPointType type;

    public EndPointName(String stageName, String pointName, ConnectionPointType type) {
      this.stageName = stageName;
      this.pointName = pointName;
      this.type = type;

    }

    @Override
    public int compareTo(EndPointName other) {
      int result = stageName.compareTo(other.stageName);
      if (result == 0) {
        result = pointName.compareTo(other.pointName);
      }
      if (result == 0) {
        result = type.compareTo(other.type);
      }
      return result;
    }

    @Override
    public int hashCode() {
      return stageName.hashCode() + 7 * pointName.hashCode() + 15 * type.toString().hashCode();
    }

    @Override
    public String toString() {
      return String.format("%s %s %s", stageName, pointName, type.toString());
    }
  }

  /**
   * In the parameter file, each stage has a connections section that describes
   * a set of connection endpoints for the stage (inputs and outputs). This
   * method verifies that all of those endpoints are connected to valid
   * connections, defined under the connections tag in the job. If the method
   * finds an dangling (unconnected) endpoint, an error message is added to the
   * ErrorStore.
   */
  public void findDanglingEndpoints(final Job job) {
    TreeSet<EndPointName> endPointNames = new TreeSet();

    // First, make a list of all endpoints referenced in all stages.
    for (Stage stage : job.stages.values()) {
      // find the corresponding description object
      // StageGroupDescription description = stages.get(stage.name);

      // add all connection points to the set
      for (StageConnectionPoint point : stage.connections.values()) {
        EndPointName ep = new EndPointName(stage.name, point.getExternalName(), point.getType());
        endPointNames.add(ep);
      }
    }

    // Now we have a list of referenced names.  We now remove every endpoint that
    // is referenced in the connections section.
    for (ConnectionDescription connection : connections.values()) {
      EndPointName inputEP = new EndPointName(connection.input.stage.getName(),
              connection.input.stagePoint.getExternalName(),
              connection.input.stagePoint.getType());
      endPointNames.remove(inputEP);

      for (EndPointDescription output : connection.outputs) {
        EndPointName ep = new EndPointName(output.stage.getName(),
                output.stagePoint.getExternalName(),
                output.stagePoint.getType());
        endPointNames.remove(ep);
      }
    }


    for (EndPointName ep : endPointNames) {
      store.addError(ep.stageName,
              ep.stageName + ": No connection references the " + ep.type
              + " with the name '" + ep.pointName + "'.");
    }
  }

  private boolean constructAndVerify() {
    clear();

    // first, we make stage group descriptions, getting ready to add connections in
    for (Stage stage : job.stages.values()) {
      stages.put(stage.name, new StageGroupDescription(stage));
    }

    // verify each stage in the job to make sure that
    // the connections between individual steps are typesafe, that
    // step classes exist, etc.
    Verification.verify(job, store);
    if (store.getErrors().size() > 0) {
      return false;
    }

    // build data about connections between stages, while verifying
    // type safety between stage connections.
    buildConnections(job);
    if (store.getErrors().size() > 0) {
      return false;
    }

    findDanglingEndpoints(job);
    if (store.getErrors().size() > 0) {
      return false;
    }

    generateDependencies();
    if (store.getErrors().size() > 0) {
      return false;
    }

    determineStageOrder();
    if (store.getErrors().size() > 0) {
      return false;
    }

    countStages();
    if (store.getErrors().size() > 0) {
      return false;
    }

    createPipeObjects();
    return true;
  }

  private void generateDependencies() {
    // generate a list of stages
    for (StageGroupDescription stage : stages.values()) {
      stageChildren.put(stage.getName(), new HashSet());
      stageParents.put(stage.getName(), new HashSet());
    }

    for (ConnectionDescription connection : connections.values()) {
      for (EndPointDescription output : connection.outputs) {
        stageChildren.get(connection.input.getStageName()).add(output.getStageName());
        stageParents.get(output.getStageName()).add(connection.input.getStageName());
      }
    }
  }

  private void determineStageOrder() {
    ArrayList<String> result = new ArrayList();
    HashSet<String> used = new HashSet();
    HashSet<String> batch = new HashSet();

    for (String stageName : stageParents.keySet()) {
      if (stageParents.get(stageName).isEmpty()) {
        batch.add(stageName);
      }
    }

    while (batch.size() > 0) {
      HashSet<String> nextBatch = new HashSet();

      for (String stageName : batch) {
        result.add(stageName);
        used.add(stageName);

        HashSet<String> children = stageChildren.get(stageName);

        // adding this stage to the list may have unblocked a child
        for (String child : children) {
          HashSet<String> childParents = stageParents.get(child);

          if (!used.contains(child) && used.containsAll(childParents)) {
            nextBatch.add(child);
          }
        }
      }

      batch = nextBatch;
      nextBatch = new HashSet();
    }

    assert result.size() == stages.size();
    stageOrder = result;
  }

  private static class EndPointDescription {

    public EndPointDescription(ConnectionDescription connection,
            StageGroupDescription stage,
            ConnectionEndPoint connectionPoint,
            StageConnectionPoint stagePoint) {
      this.connectionPoint = connectionPoint;
      this.stagePoint = stagePoint;
      this.stage = stage;
      this.connection = connection;
    }

    public String getStageName() {
      return stage.getName();
    }

    public StageConnectionPoint getStagePoint() {
      return stagePoint;
    }

    public ConnectionEndPoint getConnectionPoint() {
      return connectionPoint;
    }

    public ConnectionDescription getConnection() {
      return connection;
    }
    public StageGroupDescription stage;
    public StageConnectionPoint stagePoint;
    public ConnectionEndPoint connectionPoint;
    public ConnectionDescription connection;
  }

  private class ConnectionDescription {

    public Connection connection;
    public EndPointDescription input;
    public ArrayList<EndPointDescription> outputs;
    public DataPipe pipe;

    public ConnectionDescription(Connection connection) {
      this.connection = connection;
      this.outputs = new ArrayList();
    }

    public boolean isHashed() {
      return connection.hash != null;
    }

    public int getOutputCount() {
      int result = 1;

      if (isHashed()) {
        String globalHashCount = job.properties.get("hashCount");

        if (connection.getHashCount() > 0) {
          result = connection.getHashCount();

        } else if (globalHashCount != null && Utility.isInteger(globalHashCount)) {
          result = Integer.parseInt(globalHashCount);

        } else {
          result = defaultHashCount;

        }

      } else {
        result = getInputCount();

      }

      return result;
    }

    public int getInputCount() {
      return input.stage.instanceCount;
    }

    public String getName() {
      return connection.getName();
    }

    public String[] getOrder() {
      return connection.getOrder();
    }

    public String[] getHash() {
      return connection.getHash();
    }

    private String getClassName() {
      return connection.getClassName();
    }

    public CompressionType getCompression() {
      return connection.getCompression();
    }

    public DataPipe getPipe() {
      return pipe;
    }

    public void setPipe(DataPipe pipe) {
      this.pipe = pipe;
    }

    @Override
    public String toString() {
      return String.format("%s %s", getClassName(), getName());
    }
  }

  private EndPointDescription createEndPoint(ConnectionDescription connection,
          ConnectionEndPoint endPoint) {
    StageGroupDescription stageDescription = stages.get(endPoint.getStageName());

    if (stageDescription == null) {
      store.addError(endPoint.getStageName(),
              "The stage '" + endPoint.getStageName() + "' was not found.");
    } else {
      Stage stage = stageDescription.getStage();
      StageConnectionPoint point = stage.getConnection(endPoint.getPointName());

      if (point == null) {
        store.addError(endPoint.getStageName(), "The endpoint '" + endPoint.getPointName() + "' wasn't found in this stage, "
                + "even though there is a connection to it.");
      } else if (!ConnectionPointType.connectable(endPoint.getType(), point.getType())) {
        store.addError(endPoint.getStageName(),
                "The endpoint '" + endPoint.getPointName() + "' is in this stage, but it's going the wrong direction.");
      } else if (!point.getClassName().equals(connection.connection.getClassName())) {
        store.addError(endPoint.getStageName(), "In " + endPoint.getStageName() + ": This " + point.getType() + " has a different class name '" + point.getClassName()
                + " than the connection that connects to it: " + connection.connection.getClassName() + ".");
      } else if (!Arrays.equals(point.getOrder(), connection.connection.getOrder())) {
        store.addError(endPoint.getStageName(), "In " + endPoint.getStageName() + ": This " + point.getType() + " has a different order " + Arrays.toString(point.getOrder())
                + " than the connection that connects to it: " + Arrays.toString(
                connection.connection.getOrder()));
      } else {
        return new EndPointDescription(connection, stageDescription, endPoint, point);
      }
    }

    return null;
  }

  private ArrayList<EndPointDescription> createEndPoints(ConnectionDescription connection,
          ArrayList<ConnectionEndPoint> endPoints) {
    ArrayList<EndPointDescription> results = new ArrayList<EndPointDescription>();

    for (ConnectionEndPoint endPoint : endPoints) {
      EndPointDescription epd = createEndPoint(connection, endPoint);

      if (epd != null) {
        results.add(epd);
      }
    }

    return results;
  }

  /**
   * Creates ConnectionDescription objects for each connection listed in the Job
   * object. The ConnectionDescription objects combine information from the
   * StageConnectionPoints (in the Stages) and the ConnectionEndPoints (in the
   * Job.Connection objects) to make them easier to access. In the process of
   * making these objects, this method typechecks all of the connections.
   */
  private void buildConnections(final Job job) {
    for (Connection connection : job.connections) {
      ConnectionDescription description = new ConnectionDescription(connection);

      // verify that the class, order, and hash exist
      Verification.requireClass(connection.getClassName(), store);
      Verification.requireOrder(connection.getClassName(), connection.getOrder(), store);

      if (connection.getHash() != null) {
        Verification.requireOrder(connection.getClassName(),
                connection.getHash(),
                store);
      }
      description.input = createEndPoint(description, connection.input);
      description.outputs = createEndPoints(description, connection.outputs);
      connections.put(connection.getName(), description);
    }
  }

  /**
   * This method computes the number of copies of each stage to run. The
   * execution count of stage depends on its inputs. If the input for a stage is
   * hashed 200 ways, for instance, then there will need to be 200 copies of the
   * stage.
   */
  private void countStages() {
    HashMap<String, HashSet<EndPointDescription>> stageInputs = new HashMap();
    HashMap<String, HashSet<EndPointDescription>> stageOutputs = new HashMap();

    for (String stageName : stages.keySet()) {
      stageInputs.put(stageName, new HashSet());
      stageOutputs.put(stageName, new HashSet());
    }

    for (ConnectionDescription connection : connections.values()) {
      stageOutputs.get(connection.input.stage.getName()).add(connection.input);
      for (EndPointDescription endPoint : connection.outputs) {
        stageInputs.get(endPoint.stage.getName()).add(endPoint);
      }
    }

    for (String stageName : stageOrder) {
      StageGroupDescription stage = stages.get(stageName);

      // if stage has no inputs, then we store 1 in stageCounts
      if (stageInputs.get(stageName).isEmpty()) {
        stage.instanceCount = 1;
      } else {
        // find out what the assignment is for this connection.
        int instanceCount = 1;
        boolean unknown = true;

        HashSet<EndPointDescription> inputs = stageInputs.get(stageName);

        for (EndPointDescription description : inputs) {
          ConnectionEndPoint point = description.getConnectionPoint();
          ConnectionAssignmentType assignment = point.getAssignment();

          switch (assignment) {
            case One:
              store.addError(point.getStageName(),
                      "The 'one' mode is not currently supported.");
              break;

            case Each:
              int inputCount = description.connection.getOutputCount();

              if (unknown) {
                instanceCount = inputCount;
                unknown = false;
              } else if (!unknown && instanceCount != inputCount) {
                store.addError(point.getStageName(), "The number of stage instances for '"
                        + stageName + "' is ambiguous (" + inputCount
                        + " or " + instanceCount + ")");
              }
              break;

            case Combined:
              // this doesn't affect the number of stages.
              break;
          }
        }

        if (unknown) {
          instanceCount = 1;
        }
        stage.instanceCount = instanceCount;
      }
    }
  }

  /**
   * Creates the directory structures to hold the files for this job.
   */
  private void createPipeObjects() {
    // Now, we need to create the connections
    for (ConnectionDescription connection : connections.values()) {
      // Make the parent directory
      String directoryName = temporaryStorage + File.separator + connection.getName();
      new File(directoryName).mkdir();

      DataPipe pipe = new DataPipe(directoryName,
              connection.getName(),
              connection.getClassName(),
              connection.getOrder(),
              connection.getHash(),
              connection.getInputCount(),
              connection.getOutputCount(),
              connection.getCompression());

      int startIndex = 0;
      connection.setPipe(pipe);

      StageGroupDescription inputDescription = stages.get(connection.input.getStageName());
      inputDescription.outputs.put(connection.input.getStagePoint().getInternalName(),
              new DataPipeRegion(pipe,
              startIndex,
              startIndex + inputDescription.getInstanceCount(),
              ConnectionPointType.Input,
              connection.input.connectionPoint.getAssignment()));
      startIndex += inputDescription.getInstanceCount();

      for (EndPointDescription output : connection.outputs) {
        StageGroupDescription description = stages.get(output.getStageName());
        description.inputs.put(output.getStagePoint().getInternalName(),
                new DataPipeRegion(pipe,
                0,
                connection.getOutputCount(),
                ConnectionPointType.Output,
                output.connectionPoint.getAssignment()));
      }

      pipes.add(pipe);
    }
  }

  public static class JobExecutionStatus {
    // these are the names of all stages that have completed

    HashMap<String, StageExecutionStatus> completedStages = new HashMap<String, StageExecutionStatus>();
    // named of all stages that have been launched (contains all completed stages too)
    HashSet<String> launchedStages = new HashSet<String>();
    // all stages that are currently running.  Includes a Future object that can be used
    // to wait for the stage to complete (and to get exceptions thrown by the stage)
    HashMap<String, StageExecutionStatus> runningStages = new HashMap<String, StageExecutionStatus>();
    // names of connections that are complete, meaning that all their inputs have been created
    HashSet<String> completedConnections = new HashSet<String>();
    // map from connection names to the names of stages that provide inputs to the connection
    HashMap<String, HashSet<String>> connectionDependencies = new HashMap<String, HashSet<String>>();
    // reference to the parent class.
    HashMap<String, StageGroupDescription> stages;
    // reference to the parent class.
    String temporaryStorage;
    StageExecutor executor;
    Date startDate;
    String masterURL;
    String fullcmd = null;
    // (irmarc)
    // We're going to do something a little nutso here - accept user input!
    // Wrap System.in and poll for characters. If we see a space+<enter>, print the
    // master URL out, b/c I'm tired of not being able to see it.
    BufferedReader poller = new BufferedReader(new InputStreamReader(System.in));

    public JobExecutionStatus(HashMap<String, StageGroupDescription> stages,
            String temporaryStorage, StageExecutor executor, String masterURL, String cmd) {
      this.stages = stages;
      this.temporaryStorage = temporaryStorage;
      this.executor = executor;
      this.startDate = new Date();
      this.masterURL = masterURL;
      this.fullcmd = cmd;

      for (StageGroupDescription description : stages.values()) {
        // build a list of dependencies from pipe inputs to stage names
        for (DataPipeRegion region : description.outputs.values()) {
          String pipeName = region.pipe.pipeName;

          if (!connectionDependencies.containsKey(pipeName)) {
            connectionDependencies.put(
                    pipeName, new HashSet<String>());
          }
          connectionDependencies.get(pipeName).add(description.getName());
        }

        description.setMasterURL(masterURL);
      }
    }

    class BlockedExecutionStatus implements StageExecutionStatus {

      String name;
      int instances;

      BlockedExecutionStatus(String name, int instances) {
        this.name = name;
        this.instances = instances;
      }

      @Override
      public String getName() {
        return name;
      }

      @Override
      public int getBlockedInstances() {
        return instances;
      }

      @Override
      public int getQueuedInstances() {
        return 0;
      }

      @Override
      public int getRunningInstances() {
        return 0;
      }

      @Override
      public int getCompletedInstances() {
        return 0;
      }

      @Override
      public boolean isDone() {
        return false;
      }

      @Override
      public synchronized List<Double> getRunTimes() {
        ArrayList<Double> times = new ArrayList();
        // actually, don't do anything here. Nothing's running.
        return times;
      }

      @Override
      public List<Exception> getExceptions() {
        return Collections.EMPTY_LIST;
      }
    }

    public synchronized boolean isComplete() {
      return stages.size() == completedStages.size();
    }

    public synchronized Map<String, StageExecutionStatus> getStageStatus() {
      Map<String, StageExecutionStatus> result = new TreeMap();

      for (String stageName : stages.keySet()) {
        int instanceCount = stages.get(stageName).getInstanceCount();
        if (completedStages.containsKey(stageName)) {
          result.put(stageName, completedStages.get(stageName));
        } else if (runningStages.containsKey(stageName)) {
          result.put(stageName, runningStages.get(stageName));
        } else {
          result.put(stageName, new BlockedExecutionStatus(stageName, instanceCount));
        }
      }

      return result;
    }

    /**
     * Returns the start date for this job.
     */
    public Date getStartDate() {
      return startDate;
    }

    /**
     * Returns the total amount of free memory in this JVM.
     */
    public long getFreeMemory() {
      return Runtime.getRuntime().freeMemory();
    }

    /**
     * Returns the maximum amount of memory that can be used by this Java
     * virtual machine.
     */
    public long getMaxMemory() {
      return Runtime.getRuntime().maxMemory();
    }

    private void poll() {
      try {
        if (poller.ready()) {
          char c = (char) poller.read();
          if (c == ' ') {
            System.out.println("Web url: " + masterURL);
          }
        }
      } catch (IOException ioe) {
        // this is not that important - don't complain
      }
    }

    public void run() throws InterruptedException, ExecutionException {

      // while there are incomplete stages, choose one to execute
      while (launchedStages.size() < stages.size()) {
        // look for stages where all of their inputs are complete
        StageGroupDescription description = findRunnableStage(stages.values(), launchedStages,
                completedConnections);

        // didn't find any runnable stages, so we need to check to
        // see if any other stages have finished recently that might have
        // generated interesting input for pending stages.
        while (description == null) {
          // wait for at least one stage to complete
          waitForStages(runningStages, completedStages);
          updateCompletedConnections(completedStages, completedConnections,
                  connectionDependencies);

          // now, try again to find a runnable stage
          description = findRunnableStage(stages.values(), launchedStages,
                  completedConnections);
          poll();
        }

        StageExecutionStatus result = executor.execute(description, temporaryStorage);

        synchronized (this) {
          launchedStages.add(description.stage.name);
          runningStages.put(description.stage.name, result);
        }
        poll();
      }

      // wait for everything to complete
      while (runningStages.size() > 0) {
        waitForStages(runningStages, completedStages);
        poll();
      }
    }

    /**
     * Finds a stage that is ready to run by checking stage dependencies.
     *
     * @param descriptions
     * @param launchedJobs
     * @param completedConnections
     */
    private synchronized StageGroupDescription findRunnableStage(
            Collection<StageGroupDescription> descriptions,
            HashSet<String> launchedJobs,
            HashSet<String> completedConnections) {
      // for each job we might want to launch
      for (StageGroupDescription description : descriptions) {
        // if it has already been launched, we don't need to do it again
        if (launchedJobs.contains(description.getName())) {
          continue;            // are the inputs to this stage ready?
        }
        boolean allComplete = true;

        for (DataPipeRegion region : description.inputs.values()) {
          // if this input is incomplete, we can't run this stage yet
          if (!completedConnections.contains(region.pipe.pipeName)) {
            allComplete = false;
            break;
          }
        }

        if (allComplete) {
          return description;
        }
      }

      // there are no stages ready to run
      return null;
    }

    /**
     * Polls all the running stages to see if they've completed. When one
     * completes, it is added to completedStages and the method returns.
     *
     * @param runningStages
     * @param completedStages
     * @throws java.lang.InterruptedException
     * @throws java.util.concurrent.ExecutionException
     */
    private void waitForStages(
            HashMap<String, StageExecutionStatus> runningStages,
            final HashMap<String, StageExecutionStatus> completedStages)
            throws InterruptedException, ExecutionException {
      long delay = 1;

      while (runningStages.size() > 0) {
        synchronized (this) {
          for (String name : runningStages.keySet()) {
            StageExecutionStatus status = runningStages.get(name);
            if (status.isDone()) {
              // force the exception to throw
              List<Exception> exceptions = status.getExceptions();

              System.err.format("Stage %s completed with %d errors.\n", name, exceptions.size());
              
              if (exceptions.size() > 0) {
                for(Exception e : exceptions){
                  System.err.println(e.toString());
                }
                throw new ExecutionException("Stage threw an exception: ", exceptions.get(0));
              }
              completedStages.put(name, status);
              runningStages.remove(name);
              return;
            }
          }
        }

        // check at least once per 10 seconds, but poll faster at first
        delay = Math.min(delay * 2, 10000);
        poll();
        Thread.sleep(delay);
      }
    }

    /**
     * Reads through all completed stages, trying to find any connections that
     * have been satisfied. By satisfied, we mean that all the input has been
     * generated for a particular connection. Once a connection is satisfied,
     * stages that require that read input from that connection can start
     * running.
     *
     * @param completedStages
     * @param completedConnections
     * @param connectionDependencies
     */
    private synchronized void updateCompletedConnections(
            final HashMap<String, StageExecutionStatus> completedStages,
            final HashSet<String> completedConnections,
            final HashMap<String, HashSet<String>> connectionDependencies) {
      // Loop over all connections:
      for (String pipeName : connectionDependencies.keySet()) {
        // These are all the stages that need to be completed before
        // the connection pipeName is satisfied.
        HashSet<String> pipeInputStages = connectionDependencies.get(pipeName);

        if (completedStages.keySet().containsAll(pipeInputStages)) {
          completedConnections.add(pipeName);
        }
      }
    }
  }

  public void runWithServer(StageExecutor executor, Server server, String command) throws ExecutionException, InterruptedException, UnknownHostException {
    // FIXME: all of this needs to be refactored.
    InetAddress address = java.net.InetAddress.getLocalHost();
    int port = server.getConnectors()[0].getPort();
    String masterURL = String.format("http://%s:%d", address.getHostAddress(), port);
    JobExecutionStatus status = new JobExecutionStatus(stages, temporaryStorage, executor, masterURL, command);
    MasterWebHandler handler = new MasterWebHandler(status);
    server.addHandler(handler);
    status.run();
    handler.waitForFinalPage();
    server.removeHandler(handler);
  }

  public void runWithoutServer(StageExecutor executor) throws ExecutionException, InterruptedException {
    JobExecutionStatus status = new JobExecutionStatus(stages, temporaryStorage, executor, null, null);
    status.run();
  }

  public static boolean runLocally(Job job, ErrorStore store, Parameters p) throws IOException,
          InterruptedException, ExecutionException, Exception {
    // Extraction from parameters can go here now
    String tempPath = p.get("galagoJobDir", "");
    File tempFolder = FileUtility.createTemporaryDirectory(tempPath);

    File stdout = new File(tempFolder + File.separator + "stdout");
    File stderr = new File(tempFolder + File.separator + "stderr");
    if (stdout.isDirectory()) {
      Utility.deleteDirectory(stdout);
    }
    if (stderr.isDirectory()) {
      Utility.deleteDirectory(stderr);
    }

    String mode = p.get("mode", "local");

    int port = (int) p.get("port", 0);
    if (port == 0) {
      port = Utility.getFreePort();
    } else {
      if (!Utility.isFreePort(port)) {
        throw new IOException("Tried to bind to port " + port + " which is in use.");
      }
    }

    String[] params = new String[]{};

    String command;
    if (p.containsKey("command")) {
      command = p.getString("command");
    } else {
      command = null;
    }

    StageExecutor executor = StageExecutorFactory.newInstance(mode, params);
    System.err.printf("Created executor: %s\n", executor.toString());
    JobExecutor jobExecutor = new JobExecutor(job, tempFolder.getAbsolutePath(), store);
    jobExecutor.prepare();

    if (store.hasStatements()) {
      return false;
    }

    if (p.get("server", false)) {
      Server server = new Server(port);
      server.start();
      System.err.println("Status: http://localhost:" + port);
      try {
        jobExecutor.runWithServer(executor, server, command);
      } finally {
        server.stop();
        executor.shutdown();
      }
    } else {
      System.out.println("Running without server!\nUse --server=true to enable web-based status page.");
      try {
        jobExecutor.runWithoutServer(executor);
      } finally {
        executor.shutdown();
      }
    }

    if (p.get("deleteJobDir", true) && !store.hasStatements()) {
      Utility.deleteDirectory(tempFolder);
    }

    return !store.hasStatements();
  }
}
