// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow.execution;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * A Job object specifies a TupleFlow execution: the objects used, their
 * parameters, and how they communicate. A Job can be specified in XML (and
 * parsed with JobConstructor), or in code.
 *
 * Usage: users create these, then run them using the JobExecutor
 *
 * Possible Improvements: - the ability to branch on some condition - arbitrary
 * looping
 *
 * @author trevor
 */
public class Job implements Serializable {

  public TreeMap<String, Stage> stages = new TreeMap<String, Stage>();
  public ArrayList<Connection> connections = new ArrayList<Connection>();
  public HashMap<String, ConnectionEndPoint> exports = new HashMap<String, ConnectionEndPoint>();
  public HashMap<String, String> properties = new HashMap<String, String>();

  private String orderString(String[] order) {
    StringBuilder builder = new StringBuilder();
    for (String o : order) {
      if (builder.length() > 0) {
        builder.append(" ");
      }
      builder.append(o);
    }
    return builder.toString();
  }

  /**
   * Sometimes its convenient to specify a group of stages as its own job, with
   * connections that flow between stages specified in the job. The add method
   * allows you to add another job to this job. To avoid name conflicts, all
   * stages in the job are renamed from <tt>stageName</tt> to
   * <tt>jobName.stageName</tt>.
   */
  public Job add(String jobName, Job group) {
    assert this != group;

    for (Stage s : group.stages.values()) {
      Stage copy = s.clone();
      copy.name = jobName + "." + s.name;
      add(copy);
    }

    for (Connection c : group.connections) {
      Connection copy = c.clone();
      copy.input.setStageName(jobName + "." + copy.input.getStageName());

      for (ConnectionEndPoint output : copy.outputs) {
        output.setStageName(jobName + "." + output.getStageName());
      }

      connections.add(copy);
    }
    return this;
  }

  /**
   * Adds a stage to the current job.
   */
  public Job add(Stage s) {
    stages.put(s.name, s);
    return this;
  }

  Map<String, Stage> findStagesWithPrefix(String prefix) {
    Map<String, Stage> result;
    if (stages.containsKey(prefix)) {
      result = new HashMap<String, Stage>();
      result.put(prefix, stages.get(prefix));
    } else {
      result = stages.subMap(prefix + '.', prefix + ('.' + 1));
    }

    return result;
  }

  /**
   * Add a merge stage to this job that merges the output of stageName called
   * pointName.
   *
   * @param stageName The stage that contains the output that needs merging.
   * @param pointName The output point that needs merging in the stage
   * stageName.
   * @param factor What the reduction factor is for each merger.
   */
  public Job addMergeStage(String stageName, String pointName, int factor) {
    // find the stage and the point, initialize class/order information
    Stage inputStage = this.stages.get(stageName);

    StageConnectionPoint inputPoint = inputStage.getConnection(pointName);

    String className = inputPoint.getClassName();
    String[] typeOrder = inputPoint.getOrder();
    String mergedStageName = stageName + "-" + pointName + "-mergeStage";
    String mergedPointName = pointName + "-merged";

    // if this merge stage has already been added, don't add it again
    if (this.stages.containsKey(mergedStageName)) {
      return this;        // create the stage itself
    }
    Stage s = new Stage(mergedStageName);
    s.add(new StageConnectionPoint(ConnectionPointType.Input,
            pointName,
            className,
            typeOrder));
    s.add(new StageConnectionPoint(ConnectionPointType.Output,
            pointName + "-merged",
            className,
            typeOrder));

    s.add(new InputStep(pointName));
    s.add(new OutputStep(mergedPointName));
    this.add(s);

    String[] hash = null;
    int hashCount = factor;

    // run through the connections list, find all inputs for the previous data
    for (Connection connection : this.connections) {
      if (connection.input.getStageName().equals(stageName)
              && connection.input.getPointName().equals(pointName)) {
        if (hash != null && connection.hash != null
                && !Arrays.equals(hash, connection.hash)) {
          continue;
        }
        if (connection.hash != null) {
          hash = connection.hash;
          connection.hash = null;
        }

        connection.input.setStageName(mergedStageName);
        connection.input.setPointName(mergedPointName);
      }
    }

    // now, add a connection between the producing stage and the merge stage
    this.connect(new StagePoint(stageName, pointName),
            new StagePoint(mergedStageName, pointName),
            ConnectionAssignmentType.Each,
            hash,
            hashCount);
    return this;
  }

  public static class StagePoint implements Comparable<StagePoint> {

    String stageName;
    String pointName;
    private StageConnectionPoint point;

    public StagePoint(String stageName, String pointName) {
      this(stageName, pointName, null);
    }

    public StagePoint(String stageName, String pointName, StageConnectionPoint point) {
      this.stageName = stageName;
      this.pointName = pointName;
      this.point = point;
    }

    public boolean equals(StagePoint other) {
      return stageName.equals(other.stageName) && pointName.equals(other.pointName);
    }

    public int compareTo(StagePoint other) {
      int result = stageName.compareTo(other.stageName);
      if (result != 0) {
        return result;
      }
      return pointName.compareTo(other.pointName);
    }

    @Override
    public int hashCode() {
      return stageName.hashCode() + pointName.hashCode() * 3;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }

      final StagePoint other = (StagePoint) obj;
      if (this.stageName != other.stageName && (this.stageName == null || !this.stageName.equals(other.stageName))) {
        return false;
      }
      if (this.pointName != other.pointName && (this.pointName == null || !this.pointName.equals(other.pointName))) {
        return false;
      }

      return true;
    }

    public StageConnectionPoint getPoint() {
      return point;
    }

    public void setPoint(StageConnectionPoint point) {
      this.point = point;
    }

    @Override
    public String toString() {
      return String.format("%s:%s", stageName, pointName);
    }
  }

  HashSet<StagePoint> extractStagePoints(Collection<Stage> allStages, ConnectionPointType type) {
    HashSet<StagePoint> result = new HashSet<StagePoint>();

    for (Stage s : allStages) {
      for (Map.Entry<String, StageConnectionPoint> e : s.connections.entrySet()) {
        String pointName = e.getKey();
        String stageName = s.name;

        if (e.getValue().type == type) {
          result.add(new StagePoint(stageName, pointName, e.getValue()));
        }
      }
    }

    return result;
  }

  /**
   * Connects outputs from stage sourceName to inputs from stage
   * destinationName.
   *
   * Connect can make connections between any stage with the name sourceName, or
   * that starts with sourceName (same goes for destinationName), which makes
   * this particularly useful for making connections between sub-jobs.
   */
  public Job connect(String sourceName, String destinationName, ConnectionAssignmentType assignment) {
    return connect(sourceName, destinationName, assignment, null, -1);
  }

  public Job connect(String sourceName, String destinationName, ConnectionAssignmentType assignment, int hashCount) {
    return connect(sourceName, destinationName, assignment, null, hashCount);
  }

  public Job connect(String sourceName, String destinationName, ConnectionAssignmentType assignment, String[] hashType) {
    return connect(sourceName, destinationName, assignment, hashType, -1);
  }

  public Job connect(String sourceName, String destinationName, ConnectionAssignmentType assignment, String[] hashType, int hashCount) {
    // scan the stages, looking for sources
    Map<String, Stage> sources = findStagesWithPrefix(sourceName);
    Map<String, Stage> destinations = findStagesWithPrefix(destinationName);

    // find all inputs and outputs in these stages
    HashSet<StagePoint> outputs = extractStagePoints(sources.values(),
            ConnectionPointType.Output);
    HashSet<StagePoint> inputs = extractStagePoints(destinations.values(),
            ConnectionPointType.Input);

    // remove any inputs that are already referenced in job connections
    for (Connection c : connections) {
      for (ConnectionEndPoint p : c.outputs) {
        StagePoint point = new StagePoint(p.getStageName(), p.getPointName());
        inputs.remove(point);
      }
    }

    // now we have a list of all dangling inputs.  try to match them with outputs
    HashMap<String, ArrayList<StagePoint>> outputMap = new HashMap<String, ArrayList<StagePoint>>();
    for (StagePoint point : outputs) {
      if (!outputMap.containsKey(point.pointName)) {
        outputMap.put(point.pointName, new ArrayList<StagePoint>());
      }
      outputMap.get(point.pointName).add(point);
    }

    for (StagePoint destinationPoint : inputs) {
      if (outputMap.containsKey(destinationPoint.pointName)) {
        assert outputMap.get(destinationPoint.pointName).size() == 1;
        StagePoint sourcePoint = outputMap.get(destinationPoint.pointName).get(0);

        // this is only really applicable when assignment is 'Each'
        //  - it is ignored in other cases
        String[] connectionHashType = null;
        if (hashType != null) {
          connectionHashType = hashType;
        } else {
          connectionHashType = sourcePoint.point.getOrder();
        }

        if (assignment == ConnectionAssignmentType.Combined) {
          connectionHashType = null;
        }

        connect(sourcePoint, destinationPoint, assignment, connectionHashType, hashCount);
      }
    }
    return this;
  }

  public Job connect(StagePoint source, StagePoint destination, ConnectionAssignmentType assignment, String[] hashType, int hashCount) {
    // first, try to find a usable connection
    Connection connection = null;

    if (source.getPoint() == null) {
      Stage sourceStage = stages.get(source.stageName);
      StageConnectionPoint sourcePoint = sourceStage.getConnection(source.pointName);
      source.setPoint(sourcePoint);
    }

    for (Connection c : connections) {
      ConnectionEndPoint connectionInput = c.input;
      if (connectionInput.getPointName().equals(source.pointName)
              && connectionInput.getStageName().equals(source.stageName)) {
        connection = c;
        break;
      }
    }

    // couldn't find a connection that has this input, so we'll make one
    if (connection == null) {
      connection = new Connection(source.getPoint(), hashType, hashCount);
      ConnectionEndPoint input = new ConnectionEndPoint(
              source.stageName,
              source.pointName,
              ConnectionPointType.Input);
      connection.input = input;
      connections.add(connection);
    }

    ConnectionEndPoint output = new ConnectionEndPoint(
            destination.stageName,
            destination.pointName,
            assignment,
            ConnectionPointType.Output);
    connection.outputs.add(output);
    return this;
  }

  /**
   * Returns this job as a graph in the DOT language. This DOT text can be used
   * with GraphViz (http://www.graphviz.org) to display a picture of the job.
   */
  public String toDotString() {
    StringBuilder builder = new StringBuilder();
    builder.append("digraph {\n");

    for (Connection connection : connections) {
      for (ConnectionEndPoint output : connection.outputs) {
        String edge = String.format("  %s -> %s [label=\"%s\"];\n",
                connection.input.getStageName(), output.getStageName(), connection.getName());
        builder.append(edge);
      }
    }

    for (Stage stage : stages.values()) {
      builder.append(String.format("  %s;\n", stage.name));
    }

    builder.append("}\n");
    return builder.toString();
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append("<job>\n");

    // Properties block
    for (Map.Entry<String, String> entry : properties.entrySet()) {
      builder.append(String.format("    <property name=\"%s\" value=\"%s\" />\n",
              entry.getKey(), entry.getValue()));
    }
    builder.append("\n");

    // Connections block
    builder.append("    <connections>\n");
    for (Connection connection : connections) {
      if (connection.getHash() != null) {
        String connectionHeader = String.format(
                "        <connection id=\"%s\"           \n"
                + "                    class=\"%s\"        \n"
                + "                    order=\"%s\"        \n"
                + "                    hash=\"%s\"         \n"
                + "                    hashCount=\"%d\">   \n",
                connection.getName(),
                connection.getClassName(),
                orderString(connection.getOrder()),
                orderString(connection.getHash()),
                connection.getHashCount());
        builder.append(connectionHeader);
      } else {
        String connectionHeader = String.format(
                "        <connection id=\"%s\"         \n"
                + "                    class=\"%s\"        \n"
                + "                    order=\"%s\">       \n",
                connection.getName(),
                connection.getClassName(),
                orderString(connection.getOrder()));
        builder.append(connectionHeader);
      }

        String inputEndPointString = String.format(
                "            <input stage=\"%s\"         \n"
                + "                   endpoint=\"%s\" />   \n",
                connection.input.getStageName(),
                connection.input.getPointName());
        builder.append(inputEndPointString);

      for (ConnectionEndPoint point : connection.outputs) {
        String endPointString = String.format(
                "            <output stage=\"%s\"         \n"
                + "                    endpoint=\"%s\"      \n"
                + "                    assignment=\"%s\" /> \n",
                point.getStageName(),
                point.getPointName(),
                point.getAssignment());
        builder.append(endPointString);
      }

      builder.append("        </connection>\n");
    }
    builder.append("    </connections>\n");
    builder.append("\n");

    // Stages block
    builder.append("    <stages>\n");
    for (Stage s : stages.values()) {
      String stageHeader =
              String.format("        <stage id=\"%s\">\n", s.name);
      builder.append(stageHeader);

      builder.append("            <connections>\n");

      for (StageConnectionPoint point : s.connections.values()) {
        String pointString = String.format(
                "                <%s id=\"%s\" as=\"%s\" class=\"%s\" order=\"%s\" />\n",
                point.type, point.externalName, point.internalName, point.getClassName(),
                orderString(point.getOrder()));
        builder.append(pointString);
      }

      builder.append("            </connections>\n");
      printSteps(builder, s.steps, "steps");

      builder.append("        </stage>\n");
    }
    builder.append("    </stages>\n");

    builder.append("</job>\n");
    return builder.toString();
  }

  private void printSteps(final StringBuilder builder, final List<Step> steps, final String tag) {
    builder.append(String.format("            <%s>\n", tag));
    for (Step step : steps) {
      if (step instanceof InputStep) {
        InputStep input = (InputStep) step;
        String line = String.format("                <input id=\"%s\" />\n", input.getId());
        builder.append(line);
      } else if (step instanceof MultiInputStep) {
        MultiInputStep input = (MultiInputStep) step;
        String line = String.format("                <multiinput ids=\"%s\" />\n", Utility.join(input.getIds(), ","));
        builder.append(line);
      } else if (step instanceof OutputStep) {
        OutputStep output = (OutputStep) step;
        String line = String.format("                <output id=\"%s\" />\n", output.getId());
        builder.append(line);
      } else if (step instanceof MultiStep) {
        MultiStep multi = (MultiStep) step;
        builder.append("                <multi>\n");
        for (String name : multi) {
          printSteps(builder, multi.getGroup(name), "group");
        }
        builder.append("                </multi>\n");
      } else if (step.getParameters() == null || step.getParameters().isEmpty()) {
        String stepHeader = String.format("                <step class=\"%s\" />\n", step.getClassName());
        builder.append(stepHeader);
      } else {
        String stepHeader = String.format("                <step class=\"%s\">\n", step.getClassName());
        builder.append(stepHeader);
        String parametersString = step.getParameters().toString();

        // strip out the beginning and end parts
        //int start = parametersString.indexOf("<parameters>") + "<parameters>".length();
        //int end = parametersString.lastIndexOf("</parameters>");
        //parametersString = parametersString.substring(start, end);

        builder.append(parametersString);
        builder.append("                </step>\n");
      }
    }
    builder.append(String.format("                </%s>\n", tag));
  }
}
