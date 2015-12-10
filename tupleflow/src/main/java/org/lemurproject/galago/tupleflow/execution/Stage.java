// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow.execution;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import org.lemurproject.galago.tupleflow.CompressionType;
import org.lemurproject.galago.tupleflow.Order;

/**
 * This is a stage description for a TupleFlow job.
 *
 * @author trevor
 */
public class Stage implements Serializable, Cloneable {

  private static final long serialVersionUID = -4876017486078922298L;
  public HashMap<String, StageConnectionPoint> connections = new HashMap<String, StageConnectionPoint>();
  public ArrayList<StepInformation> steps = new ArrayList<StepInformation>();
  public String name;

  // Why is this here?
  public Stage() {
  }

  public Stage(String name) {
    this.name = name;
  }

  public ArrayList<StepInformation> getSteps() {
    return steps;
  }

  public boolean containsInput(String name) {
    return connections.containsKey(name)
            && connections.get(name).type == ConnectionPointType.Input;
  }

  public boolean containsOutput(String name) {
    return connections.containsKey(name)
            && connections.get(name).type == ConnectionPointType.Output;
  }

  public HashMap<String, StageConnectionPoint> getConnections() {
    return connections;
  }

  public StageConnectionPoint getConnection(String pointName) {
    return connections.get(pointName);
  }

  @Override
  public Stage clone() {
    Stage result = null;

    try {
      result = (Stage) super.clone();
      result.name = name;
      result.steps = new ArrayList<>(steps);
      result.connections = new HashMap<>(connections);
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException("Didn't expect clone to not be supported in superclass", e);
    }

    return result;
  }

  public Stage addInput(String pipeName, Order pipeOrder) {
    add(new StageConnectionPoint(ConnectionPointType.Input,
            pipeName, pipeOrder));
    return this;
  }

  public Stage addOutput(String pipeName, Order pipeOrder) {
    add(new StageConnectionPoint(ConnectionPointType.Output,
            pipeName, pipeOrder));
    return this;
  }

  public Stage addOutput(String pipeName, Order pipeOrder, CompressionType compression) {
    add(new StageConnectionPoint(ConnectionPointType.Output,
            pipeName, pipeOrder, compression));
    return this;
  }  
  
  public Stage add(StageConnectionPoint point) {
    connections.put(point.getExternalName(), point);
    return this;
  }

  public Stage add(StepInformation step) {
    steps.add(step);
    return this;
  }

  public Stage remove(String connectionName) {
    connections.remove(connectionName);
    return this;
  }
}
