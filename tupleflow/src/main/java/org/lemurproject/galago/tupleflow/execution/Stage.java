// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow.execution;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import org.lemurproject.galago.tupleflow.Order;

/**
 * This is a stage description for a TupleFlow job.
 * 
 * @author trevor
 */
public class Stage extends Locatable implements Serializable, Cloneable {

  public HashMap<String, StageConnectionPoint> connections = new HashMap<String, StageConnectionPoint>();
  public ArrayList<Step> steps = new ArrayList<Step>();
  public String name;

  public Stage() {
    super(null);
  }

  public Stage(String name) {
    super(null);
    this.name = name;
  }

  public Stage(FileLocation location) {
    super(location);
  }

  public ArrayList<Step> getSteps() {
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
      result.steps = new ArrayList<Step>(steps);
      result.connections = new HashMap<String, StageConnectionPoint>(connections);
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

  public Stage add(StageConnectionPoint point) {
    connections.put(point.getExternalName(), point);
    return this;
  }

  public Stage add(Step step) {
    steps.add(step);
    return this;
  }

  public Stage remove(String connectionName) {
    connections.remove(connectionName);
    return this;
  }
}
