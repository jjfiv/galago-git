/*
 * ConnectionEndPoint
 * 
 * 19 October 2007 -- tds
 *
 * BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.tupleflow.execution;

import java.io.Serializable;

/**
 * @author trevor
 */
public class ConnectionEndPoint implements Cloneable, Serializable {

  private String stageName;
  private String pointName;
  private ConnectionPointType type;
  private ConnectionAssignmentType assignment;

  public ConnectionEndPoint(String stageName, String pointName, ConnectionAssignmentType assignment, ConnectionPointType type) {
    this.stageName = stageName;
    this.pointName = pointName;
    this.type = type;
    this.assignment = assignment;
  }

  public ConnectionEndPoint(String stageName, String pointName, ConnectionPointType type) {
    this(stageName, pointName, ConnectionAssignmentType.Combined, type);
  }

  public String getStageName() {
    return stageName;
  }

  public void setStageName(String stageName) {
    this.stageName = stageName;
  }

  public String getPointName() {
    return pointName;
  }

  public void setPointName(String pointName) {
    this.pointName = pointName;
  }

  public ConnectionAssignmentType getAssignment() {
    return assignment;
  }

  public ConnectionPointType getType() {
    return type;
  }

  @Override
  public String toString() {
    return String.format("%s:%s %s %s", stageName, pointName, assignment, type.toString());
  }

  @Override
  public ConnectionEndPoint clone() {
    try {
      return (ConnectionEndPoint) super.clone();
    } catch (CloneNotSupportedException e) {
      throw new RuntimeException("Expected superclass to handle cloning", e);
    }
  }
}
