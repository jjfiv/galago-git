// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow.execution;

import org.lemurproject.galago.tupleflow.CompressionType;
import org.lemurproject.galago.tupleflow.Order;

import java.io.Serializable;
import java.util.Arrays;

/**
 * Represents an endpoint for a connection within a TupleFlow stage. This is
 * defined with an input or output tag in the connections section of a stage in
 * the XML parameter file.
 *
 * @see org.lemurproject.galago.tupleflow.execution.Job
 * @author trevor
 */
public class StageConnectionPoint implements Serializable {

  public CompressionType compression;
  public ConnectionPointType type;
  public String externalName;
  public String internalName;
  private String className;
  private String[] order;

  public StageConnectionPoint(ConnectionPointType type, String name, Order order) {
    this(type, name, order, CompressionType.UNSPECIFIED);
  }

  public StageConnectionPoint(ConnectionPointType type, String name, Order order, CompressionType compression) {
    this.type = type;
    this.externalName = name;
    this.internalName = name;
    this.className = order.getOrderedClass().getName();
    this.order = order.getOrderSpec();
    this.compression = compression;
  }

  public StageConnectionPoint(ConnectionPointType type, String name, String className, String[] order) {
    this.type = type;
    this.externalName = name;
    this.internalName = name;
    this.className = className;
    this.order = order;
    this.compression = CompressionType.UNSPECIFIED;
  }

  @Override
  public String toString() {
    return String.format("(%s) %s.%s (%s) {%s} c[%s]", type, externalName, internalName, className, Arrays.asList(order), compression);
  }

  public String getExternalName() {
    return externalName;
  }

  public String getInternalName() {
    return internalName;
  }

  public ConnectionPointType getType() {
    return type;
  }

  public CompressionType getCompression() {
    return compression;
  }

  public String getClassName() {
    return className;
  }

  public String[] getOrder() {
    return order;
  }
}
