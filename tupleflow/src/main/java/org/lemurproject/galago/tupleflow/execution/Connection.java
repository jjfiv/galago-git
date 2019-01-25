// BSD License (http://lemurproject.org)
package org.lemurproject.galago.tupleflow.execution;

import java.util.ArrayList;
import org.lemurproject.galago.tupleflow.CompressionType;

/**
 * Represents a data connection between two stages in a TupleFlow job.
 *  - each connection can have only one input, 
 *    but multiple possible outputs.
 *
 * @see org.lemurproject.galago.tupleflow.execution.Job
 * @author trevor
 */
public class Connection implements Cloneable {

  String className;
  String connectionName;
  String[] order;
  String[] hash;
  int hashCount;
  CompressionType compression;
  ConnectionEndPoint input;
  public ArrayList<ConnectionEndPoint> outputs = new ArrayList<ConnectionEndPoint>();
  
  public Connection(StageConnectionPoint point, String[] hash, int hashCount) {
    this.connectionName = null;
    this.className = point.getClassName();
    this.order = point.getOrder();
    this.compression = point.getCompression();
    this.hash = hash;
    this.hashCount = hashCount;
  }

  public String getName() {
    if (connectionName == null) {
      return input.getStageName() + "-" + input.getPointName();
    } else {
      return connectionName;
    }
  }

  String getClassName() {
    return className;
  }

  String[] getOrder() {
    return order;
  }

  String[] getHash() {
    return hash;
  }

  int getHashCount() {
    return hashCount;
  }

  CompressionType getCompression(){
    return compression;
  }
  
  @Override
  public Connection clone() {
    try {
      Connection copy = (Connection) super.clone();

      ArrayList<ConnectionEndPoint> outputCopy = new ArrayList<ConnectionEndPoint>();

      for (ConnectionEndPoint point : outputs) {
        outputCopy.add(point.clone());
      }

      copy.className = this.className;
      copy.connectionName = this.connectionName;
      copy.order = this.order;
      copy.hash = this.hash;
      copy.hashCount = this.hashCount;
      copy.compression = this.compression;
      copy.input = input.clone();
      copy.outputs = outputCopy;
      return copy;
    } catch (CloneNotSupportedException ex) {
      throw new RuntimeException("Expected cloning to be supported in superclass", ex);
    }
  }
}
