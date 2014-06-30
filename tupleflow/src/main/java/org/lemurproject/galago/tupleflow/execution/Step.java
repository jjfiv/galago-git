// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow.execution;

import java.io.Serializable;
import org.lemurproject.galago.utility.Parameters;

/**
 *
 * @author trevor
 */
public class Step implements Serializable {

  private String className;
  private Parameters parameters;

  // Don't see why this constructor is here - maybe for Serializable?
  public Step() {
  }

  public Step(Class c) {
    this(c.getName(), Parameters.instance());
  }

  public Step(String className) {
    this(className, Parameters.instance());
  }

  public Step(Class c, Parameters parameters) {
    this(c.getName(), parameters);
  }

  public Step(String className, Parameters parameters) {
    this.className = className;
    this.parameters = parameters;
  }

  public String getLocation() {
    return className;
  }

  public String getClassName() {
    return className;
  }

  public Parameters getParameters() {
    return parameters;
  }

  public boolean isStepClassAvailable() {
    return Verification.isClassAvailable(className);
  }

  @Override
  public String toString() {
    return className;
  }
}
