// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow.execution;

import org.lemurproject.galago.utility.Parameters;

import java.io.Serializable;

/**
 *
 * @author trevor
 */
public class StepInformation implements Serializable {

  private static final long serialVersionUID = -2002021272247696195L;
  private String className;
  private Parameters parameters;

  // Don't see why this constructor is here - maybe for Serializable?
  public StepInformation() {
  }

  public StepInformation(Class c) {
    this(c.getName(), Parameters.create());
  }

  public StepInformation(String className) {
    this(className, Parameters.create());
  }

  public StepInformation(Class c, Parameters parameters) {
    this(c.getName(), parameters);
  }

  public StepInformation(String className, Parameters parameters) {
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
