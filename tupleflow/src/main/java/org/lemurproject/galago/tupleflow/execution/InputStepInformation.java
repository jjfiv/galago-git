// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow.execution;

/**
 * Represents an input step in a TupleFlow stage.
 * 
 * @author trevor
 */
public class InputStepInformation extends StepInformation {

  String id;

  public InputStepInformation(String id) {
    super(id);
    this.id = id;
  }

  public String getId() {
    return id;
  }
}
