// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow.execution;

/**
 * Represents an input step in a TupleFlow stage.
 * 
 * @author trevor
 */
public class InputStep extends Step {

  String id;

  public InputStep(String id) {
    super(id);
    this.id = id;
  }

  public InputStep(FileLocation location, String id) {
    super(id);
    this.id = id;
    this.location = location;
  }

  public String getId() {
    return id;
  }
}
