// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow.execution;

/**
 * Represents an output step in a TupleFlow stage.
 * 
 * @author trevor
 */
public class OutputStepInformation extends StepInformation {
    String id;

    public OutputStepInformation(String id) {
      super(id);
        this.id = id;
    }

    public String getId() {
        return id;
    }

    @Override
    public String toString() {
	return id;
    }
}
