// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.tupleflow.execution;

import java.io.Serializable;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class Step implements Serializable {
    protected FileLocation location;
    private String className;
    private Parameters parameters;

    public Step() {
    }

    public Step(Class c) {
        this(null, c.getName(), new Parameters());
    }

    public Step(String className) {
        this(null, className, new Parameters());
    }
    
    public Step(Class c, Parameters parameters) {
        this(null, c.getName(), parameters);
    }

    public Step(String className, Parameters parameters) {
        this(null, className, parameters);
    }

    public Step(FileLocation location, String className, Parameters parameters) {
        this.location = location;
        this.className = className;
        this.parameters = parameters;
    }

    public FileLocation getLocation() {
        return location;
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
