// BSD License (http://galagosearch.org)

package org.lemurproject.galago.tupleflow.execution;

import java.util.ArrayList;

/**
 * Represents a data connection between two stages in a TupleFlow job.
 * 
 * @see org.galagosearch.tupleflow.execution.Job
 * @author trevor
 */
public class Connection extends Locatable implements Cloneable {
    String className;
    String connectionName;
    String[] order;
    String[] hash;
    int hashCount;
    public ArrayList<ConnectionEndPoint> inputs = new ArrayList<ConnectionEndPoint>();
    public ArrayList<ConnectionEndPoint> outputs = new ArrayList<ConnectionEndPoint>();

    public Connection(FileLocation location, String connectionName, String className, String[] order, String[] hash, int hashCount) {
        super(location);
        this.connectionName = connectionName;
        this.className = className;
        this.order = order;
        this.hash = hash;
        this.hashCount = hashCount;
    }

    public Connection(FileLocation location, String className, String[] order, String[] hash, int hashCount) {
        this(location, null, className, order, hash, hashCount);
    }

    public String getName() {
        if (connectionName == null) {
            assert inputs.size() > 0;
            return inputs.get(0).getStageName() + "-" + inputs.get(0).getPointName();
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

    @Override
    public Connection clone() {
        try {
            Connection copy = (Connection) super.clone();
            ArrayList<ConnectionEndPoint> inputCopy = new ArrayList<ConnectionEndPoint>();

            for (ConnectionEndPoint point : inputs) {
                inputCopy.add(point.clone());
            }

            ArrayList<ConnectionEndPoint> outputCopy = new ArrayList<ConnectionEndPoint>();

            for (ConnectionEndPoint point : outputs) {
                outputCopy.add(point.clone());
            }

            copy.inputs = inputCopy;
            copy.outputs = outputCopy;
            return copy;
        } catch (CloneNotSupportedException ex) {
            throw new RuntimeException("Expected cloning to be supported in superclass", ex);
        }
    }
} 
