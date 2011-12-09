// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow.execution;

/**
 * Represents the end point type of a TupleFlow connection: either input
 * or output.
 * 
 * @author trevor
 */
public enum ConnectionPointType {
    Input {
        @Override
        public String toString() {
            return "input";
        }
    },
    Output {
        @Override
        public String toString() {
            return "output";
        }
    };

    public static boolean connectable(ConnectionPointType one, ConnectionPointType two) {
        return one != two;
    }
}

