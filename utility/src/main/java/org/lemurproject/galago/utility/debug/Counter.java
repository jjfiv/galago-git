// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.utility.debug;

/**
 * A counter sends statistics from individual TupleFlow workers back to
 * the JobExecutor.
 * 
 * @author trevor
 */
public interface Counter {
    void increment();
    void incrementBy(int value);
}
