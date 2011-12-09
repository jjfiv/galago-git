// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.tupleflow;

import org.lemurproject.galago.tupleflow.execution.ErrorHandler;
import org.lemurproject.galago.tupleflow.execution.Verification;

/**
 *
 * @author trevor
 */

public class NullProcessor<T> implements Processor<T> {
    Class<T> inputClass;
    
    public NullProcessor() {
        inputClass = null;
    }
    
    public NullProcessor(TupleFlowParameters parameters) throws ClassNotFoundException {
        String className = parameters.getJSON().getString("class");
        this.inputClass = (Class<T>) Class.forName(className);
    }
    
    public NullProcessor(Class<T> inputClass) { this.inputClass = inputClass; }
    public void process(T object) {}
    public void close() {}
     
    public static String getInputClass(TupleFlowParameters parameters) {
        return parameters.getJSON().get("class", "");
    }
   
    public static String[] getInputOrder(TupleFlowParameters parameters) {
        String[] orderSpec = parameters.getJSON().get("order", "").split(" ");
        return orderSpec;
    }
    
    public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
        Verification.requireParameters(new String[] { "class" }, parameters.getJSON(), handler);
    }
}
