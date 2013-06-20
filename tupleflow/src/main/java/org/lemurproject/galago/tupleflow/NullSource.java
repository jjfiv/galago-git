// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.tupleflow;

import org.lemurproject.galago.tupleflow.execution.Verification;
import java.io.IOException;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;

/**
 *
 * @author trevor
 */
public class NullSource<T> implements ExNihiloSource<T> {
    public Processor<T> processor;
    Class<T> outputClass;

    public NullSource(TupleFlowParameters parameters) throws ClassNotFoundException {
        String className = parameters.getJSON().getString("class");
        this.outputClass = (Class<T>) Class.forName(className);
    }

    public NullSource(Class<T> outputClass) {
        this.outputClass = outputClass;
    }

    public static void verify(TupleFlowParameters parameters, ErrorStore store) {
        Verification.requireParameters(new String[]{"class"}, parameters.getJSON(), store);
        Verification.requireClass(parameters.getJSON().getString("class"), store);
    }

    @Override
    public void setProcessor(Step processor) throws IncompatibleProcessorException {
        Linkage.link(this, processor);
    }

    public void run() throws IOException {
        processor.close();
    }

    public static String getOutputClass(TupleFlowParameters parameters) {
        return parameters.getJSON().get("class", "");
    }
}
