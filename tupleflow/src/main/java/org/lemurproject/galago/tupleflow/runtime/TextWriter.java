// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.tupleflow.runtime;

import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Type;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;
import org.lemurproject.galago.tupleflow.execution.Verification;
import org.lemurproject.galago.utility.Parameters;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;

/**
 * Writes a stream of objects to a text file.  Useful for debugging or as
 * output for simple jobs.
 *
 * @author trevor
 */

public class TextWriter<T extends Type> implements Processor<T> {
    BufferedWriter writer;

    public TextWriter(TupleFlowParameters parameters) throws IOException {
        writer = new BufferedWriter(new FileWriter(parameters.getJSON().getString("filename")));
    }

    public void process(T object) throws IOException {
        writer.write(object.toString());
        writer.write("\n");
    }

    public void close() throws IOException {
        writer.close();
    }

    public static String getInputClass(TupleFlowParameters parameters) {
        return parameters.getJSON().getString("class");
    }

    public static boolean verify(TupleFlowParameters parameters, ErrorStore store) {
        Parameters p = parameters.getJSON();
        if (!Verification.requireParameters(new String[] { "filename", "class" }, p, store))
            return false;
        if (!Verification.requireClass(p.getString("class"), store))
            return false;
        if (!Verification.requireWriteableFile(p.getString("filename"), store))
            return false;
        return true;
    }
}
