/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.links.pagerank;

import java.io.File;
import java.io.IOException;
import org.lemurproject.galago.tupleflow.CompressionType;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.FileOrderedWriter;
import org.lemurproject.galago.tupleflow.Order;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Type;
import org.lemurproject.galago.tupleflow.execution.ErrorHandler;
import org.lemurproject.galago.tupleflow.execution.Verification;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 *
 * @author sjh
 */
@Verified
public class TypeFileWriter<T> implements Processor<T> {

  Processor<T> writer;
  Counter counter;
  long count;
  private final File outFile;

  public TypeFileWriter(TupleFlowParameters p) throws Exception {
    String outputClass = p.getJSON().get("class", "");
    String[] orderSpec = p.getJSON().get("order", "").split(" ");

    Type output = (Type) Class.forName(outputClass).getConstructor().newInstance();
    Order order = output.getOrder(orderSpec);

    outFile = new File(p.getJSON().getString("outputFile") + p.getInstanceId());

    writer = new FileOrderedWriter(outFile.getAbsolutePath(), order, CompressionType.VBYTE);
    counter = p.getCounter("Objects Written");
    count = 0;
  }

  @Override
  public void process(T t) throws IOException {
    writer.process(t);
    count += 1;
    if (counter != null) {
      counter.increment();
    }
  }

  @Override
  public void close() throws IOException {
    writer.close();

    // ensure we don't leave empty files around.
    if (count == 0) {
      outFile.delete();
    }
  }

  public static String getInputClass(TupleFlowParameters parameters) {
    return parameters.getJSON().get("inputClass", "");
  }

  public static void verify(TupleFlowParameters fullParameters, ErrorHandler handler) {
    Parameters parameters = fullParameters.getJSON();

    String[] requiredParameters = {"class", "order", "outputFile"};

    if (!Verification.requireParameters(requiredParameters, parameters, handler)) {
      return;
    }

    String className = parameters.getString("class");
    String[] orderSpec = parameters.getString("order").split(" ");

    Verification.requireClass(className, handler);
    Verification.requireOrder(className, orderSpec, handler);
  }
}
