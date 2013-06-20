/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.links.pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Order;
import org.lemurproject.galago.tupleflow.OrderedCombiner;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Type;
import org.lemurproject.galago.tupleflow.TypeReader;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;
import org.lemurproject.galago.tupleflow.execution.Verification;
import org.lemurproject.galago.tupleflow.types.FileName;

/**
 *
 * @author sjh
 */
@InputClass(className = "org.lemurproject.galago.tupleflow.types.FileName")
public class TypeFileReader<T> extends StandardStep<FileName, T> {

  List<String> files = new ArrayList();
  Order order;
  Counter counter;

  public TypeFileReader(TupleFlowParameters p) throws Exception {
    String outputClass = p.getJSON().get("outputClass", "");
    String[] orderSpec = getOutputOrder(p);

    Type output = (Type) Class.forName(outputClass).getConstructor().newInstance();
    order = output.getOrder(orderSpec);

    counter = p.getCounter("Objects Read");
  }

  @Override
  public void process(FileName f) throws IOException {
    files.add(f.filename);
  }

  @Override
  public void close() throws IOException {
    TypeReader<T> reader = OrderedCombiner.combineFromFiles(files, order);
    T t = reader.read();
    while (t != null) {
      if (counter != null) {
        counter.increment();
      }
      processor.process(t);
      t = reader.read();
    }
    processor.close();
  }

  public static String getOutputClass(TupleFlowParameters p) {
    return p.getJSON().get("outputClass", "");
  }

  public static String[] getOutputOrder(TupleFlowParameters p) throws Exception {
    String[] orderSpec = p.getJSON().get("order", "").split(" ");
    return orderSpec;
  }

  public static void verify(TupleFlowParameters fullParameters, ErrorStore store) {
    Parameters parameters = fullParameters.getJSON();

    String[] requiredParameters = {"order", "outputClass"};

    if (!Verification.requireParameters(requiredParameters, parameters, store)) {
      return;
    }

    String className = parameters.getString("outputClass");
    String[] orderSpec = parameters.getString("order").split(" ");

    Verification.requireClass(className, store);
    Verification.requireOrder(className, orderSpec, store);
  }
}
