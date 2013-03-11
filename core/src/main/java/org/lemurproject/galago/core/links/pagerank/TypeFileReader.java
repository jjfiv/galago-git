/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.links.pagerank;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.lemurproject.galago.tupleflow.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Order;
import org.lemurproject.galago.tupleflow.OrderedCombiner;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.TypeReader;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.tupleflow.types.FileName;

/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.tupleflow.types.FileName")
public class TypeFileReader<T> extends StandardStep<FileName, T> {

  List<String> files = new ArrayList();
  Order order;
  
  public TypeFileReader(TupleFlowParameters p) throws Exception {
    String orderClass = p.getJSON().getString("order");
    order = (Order) Class.forName(orderClass).getConstructor().newInstance();
  }
  
  @Override
  public void process(FileName f) throws IOException {
    files.add(f.filename);
  }

  @Override
  public void close() throws IOException {
    try {
      TypeReader<T> reader = OrderedCombiner.combineFromFiles(files, order);
      reader.setProcessor(processor);
      reader.run();
      // run should call close
    } catch (IncompatibleProcessorException ex) {
      Logger.getLogger(TypeFileReader.class.getName()).log(Level.SEVERE, null, ex);
    }    
  }

  public static String getOutputClass(TupleFlowParameters parameters) {
    return parameters.getJSON().get("outputClass", "");
  }

  public static String[] getOutputOrder(TupleFlowParameters parameters) throws Exception {
    String orderClass = parameters.getJSON().get("outputOrder","");
    Order order = (Order) Class.forName(orderClass).getConstructor().newInstance();
    return order.getOrderSpec();
  }
}

