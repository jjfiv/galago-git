/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.links;

import java.io.File;
import java.io.IOException;
import org.lemurproject.galago.tupleflow.CompressionType;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.FileOrderedWriter;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Order;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Type;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;
import org.lemurproject.galago.tupleflow.execution.Verification;

/**
 *
 * @author sjh
 */
public class DataStreamWriter implements Processor<Type> {

  FileOrderedWriter writer;
  Class inputClass;
  Counter written;

  public DataStreamWriter(TupleFlowParameters p) throws Exception {
    String folder = p.getJSON().getString("outputFolder");
    String filename = p.getJSON().getString("outputFile");
    CompressionType c = CompressionType.fromString(p.getJSON().get("compression", "GZIP"));

    File outFile = new File(folder, filename + "." + p.getInstanceId());

    FileUtility.makeParentDirectories(outFile);

    Class orderClass = Class.forName(p.getJSON().getString("order"));
    inputClass = orderClass.getEnclosingClass();
    Order order = (Order) orderClass.getConstructor().newInstance();

    writer = new FileOrderedWriter(outFile.getAbsolutePath(), order, c);

    written = p.getCounter(filename);
  }

  @Override
  public void process(Type object) throws IOException {
    writer.process(object);
    if (written != null) {
      written.increment();
    }
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

  public static String getInputClass(TupleFlowParameters p) {
    return p.getJSON().getString("inputClass");
  }

  public static void verify(TupleFlowParameters parameters, ErrorStore store) {
    if (!Verification.requireParameters(new String[]{"outputFolder", "outputFile", "order", "inputClass"}, parameters.getJSON(), store)) {
      return;
    }
  }
}
