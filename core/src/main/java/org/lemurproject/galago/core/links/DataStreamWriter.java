/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.links;

import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;
import org.lemurproject.galago.tupleflow.execution.Verification;
import org.lemurproject.galago.tupleflow.runtime.FileOrderedWriter;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.debug.Counter;

import java.io.File;
import java.io.IOException;

/**
 *
 * @author sjh
 */
public class DataStreamWriter implements Processor<Type> {

  FileOrderedWriter<Type> writer;
  Class inputClass;
  Counter written;

  public DataStreamWriter(TupleFlowParameters p) throws Exception {
    String folder = p.getJSON().getString("outputFolder");
    String filename = p.getJSON().getString("outputFile");
    CompressionType c = CompressionType.fromString(p.getJSON().get("compression", "GZIP"));

    File outFile = new File(folder, filename + "." + p.getInstanceId());

    FSUtil.makeParentDirectories(outFile);

    Class<?> orderClass = Class.forName(p.getJSON().getString("order"));
    inputClass = orderClass.getEnclosingClass();
    Order<Type> order = (Order<Type>) orderClass.getConstructor().newInstance();

    writer = new FileOrderedWriter<>(outFile.getAbsolutePath(), order, c);

    written = p.getCounter(filename);
  }

  @Override
  public void process(Type object) throws IOException {
    writer.process(object);
    written.increment();
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
