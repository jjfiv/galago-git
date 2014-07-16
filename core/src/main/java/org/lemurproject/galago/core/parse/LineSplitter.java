/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.parse;

import java.io.IOException;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.utility.ByteUtil;

/**
 *
 * @author sjh
 */
@Verified
@InputClass(className="java.lang.String")
@OutputClass(className="org.lemurproject.galago.core.types.KeyValuePair")
public class LineSplitter extends StandardStep<String, KeyValuePair> {
  
  String split;
  
  public LineSplitter(TupleFlowParameters p) {
    split = p.getJSON().get("split", "\t");
  }
  
  @Override
  public void process(String line) throws IOException {
    if (line.length() == 0) {
      return;
    }
    
    String[] parts = line.split(split, 2);
    
    if (parts.length == 2) {
      processor.process(
              new KeyValuePair(
              ByteUtil.fromString(parts[0]),
              ByteUtil.fromString(parts[1])));
    } else {
      processor.process(
              new KeyValuePair(
              ByteUtil.fromString(parts[0]),
              new byte[0]));
    }
  }
}
