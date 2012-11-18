/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.hash;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import org.lemurproject.galago.core.types.NumberWordCount;
import org.lemurproject.galago.core.window.Window;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.VByteOutput;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.window.Window")
@OutputClass(className = "org.lemurproject.galago.core.types.NumberWordCount")
public class WindowHasher extends StandardStep<Window, NumberWordCount> {

  int depth;
  UniversalStringHashFunction[] hashFunctions;

  public WindowHasher(TupleFlowParameters params) {
    Parameters p = params.getJSON();
    Parameters hashParams = p.getMap("hashFns");
    depth = (int) p.getLong("depth");
    hashFunctions = new UniversalStringHashFunction[depth];
    for (int row = 0; row < depth; row++) {
      hashFunctions[row] = new UniversalStringHashFunction(hashParams.getMap(Integer.toString(row)));
    }
  }

  @Override
  public void process(Window object) throws IOException {
    ByteArrayOutputStream array;
    VByteOutput stream;
    for (int row = 0; row < depth; row++) {
      array = new ByteArrayOutputStream();
      stream = new VByteOutput(new DataOutputStream(array));
      long hashValue = hashFunctions[row].hash(object.data);
      stream.writeLong(hashValue);
      stream.writeInt(row);
      
      processor.process(new NumberWordCount(array.toByteArray(), object.document, 1));
    }
  }
}
