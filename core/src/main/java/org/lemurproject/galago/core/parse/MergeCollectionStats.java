// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.IOException;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.tupleflow.types.SerializedParameters;

/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.tupleflow.types.SerializedParameters", order={"+parameters"})
@OutputClass(className = "org.lemurproject.galago.tupleflow.types.SerializedParameters", order={"+parameters"})
public class MergeCollectionStats extends StandardStep<SerializedParameters, SerializedParameters> {

  Parameters stats = new Parameters();

  @Override
  public void process(SerializedParameters serial) throws IOException {
    Parameters fragment = Parameters.parse(serial.parameters);
    for (String key : fragment.getKeys()) {
      if (fragment.isLong(key)) {
        long cumulativeStat = stats.get(key, 0L) + fragment.getLong(key);
        stats.set(key, cumulativeStat);
      } else if (fragment.isDouble(key)) {
        double cumulativeStat = stats.get(key, 0.0) + fragment.getDouble(key);
        stats.set(key, cumulativeStat);
      } else {
        throw new IOException("Unable to accumulate non numeric statistic: " + key);
      }
    }
  }

  @Override
  public void close() throws IOException {
    processor.process(new SerializedParameters(stats.toString()));
    processor.close();
  }
}
