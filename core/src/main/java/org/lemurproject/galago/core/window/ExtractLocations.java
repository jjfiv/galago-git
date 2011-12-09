// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.window;

import java.io.IOException;

import org.lemurproject.galago.core.types.TextFeature;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 * <p> Discards word data - Only locations are maintained.
 * This leads to some space savings in any output files.
 * </p>
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.types.TextFeature", order={"+file", "+filePosition"})
@OutputClass(className = "org.lemurproject.galago.core.types.TextFeature", order={"+file", "+filePosition"})
public class ExtractLocations extends StandardStep<TextFeature, TextFeature> {

  public void process(TextFeature tf) throws IOException {
    tf.feature = new byte[0];
    processor.process(tf);
  }
}
