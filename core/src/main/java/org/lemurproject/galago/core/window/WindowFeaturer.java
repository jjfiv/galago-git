// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.window;

import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.lemurproject.galago.core.types.TextFeature;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 * Converts ngrams to NgramFeatures
 * This keeps track of the file and location 
 *  that the ngram was extracted from.
 * 
 * An MD5 hash value is used instead of the original ngram
 * This reduces the size of the output.
 * 
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.window.Window")
@OutputClass(className = "org.lemurproject.galago.core.types.TextFeature")
public class WindowFeaturer extends StandardStep<Window, TextFeature> {

  int bytes;
  MessageDigest hashFunction;

  public WindowFeaturer(TupleFlowParameters parameters) throws IOException, NoSuchAlgorithmException {
    hashFunction = MessageDigest.getInstance("MD5");
  }

  @Override
  public void process(Window w) throws IOException {
    hashFunction.update(w.data);
    byte[] hashValue = hashFunction.digest();
    processor.process(new TextFeature(w.file, w.filePosition, hashValue));
  }
}
