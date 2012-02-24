/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.parse;

import java.io.File;
import java.io.IOException;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.index.disk.DiskNameReader;
import org.lemurproject.galago.core.index.disk.DiskNameReverseReader;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.core.types.NumberKeyValue;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.types.KeyValuePair")
@OutputClass(className = "org.lemurproject.galago.core.types.NumberKeyValue")
public class NumberKeyValuePairs extends StandardStep<KeyValuePair, NumberKeyValue> {

  private final DiskNameReverseReader.KeyIterator namesIterator;
  private Counter numbered;

  public NumberKeyValuePairs(TupleFlowParameters parameters) throws IOException {
    String namesPath = parameters.getJSON().getString("indexPath") + File.separator + "names.reverse";
    namesIterator = ((DiskNameReverseReader) DiskIndex.openIndexPart(namesPath)).getIterator();
    numbered = parameters.getCounter("Numbered Items");
  }

  @Override
  public void process(KeyValuePair kvp) throws IOException {
    if (!namesIterator.isDone()) {
      if (namesIterator.skipToKey(kvp.key)) {
        if (numbered != null) {
          numbered.increment();
        }
        processor.process(new NumberKeyValue(namesIterator.getCurrentIdentifier(), kvp.key, kvp.value));
      }
    }
  }
}
