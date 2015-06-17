package org.lemurproject.galago.core.parse;

import gnu.trove.map.hash.TObjectIntHashMap;
import gnu.trove.procedure.TObjectIntProcedure;
import org.lemurproject.galago.core.types.NumberWordCount;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.utility.ByteUtil;

import java.io.IOException;

/**
 * Extract single term counts.
 * @author jfoley.
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
@OutputClass(className = "org.lemurproject.galago.core.types.NumberWordCount")
public class UnigramCountExtractor extends StandardStep<Document, NumberWordCount> {
  @Override
  public void process(final Document doc) throws IOException {
    TObjectIntHashMap<String> freqs = new TObjectIntHashMap<>();
    for (String term : doc.terms) {
      freqs.adjustOrPutValue(term, 1, 1);
    }

    freqs.forEachEntry(
        new TObjectIntProcedure<String>() {
          @Override
          public boolean execute(String term, int count) {
            try {
              processor.process(new NumberWordCount(ByteUtil.fromString(term), doc.identifier, count));
            } catch (IOException e) {
              e.printStackTrace();
            }
            return true;
          }
        });
  }
}
