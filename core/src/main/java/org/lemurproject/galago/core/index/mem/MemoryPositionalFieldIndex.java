// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.mem;

import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Tag;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.Parameters;

import java.io.IOException;


/*
 * author MichaelZ - based on MemoryPositionalIndex
 *
 * In-memory field index
 *
 */

public class MemoryPositionalFieldIndex extends MemoryPositionalIndex {

  private String fieldName = null;

  public MemoryPositionalFieldIndex(Parameters parameters) throws Exception {
    super(parameters);
    fieldName = parameters.getAsString("field");
  }

  @Override
  public void addDocument(Document doc) throws IOException {

    int postingCount = 0;
    for (Tag tag : doc.tags) {
      if (tag.name.equals(fieldName) == false) {
        continue;
      }
      for (int i = tag.begin; i < tag.end; i++) {
        String stem = stemAsRequired(doc.terms.get(i));
        if (stem != null) {
          addPosting(ByteUtil.fromString(stem), doc.identifier, i);
          postingCount++;
        }
      }
    }
    collectionDocumentCount += 1;
    collectionPostingsCount += postingCount;
    vocabCount = postings.size();
  }

}
