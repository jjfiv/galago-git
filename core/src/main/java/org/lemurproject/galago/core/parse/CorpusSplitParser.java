// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.parse;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.lemurproject.galago.core.index.corpus.CorpusReader;
import org.lemurproject.galago.core.index.corpus.DocumentReader;
import org.lemurproject.galago.core.index.corpus.DocumentReader.DocumentIterator;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Reads Document data from an index file.  Typically you'd use this parser by
 * including UniversalParser in a TupleFlow Job.
 * 
 * @author trevor, sjh
 */
public class CorpusSplitParser implements DocumentStreamParser {
  DocumentReader reader;
  DocumentIterator iterator;
  DocumentSplit split;

  public CorpusSplitParser(DocumentSplit split) throws FileNotFoundException, IOException {
    reader = new CorpusReader( split.fileName );
    iterator = (DocumentIterator) reader.getIterator();
    iterator.skipToKey(split.startKey);
    this.split = split;
  }

  public Document nextDocument() throws IOException {
    if (iterator.isDone()) {
      reader.close();
      return null;
    }

    String key = iterator.getKeyString();
    byte[] keyBytes = Utility.fromString(key);

    // Don't go past the end of the split.
    if (split.endKey.length > 0 && Utility.compare(keyBytes, split.endKey) >= 0) {
      reader.close();
      return null;
    }

    Parameters p = new Parameters();
    p.set("terms", false);
    p.set("tags", false);
    Document document = iterator.getDocument(p);
    iterator.nextKey();
    return document;
  }

}
