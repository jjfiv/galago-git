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
 * Reads Document data from an index file. Typically you'd use this parser by
 * including UniversalParser in a TupleFlow Job.
 *
 * @author trevor, sjh
 */
public class CorpusSplitParser implements DocumentStreamParser {

  DocumentReader reader;
  DocumentIterator iterator;
  DocumentSplit split;
  Parameters extractionParameters;

  public CorpusSplitParser(DocumentSplit split) throws FileNotFoundException, IOException {
    this(split, new Parameters());
  }

  public CorpusSplitParser(DocumentSplit split, Parameters p) throws FileNotFoundException, IOException {
    System.err.printf("Creating corpus split parser with parameters:\n%s\n", p.toPrettyString());
    reader = new CorpusReader(split.fileName);
    iterator = (DocumentIterator) reader.getIterator();
    iterator.skipToKey(split.startKey);
    this.split = split;
    if (p.isEmpty()) {
      p.set("terms", false);
      p.set("tags", false);
    }
    extractionParameters = p;
    System.err.printf("Extraction parameters: %s\n", p.toPrettyString());
  }

  @Override
  public Document nextDocument() throws IOException {
    if (iterator.isDone()) {
      return null;
    }

    byte[] keyBytes = iterator.getKey();

    // Don't go past the end of the split.
    if (split.endKey.length > 0 && Utility.compare(keyBytes, split.endKey) >= 0) {
      return null;
    }

    Document document = iterator.getDocument(extractionParameters);
    iterator.nextKey();
    return document;
  }

  @Override
  public void close() throws IOException {
    reader.close();
  }
}
