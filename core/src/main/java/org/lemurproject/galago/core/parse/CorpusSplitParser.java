// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import org.lemurproject.galago.core.index.corpus.CorpusReader;
import org.lemurproject.galago.core.index.corpus.DocumentReader;
import org.lemurproject.galago.core.index.corpus.DocumentReader.DocumentIterator;
import org.lemurproject.galago.core.parse.Document.DocumentComponents;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.IOException;

/**
 * Reads Document data from an index file. Typically you'd use this parser by
 * including UniversalParser in a TupleFlow Job.
 *
 * @author trevor, sjh
 */
public class CorpusSplitParser extends DocumentStreamParser {

  DocumentReader reader;
  DocumentIterator iterator;
  DocumentSplit split;
  DocumentComponents extractionParameters;

  public CorpusSplitParser(DocumentSplit split, Parameters p) throws IOException {
    super(split, p);
    this.reader = new CorpusReader(split.fileName);
    this.iterator = (DocumentIterator) reader.getIterator();
    if(split.startKey != null) {
      this.iterator.skipToKey(split.startKey);
    }
    this.split = split;

    extractionParameters = new DocumentComponents(true, true, false);
  }

  @Override
  public Document nextDocument() throws IOException {
    if (reader != null && iterator.isDone()) {
      return null;
    }

    byte[] keyBytes = iterator.getKey();

    // Don't go past the end of the split.
    if (split.endKey != null && split.endKey.length > 0 && Utility.compare(keyBytes, split.endKey) >= 0) {
      return null;
    }

    Document document = iterator.getDocument(extractionParameters);
    iterator.nextKey();
    return document;
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    
      iterator = null;
    }
  }
}
