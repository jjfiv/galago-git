// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import org.lemurproject.galago.core.index.BTreeFactory;
import org.lemurproject.galago.core.index.corpus.CorpusReader;
import org.lemurproject.galago.core.index.corpus.DocumentReader;
import org.lemurproject.galago.core.index.corpus.DocumentReader.DocumentIterator;
import org.lemurproject.galago.core.parse.Document.DocumentComponents;
import org.lemurproject.galago.core.parse.PseudoDocument.PsuedoDocumentComponents;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Reads Document data from an index file. Typically you'd use this parser by
 * including UniversalParser in a TupleFlow Job.
 * 
 * In particular, this corpus reader filters on a set of documentNames
 *
 * @author trevor, sjh
 */
public class CorpusSelectiveSplitParser extends DocumentStreamParser {

  int idx;
  int[] docIds;
  DocumentReader reader;
  DocumentComponents extractionParameters;

  public CorpusSelectiveSplitParser(DocumentSplit split, Parameters p) throws FileNotFoundException, IOException {
    super(split, p);
    // check that the corpus is an actual corpus
    if (BTreeFactory.isBTree(split.fileName)) {
      reader = new CorpusReader(split.fileName);
    }

    if (p.get("psuedo", false)) {
      extractionParameters = new PsuedoDocumentComponents(false, false, false, true, true);
    } else {
      extractionParameters = new DocumentComponents(true, false, false, true);
    }

    // Must be a simple list of strings, one per line;
    File ids = new File(p.getString("docIds"));
    Set<String> documentIds = Utility.readFileToStringSet(ids);
    docIds = new int[documentIds.size()];
    int i = 0;
    for (String sid : documentIds) {
      int id = Integer.parseInt(sid);
      docIds[i] = id;
    }

    // ensure increasing order...
    Arrays.sort(docIds);

    idx = 0;
  }

  @Override
  public Document nextDocument() throws IOException {
    if (reader == null) {
      return null;
    }
    if (idx < docIds.length) {
      Document d;
      do {
        d = reader.getDocument(docIds[idx], extractionParameters);
        if (d == null) {
          System.err.println("Failed to extract document id:" + docIds[idx]);
        }

        // move on to next document id
        idx += 1;
      } while (d == null);
      return d;
    }
    return null;
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    }
  }
}
