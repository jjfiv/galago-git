// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.corpus;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.source.BTreeKeySource;
import org.lemurproject.galago.core.index.source.DataSource;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Document.DocumentComponents;
import org.lemurproject.galago.core.parse.TagTokenizer;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author jfoley
 */
public class CorpusReaderSource extends BTreeKeySource implements DataSource<Document> {

  DocumentComponents docParams;
  TagTokenizer tokenizer;

  public CorpusReaderSource(BTreeReader rdr) throws IOException {
    super(rdr);
    docParams = new DocumentComponents();
    final Parameters manifest = btreeReader.getManifest();

    if (manifest.containsKey("tokenizer") && manifest.isMap("tokenizer")) {
      tokenizer = new TagTokenizer(new FakeParameters(manifest.getMap("tokenizer")));
    } else {
      tokenizer = new TagTokenizer(new FakeParameters(new Parameters()));
    }
  }

  @Override
  public boolean hasAllCandidates() {
    return true;
  }

  @Override
  public String key() {
    return "corpus";
  }

  @Override
  public boolean hasMatch(long id) {
    return (!isDone() && currentCandidate() == id);
  }

  @Override
  public Document data(long id) {
    if (currentCandidate() == id) {
      try {
        Document doc = Document.deserialize(btreeIter.getValueBytes(), docParams);
        if (docParams.tokenize) {
          tokenizer.tokenize(doc);
        }
        return doc;
      } catch (IOException ex) {
        Logger.getLogger(CorpusReaderSource.class.getName()).log(Level.SEVERE, "Failed to deserialize document " + id, ex);
      }
    }
    return null;
  }
}
