// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.corpus;

import java.io.IOException;
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
  boolean psuedoDocs;
  TagTokenizer tokenizer;

  public CorpusReaderSource(BTreeReader rdr) throws IOException {
    super(rdr);
    docParams = new DocumentComponents();
    final Parameters manifest = btreeReader.getManifest();

    psuedoDocs = manifest.get("psuedo", false);
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
  public Document getData(long id) throws IOException {
    if (currentCandidate() == id) {
      Document doc = Document.deserialize(btreeIter.getValueBytes(), docParams);
      if (docParams.tokenize) {
        tokenizer.tokenize(doc);
      }
      return doc;
    }
    return null;
  }

  @Override
  public long currentCandidate() {
    return Utility.toLong(btreeIter.getKey());
  }

  @Override
  public void syncTo(long id) throws IOException {
    btreeIter.skipTo(Utility.fromLong(id));
  }
}
