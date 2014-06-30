// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.corpus;

import org.lemurproject.galago.core.corpus.DocumentSerializer;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.source.BTreeKeySource;
import org.lemurproject.galago.core.index.source.DataSource;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Document.DocumentComponents;
import org.lemurproject.galago.utility.Parameters;

import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author jfoley
 */
public class CorpusReaderSource extends BTreeKeySource implements DataSource<Document> {

  private final DocumentSerializer serializer;
  private final DocumentComponents docParams;

  public CorpusReaderSource(BTreeReader rdr) throws IOException {
    super(rdr);
    docParams = new DocumentComponents();
    final Parameters manifest = btreeReader.getManifest();
    serializer = DocumentSerializer.instance(manifest);
  }

  public CorpusReaderSource(BTreeReader rdr, DocumentComponents opts) throws IOException {
    super(rdr);
    this.docParams = opts;
    final Parameters manifest = btreeReader.getManifest();
    serializer = DocumentSerializer.instance(manifest);
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

  public Document getDocument(long id, DocumentComponents doc) {
    if (currentCandidate() == id) {
      try {
        return serializer.fromStream(btreeIter.getValueStream(), docParams);
      } catch (IOException ex) {
        Logger.getLogger(CorpusReaderSource.class.getName()).log(Level.SEVERE, "Failed to deserialize document " + id, ex);
      }
    }
    return null;
  }

  @Override
  public Document data(long id) {
    return getDocument(id, docParams);
  }
}
