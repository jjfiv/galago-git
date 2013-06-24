// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator.disk;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.lemurproject.galago.core.index.source.DataSource;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;

/**
 *
 * @author jfoley
 * @see CorpusReader
 */
public class DiskCorpusIterator extends DiskDataIterator<Document> {
  public DiskCorpusIterator(DataSource<Document> src) {
    super(src);
  }

  @Override
  public AnnotatedNode getAnnotatedNode() throws IOException {
    String type = "corpus";
    String className = this.getClass().getSimpleName();
    String parameters = "";
    long document = currentCandidate();
    boolean atCandidate = hasMatch(this.context.document);
    String returnValue = getData().name;
    String extraInfo = getData().toString();
    List<AnnotatedNode> children = Collections.EMPTY_LIST;

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, extraInfo, children);
  }
}
