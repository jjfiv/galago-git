// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator.disk;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.lemurproject.galago.core.index.corpus.CorpusReader;
import org.lemurproject.galago.core.index.source.DataSource;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
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
  public AnnotatedNode getAnnotatedNode(ScoringContext c) throws IOException {
    String type = "corpus";
    String className = this.getClass().getSimpleName();
    String parameters = "";
    long document = currentCandidate();
    boolean atCandidate = hasMatch(c.document);
    String returnValue = data().name;
    String extraInfo = data().toString();
    List<AnnotatedNode> children = Collections.EMPTY_LIST;

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, extraInfo, children);
  }
}
