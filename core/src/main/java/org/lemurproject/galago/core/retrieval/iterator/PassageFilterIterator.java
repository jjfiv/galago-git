// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.processing.PassageScoringContext;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.util.ExtentArray;

/**
 *
 * @author irmarc
 */
public class PassageFilterIterator extends TransformIterator implements ExtentIterator, CountIterator {

  ExtentIterator extentIterator;
  PassageScoringContext passageContext;
  int begin, end;
  long docid;
  ExtentArray cached;
  protected byte[] key;

  public PassageFilterIterator(NodeParameters parameters, ExtentIterator extentIterator) {
    super(extentIterator);
    this.extentIterator = extentIterator;
    this.cached = new ExtentArray();
    docid = -1;
  }

  @Override
  public boolean hasMatch(long document) {
    return super.hasMatch(document) && (extents().size() > 0);
  }

  /**
   * Filters out extents that are not in the range of the current passage
   * window.
   *
   * @return
   */
  @Override
  public ExtentArray extents() {
    if (passageContext == null) {
      return extentIterator.extents();
    }

    if (docid != passageContext.document || begin != passageContext.begin
            || end != passageContext.end) {
      loadExtents();
    }
    return cached;
  }

  private void loadExtents() {
    cached.reset();
    ExtentArray internal = extentIterator.extents();

    if (passageContext != null) {
      for (int i = 0; i < internal.size(); i++) {
        if (internal.begin(i) >= passageContext.begin
                && internal.end(i) <= passageContext.end) {
          cached.add(internal.begin(i), internal.end(i));
        }
      }
      docid = passageContext.document;
      begin = passageContext.begin;
      end = passageContext.end;
    }
  }

  @Override
  public void setContext(ScoringContext context) {
    super.setContext(context);

    if (!PassageScoringContext.class.isAssignableFrom(context.getClass())) {
      // debugging info line.
      // Logger.getLogger(PassageFilterIterator.class.getName()).info("Setting a non-Passage-capable context as a PassageScoringContext - passages can not be used.");
      passageContext = null;
    } else {
      passageContext = (PassageScoringContext) context;
    }
  }

  @Override
  public ExtentArray data() {
    return extents();
  }

  @Override
  public int count(ScoringContext c) {
    return extents().size();
  }

  @Override
  public AnnotatedNode getAnnotatedNode(ScoringContext c) throws IOException {
    String type = "extent";
    String className = this.getClass().getSimpleName();
    String parameters = "";
    long document = currentCandidate();
    boolean atCandidate = hasMatch(c.document);
    String returnValue = extents().toString();
    List<AnnotatedNode> children = Collections.singletonList(extentIterator.getAnnotatedNode(c));

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
