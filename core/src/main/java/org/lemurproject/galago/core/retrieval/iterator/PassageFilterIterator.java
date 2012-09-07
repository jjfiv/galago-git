// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.logging.Logger;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.processing.PassageScoringContext;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.AnnotatedNode;
import org.lemurproject.galago.core.util.ExtentArray;

/**
 *
 * @author irmarc
 */
public class PassageFilterIterator extends TransformIterator implements MovableExtentIterator, MovableCountIterator {

  MovableExtentIterator extentIterator;
  PassageScoringContext passageContext;
  int begin, end, docid;
  ExtentArray cached;
  protected byte[] key;

  public PassageFilterIterator(NodeParameters parameters, MovableExtentIterator extentIterator) {
    super(extentIterator);
    this.extentIterator = extentIterator;
    this.cached = new ExtentArray();
    docid = -1;
    key = extentIterator.key();
  }

  @Override
  public byte[] key() {
    return key;
  }

  /**
   * Filters out extents that are not in the range of the current passage window.
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

  @Override
  public void setContext(ScoringContext context) {
    if (!PassageScoringContext.class.isAssignableFrom(context.getClass())) {
      Logger.getLogger(PassageFilterIterator.class.getName()).info("Setting a non-Passage-capable context as a PassageScoringContext - passages will not be used.");
        passageContext = null;
    } else {
      passageContext = (PassageScoringContext) context;
    }
  }

  @Override
  public ExtentArray getData() {
    return cached;
  }

  @Override
  public int count() {
    return cached.size();
  }

  @Override
  public int maximumCount() {
    return this.extentIterator.maximumCount();
  }

  @Override
  public AnnotatedNode getAnnotatedNode() throws IOException {
    String type = "extent";
    String className = this.getClass().getSimpleName();
    String parameters = "";
    int document = currentCandidate();
    boolean atCandidate = hasMatch(this.context.document);
    String returnValue = extents().toString();
    List<AnnotatedNode> children = Collections.singletonList(extentIterator.getAnnotatedNode());

    return new AnnotatedNode(type, className, parameters, document, atCandidate, returnValue, children);
  }
}
