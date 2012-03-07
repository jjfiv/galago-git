// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.core.retrieval.processing.PassageScoringContext;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.util.ExtentArray;

/**
 *
 * @author irmarc
 */
public class PassageFilterIterator extends TransformIterator implements MovableExtentIterator, MovableCountIterator {

  ExtentIterator extentIterator;
  PassageScoringContext passageContext;
  int begin, end, docid;
  ExtentArray cached;

  public PassageFilterIterator(NodeParameters parameters, MovableExtentIterator extentIterator) {
    super(extentIterator);
    this.extentIterator = extentIterator;
    docid = -1;
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
    cached = new ExtentArray();
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
      throw new RuntimeException("Trying to set a non-Passage-capable context as a PassageScoringContext");
    }
    context = (PassageScoringContext) context;
  }

  @Override
  public ExtentArray getData() {
    return extents();
  }

  @Override
  public int count() {
    return extents().size();
  }

  @Override
  public int maximumCount() {
    return this.extentIterator.maximumCount();
  }
}
