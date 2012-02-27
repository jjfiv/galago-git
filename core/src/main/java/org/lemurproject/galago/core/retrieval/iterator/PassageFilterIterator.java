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
public class PassageFilterIterator extends ExtentFilterIterator implements ContextualIterator {

  PassageScoringContext context;
  int begin, end, docid;
  ExtentArray cached;

  public PassageFilterIterator(NodeParameters parameters, ExtentValueIterator extentIterator) {
    super(parameters, extentIterator);
    docid = -1;
  }

  /**
   * Filters out extents that are not in the range of the current passage window.
   *
   * @return
   */
  @Override
  public ExtentArray extents() {
    if (context == null) {
      return extentIterator.extents();
    }
    
    if (docid != context.document || begin != context.begin
            || end != context.end) {
      loadExtents();
    }
    return cached;
  }

  private void loadExtents() {
    cached = new ExtentArray();
    ExtentArray internal = extentIterator.extents();

    for (int i = 0; i < internal.size(); i++) {
      if (internal.begin(i) >= context.begin
              && internal.end(i) <= context.end) {
        cached.add(internal.begin(i), internal.end(i));
      }
    }
    docid = context.document;
    begin = context.begin;
    end = context.end;
  }

  @Override
  public void setContext(ScoringContext context) {
    if (!PassageScoringContext.class.isAssignableFrom(context.getClass())) {
      throw new RuntimeException("Trying to set a non-Passage-capable context as a PassageScoringContext");
    }
    context = (PassageScoringContext) context;
  }
}
