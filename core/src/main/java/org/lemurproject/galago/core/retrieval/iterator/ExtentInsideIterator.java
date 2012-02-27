// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 * <p>Implements the #inside operator.  The #inside operator is usually implicit
 * in the query language, where <tt>a.b</tt> is equivalent to <tt>#inside(a b)</tt>.
 * This is usually used to find terms that occur in fields.  For example,
 * <tt>#1(bruce croft).author</tt>, which finds instances of "bruce croft" occurring
 * in the author field of a identifier.</p>
 *
 * @author trevor
 */
public class ExtentInsideIterator extends MovableExtentConjunctionIterator {

  ExtentValueIterator innerIterator;
  ExtentValueIterator outerIterator;

  /**
   * <p>Constructs an #inside instance.  For <tt>#inside(a b)</tt>, this
   * produces an extent whenever <tt>a</tt> is found inside <tt>b</tt>.</p>
   *
   * <p>For example, in the expression <tt>#inside(#1(white house) #extents:title())</tt>,
   * <tt>#1(white house)</tt> is the inner iterator and <tt>#extents:title()</tt>
   * is the outer iterator.  Whenever <tt>#1(white house)</tt> is found in the title of
   * a identifier, this is a match.  The extent for <tt>#1(white house)</tt> is returned
   * (not the extent for <tt>#extents:title()</tt> that surrounds it).</tt>
   *
   * @param parameters extra parameters, not used for anything.
   * @param innerIterator The source of extents that must be inside.
   * @param outerIterator The source of extents that must contain the inner extents.
   * @throws java.io.IOException
   */
  public ExtentInsideIterator(Parameters globalParams, NodeParameters parameters,
          ExtentValueIterator innerIterator,
          ExtentValueIterator outerIterator) throws IOException {
    super(new ExtentValueIterator[]{innerIterator, outerIterator});
    this.innerIterator = innerIterator;
    this.outerIterator = outerIterator;
    // load the first document
    moveTo(0);
  }

  /**
   * This method is called whenever the ExtentConjunctionIterator has verified
   * that both the inner and outer iterators match this identifier.  This method's job
   * is to find all matching extents within the identifier, if they exist.
   */
  @Override
  public void loadExtents() {
    
    int document = currentCandidate();
    
    if(innerIterator.isDone() || ! innerIterator.atCandidate(document)
            || outerIterator.isDone() || ! outerIterator.atCandidate(document)){
      // then we can't have any extents for this document
      return;
    }
    
    ExtentArrayIterator inner = new ExtentArrayIterator(innerIterator.extents());
    ExtentArrayIterator outer = new ExtentArrayIterator(outerIterator.extents());

    extents.setDocument(document);
    while (!inner.isDone() && !outer.isDone()) {
      if (outer.currentlyContains(inner)) {
        extents.add(inner.currentBegin(), inner.currentEnd());
        inner.next();
      } else if (outer.currentEnd() <= inner.currentBegin()) {
        outer.next();
      } else {
        inner.next();
      }
    }
  }
}
