// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator;

import java.io.IOException;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;

/**
 * <p>Implements the #inside operator. The #inside operator is usually implicit
 * in the query language, where <tt>a.b</tt> is equivalent to <tt>#inside(a
 * b)</tt>. This is usually used to find terms that occur in fields. For
 * example, <tt>#1(bruce croft).author</tt>, which finds instances of "bruce
 * croft" occurring in the author field of a identifier.</p>
 *
 * @author trevor
 */
public class ExtentInsideIterator extends ExtentConjunctionIterator {

  ExtentIterator innerIterator;
  ExtentIterator outerIterator;
  private ScoringContext cachedContext = null;

  /**
   * <p>Constructs an #inside instance. For <tt>#inside(a b)</tt>, this produces
   * an extent whenever <tt>a</tt> is found inside <tt>b</tt>.</p>
   *
   * <p>For example, in the expression <tt>#inside(#1(white house)
   * #extentCache:title())</tt>, <tt>#1(white house)</tt> is the inner iterator and
   * <tt>#extentCache:title()</tt> is the outer iterator. Whenever <tt>#1(white
   * house)</tt> is found in the title of a identifier, this is a match. The
   * extent for <tt>#1(white house)</tt> is returned (not the extent for
   * <tt>#extentCache:title()</tt> that surrounds it).</tt>
   *
   * @param parameters extra parameters, not used for anything.
   * @param innerIterator The source of extentCache that must be inside.
   * @param outerIterator The source of extentCache that must contain the inner
   * extentCache.
   * @throws java.io.IOException
   */
  public ExtentInsideIterator(NodeParameters parameters,
          ExtentIterator innerIterator,
          ExtentIterator outerIterator) throws IOException {
    super(parameters, new ExtentIterator[]{innerIterator, outerIterator});
    this.innerIterator = innerIterator;
    this.outerIterator = outerIterator;
    // load the first document
    syncTo(0);
  }

  /**
   * This method is called whenever the ExtentConjunctionIterator has verified
   * that both the inner and outer iterators match this identifier. This
   * method's job is to find all matching extentCache within the identifier, if they
   * exist.
   */
  @Override
  public void loadExtents(ScoringContext c) {
    // get the document
    long document = c.document;

    if (c.cachable && c.equals(cachedContext)) {
      assert (this.extentCache.getDocument() == c.document);
      return; // we already have it computed
    }
    // set current context as cached
    if (cachedContext == null) cachedContext = c.getPrototype();
    else cachedContext.setFrom(c);


    // reset the extentCache
    extentCache.reset();
    extentCache.setDocument(document);

    // if we're done - quit now 
    //  -- (leaving extentCache object empty just in cast someone asks for them.)
    if (isDone()) {
      return;
    }

    if (innerIterator.isDone() || !innerIterator.hasMatch(document)
            || outerIterator.isDone() || !outerIterator.hasMatch(document)) {
      // then we can't have any extentCache for this document
      return;
    }

    ExtentArrayIterator inner = new ExtentArrayIterator(innerIterator.extents(c));
    ExtentArrayIterator outer = new ExtentArrayIterator(outerIterator.extents(c));

    while (!inner.isDone() && !outer.isDone()) {
      if (outer.currentlyContains(inner)) {
        extentCache.add(inner.currentBegin(), inner.currentEnd());
        inner.next();
      } else if (outer.currentEnd() <= inner.currentBegin()) {
        outer.next();
      } else {
        inner.next();
      }
    }
  }
}
