// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.retrieval.extents;

import org.junit.Test;
import org.lemurproject.galago.core.retrieval.iterator.ExtentArrayIterator;
import org.lemurproject.galago.core.util.ExtentArray;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author trevor
 */
public class ExtentArrayIteratorTest {
  @Test
  public void testEmpty() {
    ExtentArrayIterator instance = new ExtentArrayIterator( new ExtentArray() );
    assertTrue( instance.isDone() );
  }

  @Test
  public void testSingleExtent() {
    ExtentArray array = new ExtentArray();
    array.setDocument(1);
    array.add( 5, 7 );

    ExtentArrayIterator instance = new ExtentArrayIterator( array );
    assertFalse( instance.isDone() );
    assertEquals( instance.getDocument(), 1 );
    assertEquals( instance.currentBegin(), 5 );
    assertEquals( instance.currentEnd(), 7 );

    instance.next();
    assertTrue( instance.isDone() );
  }

  @Test
  public void testTwoExtents() {
    ExtentArray array = new ExtentArray();
    array.setDocument(1);
    array.add( 5, 7 );
    array.add( 9, 11 );

    ExtentArrayIterator instance = new ExtentArrayIterator( array );
    assertFalse( instance.isDone() );
    assertEquals( instance.getDocument(), 1 );
    assertEquals( instance.currentBegin(), 5 );
    assertEquals( instance.currentEnd(), 7 );

    instance.next();
    assertFalse( instance.isDone() );
    assertEquals( instance.getDocument(), 1 );
    assertEquals( instance.currentBegin(), 9 );
    assertEquals( instance.currentEnd(), 11 );

    instance.next();
    assertTrue( instance.isDone() );
  }
}
