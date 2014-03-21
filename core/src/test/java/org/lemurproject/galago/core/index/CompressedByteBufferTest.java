/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author trevor
 */
public class CompressedByteBufferTest {
  @Test
  public void testAdd() throws Exception {
    CompressedByteBuffer instance = new CompressedByteBuffer();
    instance.add( 5 );
    instance.add( 10 );
    instance.add( 200 );
    instance.add( 400 );

    byte[] result = new byte[] { (byte) (5 | 1<<7),
        (byte) (10 | 1<<7),
        72,
        (byte) (1 | 1<<7),
        16,
        (byte) (3 | 1<<7) };

    assertEquals( result.length, instance.length() );
    for( int i=0; i<result.length; i++ ) {
      assertEquals( result[i], instance.getBytes()[i] );
    }
  }

  @Test
  public void testAddRaw() throws Exception {
    CompressedByteBuffer instance = new CompressedByteBuffer();
    instance.addRaw(5);
    instance.addRaw(6);

    assertEquals( 2, instance.length() );
    assertTrue( instance.getBytes().length >= 2 );
    assertEquals( 5, instance.getBytes()[0] );
    assertEquals( 6, instance.getBytes()[1] );
  }
}
