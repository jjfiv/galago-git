// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author trevor, sjh
 */
public class TagTest{
  @Test
  public void testTag() {
    Tag t = new Tag("a", Collections.singletonMap("b", "c"), 0, 1);

    assertEquals("a", t.name);
    assertEquals(0, t.begin);
    assertEquals(1, t.end);
    assertTrue(t.attributes.containsKey("b"));
    assertEquals("c", t.attributes.get("b"));
    assertEquals(1, t.attributes.size());
  }

  @Test
  public void testToString() {
    Tag t = new Tag("a", Collections.singletonMap("b", "c"), 0, 1);
    assertEquals("<a [0-1] b=\"c\">", t.toString());
  }
}
