// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.parse;

import org.lemurproject.galago.core.parse.Tag;
import java.util.Collections;
import junit.framework.TestCase;

/**
 *
 * @author trevor
 */
public class TagTest extends TestCase {
    
    public TagTest(String testName) {
        super(testName);
    }

    public void testTag() {
        Tag t = new Tag("a", Collections.singletonMap("b", "c"), 0, 1);

        assertEquals("a", t.name);
        assertEquals(0, t.begin);
        assertEquals(1, t.end);
        assertTrue(t.attributes.containsKey("b"));
        assertEquals("c", t.attributes.get("b"));
        assertEquals(1, t.attributes.size());
    }

    public void testTrimTagAscii() {
        Tag t = new Tag("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                        "dcvbxhdhhggbsadkjfjehjhhdudhieuhyyipkjjhjehjwjdjdmnfjikkkklllkjl" +
                        "dcvbxhdhhggbsadkjfjehjhhdudhieuhyyipkjjhjehjwjdjdmnfjikkkklllkjl" +
                        "dcvbxhdhhggbsadkjfjehjhhdudhieuhyyipkjjhjehjwjdjdmnfjikkkklllkjl" +
                        "dcvbxhdhhggbsadkjfjehjhhdudhieuhyyipkjjhjehjwjdjdmnfjikkkklllkjl",
                        Collections.EMPTY_MAP, 0, 1);

        assertEquals(255, t.name.length());
    }

    public void testTrimTagUnicode() {
        Tag t = new Tag("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                        "aaaaaaaa\u229aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa" +
                        "dcvbxhdhhggbsadkjfjehjhhdudhieuhyyipkjjhjehjwjdjdmnfjikkkklllkjl" +
                        "dcvbxhdhhggbsadkjfjehjhhdudhieuhyyipkjjhjehjwjdjdmnfjikkkklllkjl" +
                        "dcvbxhdhhggbsadkjfjehjhhdudhieuhyyipkjjhjehjwjdjdmnfjikkkklllkjl" +
                        "dcvbxhdhhggbsadkjfjehjhhdudhieuhyyipkjjhjehjwjdjdmnfjikkkklllkjl",
                        Collections.EMPTY_MAP, 0, 1);

        assertEquals(253, t.name.length());
    }

    public void testToString() {
        Tag t = new Tag("a", Collections.singletonMap("b", "c"), 0, 1);
        assertEquals("<a b=\"c\">", t.toString());
    }
}
