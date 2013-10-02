// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.tokenize;

import junit.framework.TestCase;
import org.lemurproject.galago.core.parse.Document;

/**
 *
 * @author jfoley
 */
public class NewlineTokenizerTest extends TestCase {
  public NewlineTokenizerTest(String testName) {
    super(testName);
  }
  
  public void testSimple() {
    NewlineTokenizer tok = new NewlineTokenizer();
    Document ridiculous = tok.tokenize("this\nIs\tnot\na\nCAPitaliZATION\nunder_scored\n14.95.dots\ntest");
    String[] expected = {"this", "Is\tnot", "a", "CAPitaliZATION", "under_scored", "14.95.dots", "test"};
    assertEquals(ridiculous.terms.size(), expected.length);
    for(int i=0; i<expected.length; i++) {
      assertEquals(expected[i], ridiculous.terms.get(i));
    }
  }
}
