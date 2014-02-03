// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.tokenize;

import java.io.IOException;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import org.junit.Test;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.tupleflow.Parameters;

/**
 *
 * @author jfoley
 */
public class NewlineTokenizerTest {
	@Test
  public void testSimple() {
    NewlineTokenizer tok = new NewlineTokenizer();
    Document ridiculous = tok.tokenize("this\nIs\tnot\na\nCAPitaliZATION\nunder_scored\n14.95.dots\ntest");
    String[] expected = {"this", "Is\tnot", "a", "CAPitaliZATION", "under_scored", "14.95.dots", "test"};
    assertEquals(ridiculous.terms.size(), expected.length);
    for(int i=0; i<expected.length; i++) {
      assertEquals(expected[i], ridiculous.terms.get(i));
    }
  }
	
	@Test
	public void testFromParameters() throws IOException {
		Parameters tokenizerParms = Parameters.parseString("{\"tokenizer\": { \"tokenizerClass\": \""+NewlineTokenizer.class.getCanonicalName()+"\" } }");
		Tokenizer tok = Tokenizer.instance(tokenizerParms);
		assertTrue(tok instanceof NewlineTokenizer);
	}
}
