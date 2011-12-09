// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import org.lemurproject.galago.core.parse.TagTokenizer;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Tag;
import org.lemurproject.galago.tupleflow.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.NullProcessor;
import java.io.IOException;
import junit.framework.*;

/**
 *
 * @author trevor
 */
public class TagTokenizerTest extends TestCase {
    public TagTokenizerTest(String testName) {
        super(testName);
    }

    /**
     * Test of reset method, of class galago.parse.TagTokenizer.
     */
    public void testReset() {
        TagTokenizer instance = new TagTokenizer();
        instance.reset();
    }

    /**
     * Test of tokenize method, of class galago.parse.TagTokenizer.
     */
    public void testTokenize() throws IOException, IncompatibleProcessorException {
        TagTokenizer tokenizer = new TagTokenizer();
        tokenizer.addField("title");
        tokenizer.addField("a");
	      tokenizer.addField("html");
        Document document = new Document();
        tokenizer.setProcessor(new NullProcessor(Document.class));

        document.text = "<html> <a href=\"http://www.yahoo.com\">Yahoo</a> this is some text " +
                "<title>title text</title> \n" +
                "That's all folks!" +
                "</html>" +
                "ciir.cs.umass.edu " +
                "m.b.a.";

        tokenizer.process(document);

        // first, check tokens
        String[] tokens = {"yahoo", "this", "is", "some", "text", "title", "text", "thats", "all", "folks", "ciir", "cs", "umass", "edu", "mba"};

        assertEquals("Token length", tokens.length, document.terms.size());
        for (int i = 0; i < tokens.length; i++) {
            assertEquals("Token text", tokens[i], document.terms.get(i));
        }

        // then, check tags
        assertEquals(3, document.tags.size());

        // html tag
        Tag html = document.tags.get(0);
        assertEquals(html.name, "html");
        assertEquals(html.attributes.size(), 0);
        assertEquals(html.begin, 0);
        assertEquals(html.end, 10);

        // a tag
        Tag a = document.tags.get(1);
        assertEquals(a.name, "a");
        assertEquals(a.attributes.size(), 1);
        assertEquals(a.attributes.get("href"), "http://www.yahoo.com");
        assertEquals(a.begin, 0);
        assertEquals(a.end, 1);

        // title tag
        Tag title = document.tags.get(2);
        assertEquals(title.name, "title");
        assertEquals(title.attributes.size(), 0);
        assertEquals(title.begin, 5);
        assertEquals(title.end, 7);
    }
}
