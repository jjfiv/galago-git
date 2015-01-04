// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import org.junit.Test;
import org.lemurproject.galago.tupleflow.error.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.runtime.NullProcessor;

import java.io.IOException;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author trevor
 */
public class TagTokenizerTest {

    /**
     * Test of reset method, of class galago.parse.TagTokenizer. todo this is a
     * terrible "test"
     */
    @Test
    public void testReset() {
        TagTokenizer instance = new TagTokenizer();
        instance.reset();
    }

    /**
     * Test of tokenize method, of class galago.parse.TagTokenizer.
     */
    @Test
    public void testTokenize() throws IOException, IncompatibleProcessorException {
        TagTokenizer tokenizer = new TagTokenizer();
        tokenizer.addField("title");
        tokenizer.addField("a");
        tokenizer.addField("html");
        Document document = new Document();
        tokenizer.setProcessor(new NullProcessor(Document.class));

        document.text = "<html> <a href=\"http://www.yahoo.com\">Yahoo</a> this is some text "
                + "<title>title text</title> \n"
                + "That's all folks!"
                + "</html>"
                + "ciir.cs.umass.edu "
                + "m.b.a. "
                + "17.2";

        tokenizer.process(document);

        // first, check tokens
        String[] tokens = {"yahoo", "this", "is", "some", "text", "title", "text", "thats", "all", "folks", "ciir", "cs", "umass", "edu", "mba", "17", "2"};

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

    @Test
    public void testDoNotTokenizeAttributes() throws IncompatibleProcessorException, IOException {

        TagTokenizer tokenizer = new TagTokenizer();
        tokenizer.addField("title");
        tokenizer.addField("category");
        tokenizer.addField("anchor");
        Document document = new Document();
        tokenizer.setProcessor(new NullProcessor(Document.class));

        document.text = "<document> "
                + "<title>Aberdeen</title> \n"
                + "<category tokenizeTagContent=\"false\">/location/citytown</category>\n"
                + "<category tokenizeTagContent=\"false\">/base/scotland/topic</category>\n"
                + "<anchor tokenizeTagContent=\"false\">Aberdeen Angus</anchor>\n"
                + "</html>";

        tokenizer.process(document);

        // first, check tokens
        String[] tokens = {"aberdeen", "/location/citytown", "/base/scotland/topic", "aberdeen angus"};

        assertEquals("Token length", tokens.length, document.terms.size());
        for (int i = 0; i < tokens.length; i++) {
            assertEquals("Token text", tokens[i], document.terms.get(i));
        }

        // then, check tags
        assertEquals(4, document.tags.size());

    }
}
