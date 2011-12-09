// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.parse;

import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.TrecWebParser;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import junit.framework.TestCase;

/**
 *
 * @author trevor
 */
public class TrecWebParserTest extends TestCase {
    
    public TrecWebParserTest(String testName) {
        super(testName);
    }


    public void testParseNothing() throws IOException {
        String fileText = "";
        BufferedReader reader = new BufferedReader(new StringReader(fileText));
        TrecWebParser parser = new TrecWebParser(reader);

        Document document = parser.nextDocument();
        assertNull(document);
    }

    public void testParseOneDocument() throws IOException {
        String fileText =
                "<DOC>\n" +
                "<DOCNO>CACM-0001</DOCNO>\n" +
                "<DOCHDR>\n" +
                "http://www.yahoo.com:80 some extra text here\n" +
                "even more text in this part\n" +
                "</DOCHDR>\n" +
                "This is some text in a document.\n" +
                "</DOC>\n";
        BufferedReader reader = new BufferedReader(new StringReader(fileText));
        TrecWebParser parser = new TrecWebParser(reader);

        Document document = parser.nextDocument();
        assertNotNull(document);
        assertEquals("CACM-0001", document.name);
        assertEquals("http://www.yahoo.com", document.metadata.get("url"));
        assertEquals("This is some text in a document.\n", document.text);

        document = parser.nextDocument();
        assertNull(document);
    }

}
