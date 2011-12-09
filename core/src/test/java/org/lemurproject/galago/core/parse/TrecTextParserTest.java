// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.parse;

import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.TrecTextParser;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import junit.framework.TestCase;

/**
 *
 * @author trevor
 */
public class TrecTextParserTest extends TestCase {
    
    public TrecTextParserTest(String testName) {
        super(testName);
    }

    public void testParseNothing() throws IOException {
        String fileText = "";
        BufferedReader reader = new BufferedReader(new StringReader(fileText));
        TrecTextParser parser = new TrecTextParser(reader);

        Document document = parser.nextDocument();
        assertNull(document);
    }

    public void testParseOneDocument() throws IOException {
        String fileText =
                "<DOC>\n" +
                "<DOCNO>CACM-0001</DOCNO>\n" +
                "<TEXT>\n" +
                "This is some text in a document.\n" +
                "</TEXT>\n" +
                "</DOC>\n";
        BufferedReader reader = new BufferedReader(new StringReader(fileText));
        TrecTextParser parser = new TrecTextParser(reader);

        Document document = parser.nextDocument();
        assertNotNull(document);
        assertEquals("CACM-0001", document.name);
        assertEquals("<TEXT>\nThis is some text in a document.\n</TEXT>\n", document.text);

        document = parser.nextDocument();
        assertNull(document);
    }

    public void testParseTwoDocuments() throws IOException {
        String fileText =
                "<DOC>\n" +
                "<DOCNO>CACM-0001</DOCNO>\n" +
                "<TEXT>\n" +
                "This is some text in a document.\n" +
                "</TEXT>\n" +
                "</DOC>\n" +
                "<DOC>\n" +
                "<DOCNO>CACM-0002</DOCNO>\n" +
                "<TEXT>\n" +
                "This is some text in a document.\n" +
                "</TEXT>\n" +
                "</DOC>\n";
        BufferedReader reader = new BufferedReader(new StringReader(fileText));
        TrecTextParser parser = new TrecTextParser(reader);

        Document document = parser.nextDocument();
        assertNotNull(document);
        assertEquals("CACM-0001", document.name);
        assertEquals("<TEXT>\nThis is some text in a document.\n</TEXT>\n", document.text);

        document = parser.nextDocument();
        assertNotNull(document);
        assertEquals("CACM-0002", document.name);
        assertEquals("<TEXT>\nThis is some text in a document.\n</TEXT>\n", document.text);

        document = parser.nextDocument();
        assertNull(document);
    }
}
