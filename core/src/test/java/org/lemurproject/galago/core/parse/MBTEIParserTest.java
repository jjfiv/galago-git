// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.parse;

import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.MBTEIBookParser;
import org.lemurproject.galago.core.parse.MBTEIPageParser;
import org.lemurproject.galago.core.types.DocumentSplit;
import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.IOException;
import junit.framework.TestCase;

/**
 * Inspired by the TrecTextParserTest.
 *
 * @author irmarc
 */
public class MBTEIParserTest extends TestCase {

    public MBTEIParserTest(String testName) {
        super(testName);
    }

    public void testBookParseNothing() throws IOException {
	String text = "";
	DocumentSplit split = new DocumentSplit();
	split.fileName = "test_book";
	BufferedInputStream bufferedStream = 
	    new BufferedInputStream(new ByteArrayInputStream(text.getBytes()));
	MBTEIBookParser parser = new MBTEIBookParser(split, bufferedStream);
	
	Document document = parser.nextDocument();
	assertNull(document);
    }

    public void testPageParseNothing() throws IOException {
	String text = "";
	DocumentSplit split = new DocumentSplit();
	split.fileName = "test_book";
	BufferedInputStream bufferedStream = 
	    new BufferedInputStream(new ByteArrayInputStream(text.getBytes()));
	MBTEIPageParser parser = new MBTEIPageParser(split, bufferedStream);
	
	Document document = parser.nextDocument();
	assertNull(document);
    }

    public void testBookParse() throws IOException {
	DocumentSplit split = new DocumentSplit();
	split.fileName = "testdoc";
	InputStream stream = new ByteArrayInputStream(teiDocument.getBytes());
	MBTEIBookParser parser = new MBTEIBookParser(split, stream);
	
	Document document = parser.nextDocument();
	String expected = "<TEI><metadata>" +	
	    "<identifier>testDocument</identifier>" +
	    "<collection>galago</collection>" +
	    "<title>test1</title>" + 
	    "<date>1916</date>" +
	    "<language>eng</language>" +
	    "</metadata>" +
	    "<text>The blue emerald , that ugly one </text></TEI>";
	assertEquals("testdoc", document.name);
	assertEquals(expected, document.text);
	assertTrue(document.metadata.containsKey("title"));
	assertEquals("test1", document.metadata.get("title"));
	assertTrue(document.metadata.containsKey("date"));
	assertEquals("1916", document.metadata.get("date"));
	assertTrue(document.metadata.containsKey("language"));
	assertEquals("eng", document.metadata.get("language"));
	document = parser.nextDocument();
	assertNull(document);
    }

    public void testIncompleteBookParse() throws IOException {
	DocumentSplit split = new DocumentSplit();
	split.fileName = "testdoc";
	InputStream stream = new ByteArrayInputStream(badTEIDocument.getBytes());
	MBTEIBookParser parser = new MBTEIBookParser(split, stream);
	
	Document document = parser.nextDocument();
	assertNull(document);
    }

    public void testEntityParse() throws IOException {
	DocumentSplit split = new DocumentSplit();
	split.fileName = "testdoc";
	InputStream stream = new ByteArrayInputStream(entityDocument.getBytes());
	MBTEIEntityParser parser = new MBTEIEntityParser(split, stream);
	
	Document document = parser.nextDocument();
	assertEquals("blue emerald", document.name);
	String expected = "<text>The blue emerald , that ugly one </text>";
    }

    public void testPageParse() throws IOException {
	DocumentSplit split = new DocumentSplit();
	split.fileName = "testdoc";
	InputStream stream = new ByteArrayInputStream(teiDocument.getBytes());
	MBTEIPageParser parser = new MBTEIPageParser(split, stream);
	
	Document document = parser.nextDocument();
	String expected = "<TEI><metadata>" +	
	    "<identifier>testDocument</identifier>" +
	    "<collection>galago</collection>" +
	    "<title>test1</title>" + 
	    "<date>1916</date>" +
	    "<language>eng</language>" +
	    "</metadata>" +
	    "<text>The blue emerald</text></TEI>";
	assertEquals("testdoc_2", document.name);
	assertEquals(expected, document.text);

	document = parser.nextDocument();
	expected = "<TEI><metadata>" +	
	    "<identifier>testDocument</identifier>" +
	    "<collection>galago</collection>" +
	    "<title>test1</title>" + 
	    "<date>1916</date>" +
	    "<language>eng</language>" +
	    "</metadata>" +
	    "<text>, that ugly one</text></TEI>";
	assertEquals("testdoc_3", document.name);
	assertEquals(expected, document.text);
	assertTrue(document.metadata.containsKey("title"));
	assertEquals("test1", document.metadata.get("title"));
	assertTrue(document.metadata.containsKey("date"));
	assertEquals("1916", document.metadata.get("date"));
	assertTrue(document.metadata.containsKey("language"));
	assertEquals("eng", document.metadata.get("language"));
	document = parser.nextDocument();
	assertNull(document);       
    }

    public void testIncompletePageParse() throws IOException {
	DocumentSplit split = new DocumentSplit();
	split.fileName = "testdoc";
	InputStream stream = new ByteArrayInputStream(badTEIDocument.getBytes());
	MBTEIPageParser parser = new MBTEIPageParser(split, stream);
	
	Document document = parser.nextDocument();
	String expected = "<TEI><metadata>" +	
	    "<identifier>testDocument</identifier>" +
	    "<collection>galago</collection>" +
	    "<title>test1</title>" + 
	    "<date>1916</date>" +
	    "<language>eng</language>" +
	    "</metadata>" +
	    "<text>The blue emerald</text></TEI>";
	assertEquals("testdoc_2", document.name);
	assertEquals(expected, document.text);
	assertTrue(document.metadata.containsKey("title"));
	assertEquals("test1", document.metadata.get("title"));
	assertTrue(document.metadata.containsKey("date"));
	assertEquals("1916", document.metadata.get("date"));
	assertTrue(document.metadata.containsKey("language"));
	assertEquals("eng", document.metadata.get("language"));

	document = parser.nextDocument();
	expected = "<TEI><metadata>" +	
	    "<identifier>testDocument</identifier>" +
	    "<collection>galago</collection>" +
	    "<title>test1</title>" + 
	    "<date>1916</date>" +
	    "<language>eng</language>" +
	    "</metadata>" +
	    "<text>, that ugly one</text></TEI>";
	assertEquals("testdoc_3", document.name);
	assertEquals(expected, document.text);
	assertTrue(document.metadata.containsKey("title"));
	assertEquals("test1", document.metadata.get("title"));
	assertTrue(document.metadata.containsKey("date"));
	assertEquals("1916", document.metadata.get("date"));
	assertTrue(document.metadata.containsKey("language"));
	assertEquals("eng", document.metadata.get("language"));
	document = parser.nextDocument();
	assertNull(document);       
    }

    public static String teiDocument = 
	"<?xml version=\"1.0\" encoding=\"UTF-8\"?><TEI><metadata><identifier>testDocument</identifier><collection>galago</collection><title>test1</title><date>1916</date><language>eng</language></metadata><text lang=\"eng\"><pb n=\"1\" /><cb /><pb n=\"2\" /><w form=\"The\" deprel=\"ROOT\">The</w><w form=\"blue\">blue</w><w form=\"emerald\">emerald</w><pb n=\"3\"><w form=\",\">,</w><w form=\"that\" deprel=\"ROOT\">that</w><w form=\"ugly\">ugly</w><w form=\"one\">one</w></pb><pb n=\"4\"></pb></text></TEI>";

    public static String entityDocument = 
	"<?xml version=\"1.0\" encoding=\"UTF-8\"?><TEI><metadata><identifier>testDocument</identifier><collection>galago</collection><title>test1</title><date>1916</date><language>eng</language></metadata><text lang=\"eng\"><pb n=\"1\" /><cb /><pb n=\"2\" /><w form=\"The\" deprel=\"ROOT\">The</w><name type=\"object\"><w form=\"blue\">blue</w><w form=\"emerald\">emerald</w></name><pb n=\"3\"><w form=\",\">,</w><w form=\"that\" deprel=\"ROOT\">that</w><w form=\"ugly\">ugly</w><w form=\"one\">one</w></pb><pb n=\"4\"></pb></text></TEI>";

    public static String badTEIDocument = 
	"<?xml version=\"1.0\" encoding=\"UTF-8\"?><TEI><metadata><identifier>testDocument</identifier><collection>galago</collection><title>test1</title><date>1916</date><language>eng</language></metadata><text lang=\"eng\"><pb n=\"1\" /><cb /><pb n=\"2\" /><w form=\"The\" deprel=\"ROOT\">The</w><w form=\"blue\">blue</w><w form=\"emerald\">emerald</w><pb n=\"3\"><w form=\",\">,</w><w form=\"that\" deprel=\"ROOT\">that</w><w form=\"ugly\">ugly</w><w form=\"one\">one</w></pb><pb n=\"4\"></pb><pb n=\"4\"><w coords=\"0234\" form=\"ha";
}