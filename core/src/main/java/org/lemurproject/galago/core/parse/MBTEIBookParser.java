// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import javax.xml.stream.XMLInputFactory;
import javax.xml.stream.XMLStreamConstants;
import javax.xml.stream.XMLStreamException;
import javax.xml.stream.util.StreamReaderDelegate;
import org.lemurproject.galago.core.types.DocumentSplit;

/**
 * Produces page-level postings from books in MBTEI format. Pages with no text
 * are not emitted as documents, and the header is prepended to every emitted
 * Document object. Each document is emitted as an XML file, but only the header
 * retains tags.
 *
 * Otherwise the page text is just in a "<text>" element as one large span of
 * text.
 *
 * @author irmarc
 */
class MBTEIParser implements DocumentStreamParser {

  // External/global switch to flip for different level parsing.
  public static String splitTag;
  // For XML stream processing
  StreamReaderDelegate reader;
  XMLInputFactory factory;
  String headerdata;
  String bookIdentifier;
  int pagenumber;

  public MBTEIParser(DocumentSplit split, InputStream is) {

    // XML processing
    try {
      factory = XMLInputFactory.newInstance();
      factory.setProperty(XMLInputFactory.IS_COALESCING, true);
      reader = new StreamReaderDelegate(factory.createXMLStreamReader(is));
      bookIdentifier = getIdentifier(split);
      pagenumber = 0;
      readHeader();
    } catch (Exception e) {
      System.err.printf("SKIPPING %s: Caught exception %s\n", split.fileName, e.getMessage());
      reader = null;
    }
  }

  @Override
  public Document nextDocument() throws IOException {
    if (reader == null) {
      return null;
    }

    StringBuilder builder = new StringBuilder();
    int status = 0;
    Document d;
    try {
      while (reader.hasNext()) {
        status = reader.next();
        if (status == XMLStreamConstants.START_ELEMENT && reader.getLocalName().equals(splitTag)) {
          d = buildDocument(builder);
          pagenumber = Integer.parseInt(reader.getAttributeValue(null, "n"));
          if (d != null) {
            // Have a legitimate document built - send it up.
            return d;
          }
          // Otherwise, keep going, because we didn't emit a document
        }

        // if it's text, add it to the buffer
        if (status == XMLStreamConstants.CHARACTERS) {
          builder.append(scrub(reader.getText())).append(" ");
        }
      }

      // All done - either emitting or returning nothing
      d = buildDocument(builder);
      return d;
    } catch (Exception e) {
      System.err.printf("EXCEPTION [%s, %d]: %s\n", bookIdentifier, pagenumber, e.getMessage());
      return null;
    }
  }

  public String scrub(String dirty) {
    String cleaned = dirty.replaceAll("&apos;", "'");
    cleaned = cleaned.replaceAll("&quot;", "\"");
    cleaned = cleaned.replaceAll("&amp;", "&");
    return cleaned;
  }

  private Document buildDocument(StringBuilder builder) {
    if (builder.length() == 0) {
      // we stopped because there are no more tokens
      // if the builder is empty, then we read nothing useful.
      return null;
    } else {
      // We got something - let's emit it
      StringBuilder content = new StringBuilder(headerdata);
      content.append(builder);
      content.append("</text></TEI>");
      String pageIdentifier = String.format("%s_%d", bookIdentifier, pagenumber);
      return new Document(pageIdentifier, content.toString());
    }
  }

  public void readHeader() throws IOException {
    boolean stop = false;
    StringBuilder builder = new StringBuilder();
    int status;
    String previousStart = "";
    try {

      while (!stop && reader.hasNext()) {
        status = reader.next();

        // Emit element starts, text, and element ends
        switch (status) {
          case XMLStreamConstants.CHARACTERS:
            builder.append(reader.getText());
            break;
          case XMLStreamConstants.START_ELEMENT:
            builder.append("<").append(reader.getLocalName()).append(">");
            break;
          case XMLStreamConstants.END_ELEMENT:
            builder.append("</").append(reader.getLocalName()).append(">");
            break;
        }

        if (status == XMLStreamConstants.START_ELEMENT) {
          previousStart = reader.getLocalName();
          // Do we need to stop? DO WE EVEN KNOW HOW TO STOP?
          if (reader.getLocalName().equals("text")) {
            stop = true;
          }
        }
      }
    } catch (Exception e) {
      throw new IOException(String.format("While scanning header of split %s", bookIdentifier), e);
    }
    headerdata = builder.toString();
  }

  public String getIdentifier(DocumentSplit split) {
    File f = new File(split.fileName);
    String basename = f.getName();
    String[] parts = basename.split("_");
    return parts[0];
  }

  @Override
  public void close() throws IOException {
    try {
      reader.close();
    } catch (XMLStreamException ex) {
      System.err.printf("EXCEPTION CLOSING [%s, %d]: %s\n", bookIdentifier, pagenumber, ex.getMessage());
    }
  }
}
