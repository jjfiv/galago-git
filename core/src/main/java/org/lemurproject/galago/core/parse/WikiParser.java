/*
 * BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.parse;

import info.bliki.wiki.model.WikiModel;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.Parameters;
import org.w3c.dom.CharacterData;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.StringReader;
import java.util.HashSet;

/*
 *
 * @author irmarc
 */
public class WikiParser extends DocumentStreamParser {

  HashSet<String> specialPrefixWhiteList;
  BufferedReader reader;
  DocumentBuilder builder;
  WikiModel wikiParser;

  /**
   * Creates a new instance of TrecWebParser
   */
  public WikiParser(DocumentSplit split, Parameters p) throws IOException {
    super(split, p);
    this.reader = getBufferedReader(split);
    DocumentBuilderFactory builderFactory =
            DocumentBuilderFactory.newInstance();

    try {
      builder = builderFactory.newDocumentBuilder();
    } catch (ParserConfigurationException e) {
      throw new IOException("bliki configuration exception", e);
    }
    this.wikiParser = new WikiModel("http://en.wikipedia.org/wiki/${image}",
            "http://en.wikipedia.org/wiki/${title}");

    this.specialPrefixWhiteList = new HashSet<String>();
    this.specialPrefixWhiteList.add("Category:");
  }

  @Override
  public Document nextDocument() throws IOException {
    if (reader == null) {
      return null;
    }

    StringBuilder xmlPage = new StringBuilder();
    String line = reader.readLine();
    boolean inPage = false;

    while (line != null) {
      if (line.trim().startsWith("<page>")) {
        inPage = true;
        xmlPage.append(line).append("\n");
      } else if (line.trim().startsWith("</page>")) {
        xmlPage.append(line).append("\n");
        inPage = false;
        if (xmlPage.length() > 0) {
          Document d = processPage(xmlPage.toString());
          if (d != null) {
            return d;
          }
        }
        // clear the dud page
        xmlPage = new StringBuilder();

      } else if (inPage) {
        xmlPage.append(line).append("\n");
      }
      line = reader.readLine();
    }
    return null;
  }

  private String getFirstTagContents(org.w3c.dom.Document xmlDoc, String tagName) {
    NodeList idList = xmlDoc.getElementsByTagName(tagName);
    Element idElement = (Element) idList.item(0);
    return ((CharacterData) idElement.getFirstChild()).getData();
  }
  
  private Document processPage(String page) {
    Document d = null;
    String documentTitle = "";
    try {
      InputSource is = new InputSource();
      is.setCharacterStream(new StringReader(page));
      org.w3c.dom.Document xmlDoc = builder.parse(is);

      long wikiId = Integer.parseInt(getFirstTagContents(xmlDoc, "id"));
      String documentName = "w"+wikiId;
      documentTitle = getFirstTagContents(xmlDoc, "title");

      if (!checkTitle(documentTitle)) {
        return null;
      }

      String documentTimestamp = getFirstTagContents(xmlDoc, "timestamp");
      String text = getFirstTagContents(xmlDoc, "text");

      // prepend document with title and timestamp.
      StringBuilder documentText = new StringBuilder();
      documentText.append("<title>").append(documentTitle).append("</title>\n");
      documentText.append("<timestamp>").append(documentTimestamp).append("</timestamp>\n");

      if (text.length() > 0) {
        String htmlText = wikiParser.render(text);
        documentText.append(htmlText);
      }

      d = new Document(documentName, documentText.toString().toLowerCase());
      d.metadata.put("title", documentTitle);
      d.metadata.put("timestamp", documentTimestamp);
      d.metadata.put("url", "http://en.wikipedia.org/wiki/" + documentTitle);

    } catch (Exception ex) {
      System.err.println("FAILED TO PROCESS: " + documentTitle);// failed to parse document data from the page string - return null
      ex.printStackTrace();
    }
    return d;
  }

  private boolean checkTitle(String documentTitle) {
    if (documentTitle.contains(":")) {
      for (String prefix : this.specialPrefixWhiteList) {
        if (documentTitle.startsWith(prefix)) {
          return true;
        }
      }
      return false;
    }
    return true;
  }

  @Override
  public void close() throws IOException {
    this.reader.close();
  }
}
