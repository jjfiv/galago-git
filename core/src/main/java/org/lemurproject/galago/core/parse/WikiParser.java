/*
 * BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.parse;

import info.bliki.wiki.model.WikiModel;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashSet;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import org.w3c.dom.CharacterData;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;
import org.xml.sax.InputSource;

/*
 *
 * @author irmarc
 */
public class WikiParser implements DocumentStreamParser {

  HashSet<String> specialPrefixWhiteList;
  BufferedReader reader;
  DocumentBuilder builder;
  WikiModel wikiParser;

  /** Creates a new instance of TrecWebParser */
  public WikiParser(BufferedReader reader) throws FileNotFoundException, IOException {
    this.reader = reader;
    DocumentBuilderFactory builderFactory =
            DocumentBuilderFactory.newInstance();

    try {
      builder = builderFactory.newDocumentBuilder();
    } catch (ParserConfigurationException e) {
      e.printStackTrace();
    }
    this.wikiParser = new WikiModel("http://en.wikipedia.org/wiki/${image}",
            "http://en.wikipedia.org/wiki/${title}");
    
    this.specialPrefixWhiteList = new HashSet();
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

  private Document processPage(String page) {
    Document d = null;
    String documentTitle = "";
    try {
      InputSource is = new InputSource();
      is.setCharacterStream(new StringReader(page));
      org.w3c.dom.Document xmlDoc = builder.parse(is);

      // Highlander principle parsing - id!
      NodeList idList = xmlDoc.getElementsByTagName("id");
      Element idElement = (Element) idList.item(0);
      String documentId = ((CharacterData) idElement.getFirstChild()).getData();

      // Highlander principle parsing - title!
      NodeList titleList = xmlDoc.getElementsByTagName("title");
      Element titleElement = (Element) titleList.item(0);
      documentTitle = ((CharacterData) titleElement.getFirstChild()).getData();

      if(!checkTitle(documentTitle)){
        return null;
      }
      
      // Highlander principle parsing - timestamp!
      NodeList timeList = xmlDoc.getElementsByTagName("timestamp");
      Element timeElement = (Element) timeList.item(0);
      String documentTimestamp = ((CharacterData) timeElement.getFirstChild()).getData();

      // Highlander principle parsing - text!
      NodeList textList = xmlDoc.getElementsByTagName("text");
      Element textElement = (Element) textList.item(0);
      String text = ((CharacterData) textElement.getFirstChild()).getData();

      // prepend document with title and timestamp.
      StringBuilder documentText = new StringBuilder();
      documentText.append("<title>").append(documentTitle).append("</title>\n");
      documentText.append("<timestamp>").append(documentTimestamp).append("</timestamp>\n");

      if (text.length() > 0) {
        String htmlText = wikiParser.render(text);
        documentText.append(htmlText);
      }

      d = new Document(documentId, documentText.toString());

    } catch (Exception ex) {
      System.err.println("FAILED TO PROCESS: " + documentTitle);// failed to parse document data from the page string - return null
    }
    return d;
  }

  private boolean checkTitle(String documentTitle) {
    if(documentTitle.contains(":")){
      for(String prefix : this.specialPrefixWhiteList){
        if(documentTitle.startsWith(prefix)){
          return true;
        }
      }
      return false;
    }
    return true;
  }
}
