// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.utility.Parameters;

import java.io.BufferedReader;
import java.io.IOException;

/**
 *
 * @author jdalton
 */
public class TacTextParser extends DocumentStreamParser {

  BufferedReader reader;

  public TacTextParser(DocumentSplit split, Parameters p)
          throws IOException {
    super(split, p);
    this.reader = getBufferedReader(split);
  }

  public String waitFor(String tag) throws IOException {
    String line;

    while ((line = reader.readLine()) != null) {
      if (line.startsWith(tag)) {
        return line;
      }
    }

    return null;
  }

  public String parseDocNumber() throws IOException {
    String allText = waitFor("<DOCID>");
    if (allText == null) {
      return null;
    }

    while (!allText.contains("</DOCID>")) {
      String line = reader.readLine();
      if (line == null) {
        break;
      }
      allText += line;
    }

    int start = allText.indexOf("<DOCID>") + 7;
    int end = allText.indexOf("</DOCID>");

    return allText.substring(start, end).trim();
  }

  public String parseSource() throws IOException {

    String start = "<DOCTYPE";
    String end = "</DOCTYPE>";
    String allText = waitFor(start);
    if (allText == null) {
      return null;
    }

    while (!allText.contains(end)) {
      String line = reader.readLine();
      if (line == null) {
        break;
      }
      allText += line;
    }

    int startOffset = allText.indexOf('>', allText.indexOf(start)) + 1;
    int endOffset = allText.indexOf(end);

    return allText.substring(startOffset, endOffset).trim();
  }

  @Override
  public Document nextDocument() throws IOException {
    String line;

    if (null == waitFor("<DOC>")) {
      return null;
    }
    String identifier = parseDocNumber();
    if (identifier == null) {
      return null;
    }

    String source = parseSource();

    StringBuilder buffer = new StringBuilder();

    String[] startTags = {"<TEXT>", "<HEADLINE>", "<BODY>", "<TITLE>", "<HL>", "<HEAD>",
      "<TTL>", "<DD>", "<DATE>", "<LP>", "<LEADPARA>"
    };
    String[] endTags = {"</TEXT>", "</BODY>", "</HEADLINE>", "</TITLE>", "</HL>", "</HEAD>",
      "</TTL>", "</DD>", "</DATE>", "</LP>", "</LEADPARA>"
    };

    int inTag = -1;

    while ((line = reader.readLine()) != null) {
      if (line.startsWith("</DOC>")) {
        break;
      }
      if (line.startsWith("<")) {
        if (inTag >= 0 && line.startsWith(endTags[inTag])) {
          inTag = -1;

          buffer.append(line);
          buffer.append('\n');
        } else if (inTag < 0) {
          for (int i = 0; i < startTags.length; i++) {
            if (line.startsWith(startTags[i])) {
              inTag = i;
              break;
            }
          }
        }
      }

      if (inTag >= 0) {
        buffer.append(line);
        buffer.append('\n');
      }
    }

    Document doc = new Document(identifier, buffer.toString());
    doc.metadata.put("doctype", source);
    return doc;
  }

  @Override
  public void close() throws IOException {
    this.reader.close();
  }
}
