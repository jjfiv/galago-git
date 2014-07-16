// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.BufferedInputStream;
import java.io.IOException;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.utility.ByteUtil;
import org.lemurproject.galago.utility.Parameters;

/**
 * Parses ARC files, like those produced by the Heretrix web crawler.
 * @author trevor
 */
public class ArcParser extends DocumentStreamParser {

  BufferedInputStream stream;

  public ArcParser(DocumentSplit split, Parameters parameters) throws IOException {
    super(split, parameters);
    this.stream = getBufferedInputStream(split);
  }

  private String readLine() throws IOException {
    StringBuilder buffer = new StringBuilder();
    boolean seenNonNewline = false;

    do {
      int c = stream.read();

      if (c == -1) {
        break;
      }
      if (c == '\n') {
        if (seenNonNewline) {
          break;
        } else {
          continue;
        }
      }

      seenNonNewline = true;
      buffer.append((char) c);
    } while (true);

    return buffer.toString();
  }

  public Document nextDocument() throws IOException {

    // check if we have closed this document extractor
    if (stream == null) {
      return null;
    }

    // http://www.dmoz.org/robots.txt 207.200.81.154 20070312180115 text/plain 593

    // read the header line
    String header = readLine();
    String[] fields = header.split(" ");

    String url = fields[0];
    String ip = fields[1];
    String date = fields[2];
    String contentType = fields[3];
    long length = Long.parseLong(fields[4]);

    // read the full document text
    byte[] data = new byte[(int) length];
    stream.read(data);
    // get the training newline
    stream.read();
    String fullText = ByteUtil.toString(data);
    int headerEnd = findDoubleNewline(fullText);

    String serverHeader;
    String documentText;

    if (headerEnd == 0) {
      documentText = fullText;
      serverHeader = "";
    } else {
      serverHeader = fullText.substring(0, headerEnd);
      documentText = fullText.substring(headerEnd + 1);
    }

    Document result = new Document(new String(url), documentText);
    System.out.println(url);
    result.metadata.put("serverHeader", serverHeader);
    result.metadata.put("contentType", contentType);
    result.metadata.put("ip", ip);
    result.metadata.put("date", date);

    return result;
  }

  private int findDoubleNewline(final String fullText) {
    // scan the full text string looking for two '\n' chars in a row
    boolean lastNewline = false;
    int headerEnd = 0;
    for (int i = 0; i < fullText.length(); i++) {
      if (fullText.charAt(i) == '\n') {
        if (lastNewline) {
          headerEnd = i;
          break;
        }
        lastNewline = true;
      } else {
        lastNewline = false;
      }
    }
    return headerEnd;
  }

  @Override
  public void close() throws IOException {
    if (stream != null) {
      stream.close();
      stream = null;
    }
  }
}
