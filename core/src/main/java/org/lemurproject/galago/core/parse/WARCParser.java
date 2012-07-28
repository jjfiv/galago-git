// BSD License (http://lemurproject.org/galago-license)
/*
 * WARC record parser
 * 
 * Originally written by:
 *   mhoy@cs.cmu.edu (Mark J. Hoy)
 * 
 * Modified for Galagosearch by:
 *   sjh
 */
package org.lemurproject.galago.core.parse;

import java.io.DataInputStream;
import java.io.IOException;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.Parameters;

public class WARCParser extends DocumentStreamParser {

  private DataInputStream reader = null;
  private WARCRecord fileHeader = null;
  private long recordCount = 0;
  private long totalNumBytesRead = 0;

  public WARCParser(DocumentSplit split, Parameters p) throws IOException {
    super(split, p);
    reader = new DataInputStream(getBufferedInputStream(split));
    fileHeader = WARCRecord.readNextWarcRecord(reader);
  }

  public void close() throws IOException {
    if (reader != null) {
      reader.close();
      reader = null;
    }
  }

  public Document nextDocument() throws IOException {

    WARCRecord record = WARCRecord.readNextWarcRecord(reader);

    if (record == null) {
      return null;
    }

    totalNumBytesRead += (long) record.getTotalRecordLength();

    Document doc = new Document(record.getDocid(), record.getContent());
    doc.metadata = record.warcHeader.metadata;
    doc.metadata.put("url", record.getHeaderMetadataItem("WARC-Target-URI"));

    return doc;
  }
}
