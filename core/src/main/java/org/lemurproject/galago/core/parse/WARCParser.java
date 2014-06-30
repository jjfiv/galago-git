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

import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.utility.Parameters;

import java.io.DataInputStream;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

    Document doc = new Document(record.getHeaderMetadataItem("WARC-TREC-ID"), record.getContentUTF8());
    doc.metadata = new HashMap<String,String>();
    for(Entry<String, String> entry : record.getHeaderMetadata()){
      doc.metadata.put(entry.getKey(), entry.getValue());
    }
    doc.metadata.put("url", record.getHeaderMetadataItem("WARC-Target-URI"));

    // extract html header from the document
    String text = doc.text;
    Matcher matcher = Pattern.compile("\n\n|\r\n\r\n").matcher(text);
    if(matcher.find(0)){
      int start = matcher.start();
      int end = matcher.end();
      
      String header = text.substring(0, end);
      String body = text.substring(end);
      
      doc.metadata.put("dochdr", header);
      doc.text = body;
    }
    
    return doc;
  }
  
  /**
   * Test function for parser.
   */
  public static void main(String[] args) throws IOException{
    if(args.length < 1){
      System.out.println("Usage: <filename.warc>");
      return;
    }
    
    File f = new File(args[0]);
    if(!f.isFile()){
      System.out.println("File does not exist");
      System.out.println("Usage: <filename.warc>");
      return;
    }

    DocumentSplit split = new DocumentSplit();
    split.fileName = f.getAbsolutePath();
    split.fileType = "warc";
    WARCParser parser = new WARCParser(split, Parameters.instance());
    Document d;
    while((d = parser.nextDocument()) != null){
      System.out.format( "NAME-:\n%s\n---\n", d.name );
      System.out.format( "META-DATA-:\n");
      for(String key : d.metadata.keySet()){
        System.out.format( "%s -: %s\n", key, d.metadata.get(key));
      }
      System.out.format( "TEXT-:\n%s\n---\n", d.text );
    }
  }
}
