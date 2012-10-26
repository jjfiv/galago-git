package org.lemurproject.galago.contrib.parse;

import java.io.DataInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;

import kba.StreamItem;

import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TTransportException;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.DocumentStreamParser;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.Parameters;

public class TrecKBAParser extends DocumentStreamParser {

  private DataInputStream reader = null;
  private TProtocol tp;
  private Date date;
  //TDeserializer deserializer = new TDeserializer(new TBinaryProtocol.Factory());
  private InputStream stream = null;
  private String sourceFile = null;
  private String contentType = null;
  private long numSkipped = 0;
  private long totalRecords = 0;

  public TrecKBAParser(DocumentSplit split, Parameters p)
          throws FileNotFoundException, IOException {
    super(split, p);
    reader = new DataInputStream(getBufferedInputStream(split));
    tp = new TBinaryProtocol.Factory().getProtocol(new TIOStreamTransport(stream));
    sourceFile = split.fileName;
    String fileName = sourceFile.substring(sourceFile.lastIndexOf('/') + 1);
    String type = fileName.substring(0, fileName.indexOf('.'));
    contentType = type;
    String dateString = getIdentifier(split);
    SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd-HH");
    try {
      date = simpleDateFormat.parse(dateString);
    } catch (ParseException e) {
      throw new IllegalArgumentException("Error parsing date for directory: " + dateString);
    }
    this.stream = stream;
  }

  private String getIdentifier(DocumentSplit split) {
    String filename = split.fileName;
    String dirDate = filename.substring(
            filename.lastIndexOf('/', filename.lastIndexOf('/') - 1) + 1,
            filename.lastIndexOf('/'));
    return dirDate;
  }

  public void close() throws IOException {
    System.out.println("skipped: " + numSkipped + " out of: " + totalRecords);
    reader.close();
    reader = null;
  }

  @Override
  public Document nextDocument() throws IOException {

    StreamItem item = new StreamItem();
    while (true) {
      try {
        item.read(tp);
        totalRecords++;
        String streamId = item.getStream_id();
        // String title = new String(item.title.getCleansed());
        String encoding = item.getBody().encoding;

        if (encoding == null || encoding.length() == 0) {
          encoding = "UTF-8";
        } else if (encoding.equals("8859-1")) {
          encoding = "ISO-8859-1";
        }

        // NOTE: for 2012 we only index documents with cleansed text.
        // These were the only ones judged by the assessors.
        String bodyText = null;
        byte[] cleansedTextBytes = item.getBody().getCleansed();
        if (cleansedTextBytes == null) {
          //System.out.println("Skipping doc without cleansed text." + streamId);
          numSkipped++;
          continue;
        } else {
          try {
            bodyText = new String(cleansedTextBytes, encoding);
          } catch (UnsupportedEncodingException e) {
            bodyText = new String(cleansedTextBytes, "UTF-8");
            encoding = "UTF-8";
          }
        }

        if (bodyText.length() == 0) {
          //System.out.println("Skipping doc without cleansed text." + streamId);
          numSkipped++;
          continue;
        }


        String rawHtml = new String(item.getBody().getRaw(), encoding);

        String nerData = null;
        try {
          nerData = new String(item.getBody().getNer(), encoding);
        } catch (UnsupportedEncodingException e) {
          nerData = new String(item.getBody().getNer(), "UTF-8");
          encoding = "UTF-8";
        } catch (Exception e) {
          nerData = "";
        }

        String srcMetadata = new String(item.source_metadata.array());
        StringBuilder content = new StringBuilder();
        content.append("<kbadate>");
        content.append(Long.toString(date.getTime()));
        content.append("</kbadate>");
        content.append("<kbastreamticks>");
        content.append(Long.toString((long) item.getStream_time().getEpoch_ticks()));
        content.append("</kbastreamticks>");
        content.append("<kbastreamtimestamp>");
        content.append(item.getStream_time().getZulu_timestamp() + "");
        content.append("</kbastreamtimestamp>");
        content.append("<kbatype>");
        content.append(contentType);
        content.append("</kbatype>");
        content.append(rawHtml);


        Document res = new Document(streamId, content.toString());


        String absUrl = new String(item.abs_url.array());
        //String originalUrl = new String(item.original_url.array());
        res.metadata.put("absoluteUrl", absUrl);
        res.metadata.put("docId", item.doc_id);
        res.metadata.put("metadata", srcMetadata);
        res.metadata.put("epochTicks", item.getStream_time().getEpoch_ticks() + "");
        res.metadata.put("zulu_timestamp", item.getStream_time().getZulu_timestamp());
        res.metadata.put("date", date.toString());
        res.metadata.put("nerData", nerData);
        res.metadata.put("sourceFile", sourceFile);

        return res;
      } catch (TTransportException tt) {
        // bad! the compressed input stream finishes by throwing...
        return null;
      } catch (Exception e) {
        e.printStackTrace();
        throw new IOException(e);
      }
    }


  }

  public static void main(String[] args) throws Exception {

    File testFile = new File("/usr/aubury/scratch2/jdalton/treckba/data/test/2012-05-01-23/news.0cd0144d9af6bdb4b70939dbcb879c85.xz");
    DocumentSplit split = new DocumentSplit(testFile.getAbsolutePath(), "", false, new byte[0], new byte[0], 0, 0);

    TrecKBAParser parser = new TrecKBAParser(split, new Parameters());
    Document doc = null;
    while ((doc = parser.nextDocument()) != null) {
      System.out.println(doc.name);
      //System.out.println(doc.text);
    }

  }
}
