/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.contrib.document;

import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.tupleflow.execution.Verified;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;

/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
public class TrecTextDocumentWriter implements Processor<Document> {

  private final int uniqId;
  private final String folder;
  private final String shardName;
  private final long maxFileSize;
  private final boolean compress;
  BufferedOutputStream currentWriter;
  long currentFileId;
  long currentFileSize;

  public TrecTextDocumentWriter(TupleFlowParameters tp) {
    Parameters p = tp.getJSON();

    uniqId = tp.getInstanceId();
    folder = p.getString("outputPath");
    shardName = p.getString("shardName");
    maxFileSize = p.getLong("outFileSize");
    compress = p.getBoolean("compress");

    currentWriter = null;
    currentFileId = 0;
    currentFileSize = maxFileSize;
  }

  @Override
  public void process(Document d) throws IOException {
    if (currentFileSize >= maxFileSize) {
      flush();
    }

    byte[] bytes =
            Utility.fromString(String.format("<DOC>\n"
            + "<DOCNO>%s</DOCNO>\n"
            + "<TEXT>\n"
            + "%s\n"
            + "</TEXT>\n"
            + "</DOC>\n", d.name, d.getText()));

    currentWriter.write(bytes);
    currentFileSize += bytes.length;
  }

  @Override
  public void close() throws IOException {
    if (currentWriter != null) {
      currentWriter.close();
    }
  }

  public void flush() throws IOException {
    if (currentWriter != null) {
      currentWriter.close();
    }

    String path = String.format("%s%s%s.%04d.%04d.trectext", folder, File.separator, shardName, uniqId, currentFileId);
//            folder + File.separator + shardName + "." + uniqId + "." + currentFileId + ".trectext";
    if (compress) {
      currentWriter = new BufferedOutputStream(StreamCreator.openOutputStream(path + ".gz"));
    } else {
      currentWriter = new BufferedOutputStream(StreamCreator.openOutputStream(path));
    }
    
    currentFileId+=1;
    currentFileSize = 0;
  }
}
