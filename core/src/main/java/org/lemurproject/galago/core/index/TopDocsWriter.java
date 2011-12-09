// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import org.lemurproject.galago.core.index.disk.IndexWriter;
import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Logger;
import org.lemurproject.galago.core.types.TopDocsEntry;
import org.lemurproject.galago.tupleflow.FileSource;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.execution.ErrorHandler;

/**
 *
 * @author irmarc
 */
@InputClass(className = "org.lemurproject.galago.core.types.TopDocsEntry", order = {"+word", "-probability", "+document"})
public class TopDocsWriter implements TopDocsEntry.WordDescProbabilityDocumentOrder.ShreddedProcessor {
  private Logger LOG = Logger.getLogger(getClass().toString());

  public class TopDocsList implements IndexElement {

    byte[] key;
    CompressedRawByteBuffer data;
    CompressedByteBuffer header;
    long count;
    int lastDoc;

    public TopDocsList(byte[] key) {
      this.key = key;
      data = new CompressedRawByteBuffer();
      header = new CompressedByteBuffer();
      lastDoc = 0;
      count = 0;
    }

    public byte[] key() {
      return key;
    }

    public void addDocument(int document) {
      data.add(document);
    }

    // Write the score to the buffer, the # of docs,  then the doc ids
    public void addExpandedScore(int freq, int length) {
      data.add(freq);
      data.add(length);
      count++;
    }

    public long dataLength() {
      long length = 0;
      length += header.length();
      length += data.length();
      return length;
    }

    public void write(OutputStream stream) throws IOException {
      header.write(stream);
      data.write(stream);
    }

    public void close() {
      header.add(count);
    }
  }
  IndexWriter writer;
  TopDocsList currentList;

  public TopDocsWriter(TupleFlowParameters parameters) throws Exception {
    writer = new IndexWriter(parameters);
    writer.getManifest().set("writerClass", getClass().getName());
    writer.getManifest().set("readerClass", TopDocsReader.class.getName());
    currentList = null;
  }

  public void processWord(byte[] word) throws IOException {
    if (currentList != null) {
      currentList.close();
      writer.add(currentList);
    }
    currentList = new TopDocsList(word);
  }

  public void processProbability(double probability) throws IOException {
      // do nothing - we're not storing the probs
  }

  public void processDocument(int document) throws IOException {
    currentList.addDocument(document);
  }

  public void processTuple(int count, int doclength) throws IOException {
    currentList.addExpandedScore(count, doclength);
  }

  public void close() throws IOException {
    writer.close();
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    FileSource.verify(parameters, handler);
  }
}
