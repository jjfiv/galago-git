// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.contrib.index.disk;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.lemurproject.galago.core.index.GenericElement;
import org.lemurproject.galago.core.index.KeyValueWriter;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.execution.Verification;
import org.lemurproject.galago.core.types.WordCount;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;
import org.lemurproject.galago.utility.compression.VByte;

/**
 *
 * @author sjh
 */
@InputClass(className = "org.lemurproject.galago.core.types.WordCount", order = {"+word"})
public class BackgroundStatsWriter extends KeyValueWriter<WordCount> {

  DataOutputStream output;
  ByteArrayOutputStream bstream;
  DataOutputStream stream;
  long collectionLength = 0;
  long highestCollectionFrequency = 0;
  long highestDocumentCount = 0;
  long highestMaxDocumentFrequency = 0;
  long vocabCount = 0;

  /**
   * Creates a new create of DocumentLengthsWriter
   */
  public BackgroundStatsWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
    super(parameters, "Term Counts written");
    Parameters p = writer.getManifest();
    p.set("writerClass", this.getClass().getName());
    p.set("readerClass", BackgroundStatsReader.class.getName());
    p.copyFrom(parameters.getJSON());

    bstream = new ByteArrayOutputStream();
    stream = new DataOutputStream(bstream);
  }

  @Override
  public GenericElement prepare(WordCount wc) throws IOException {
    vocabCount++;
    collectionLength += wc.collectionFrequency;
    highestCollectionFrequency = Math.max(wc.collectionFrequency, highestCollectionFrequency);
    highestDocumentCount = Math.max(wc.documentCount, highestDocumentCount);
    highestMaxDocumentFrequency = Math.max(wc.maxDocumentFrequency, highestMaxDocumentFrequency);

    bstream.reset();
    VByte.compressLong(stream, wc.collectionFrequency);
    VByte.compressLong(stream, wc.documentCount);
    VByte.compressLong(stream, wc.maxDocumentFrequency);
    GenericElement element = new GenericElement(wc.word, bstream.toByteArray());
    return element;
  }

  public static void verify(TupleFlowParameters parameters, ErrorStore store) {
    if (!parameters.getJSON().isString("filename")) {
      store.addError("KeyValueWriters require a 'filename' parameter.");
      return;
    }

    String index = parameters.getJSON().getString("filename");
    Verification.requireWriteableFile(index, store);
  }

  @Override
  public void close() throws IOException {
    Parameters manifest = writer.getManifest();
    manifest.set("statistics/vocabCount", vocabCount);
    manifest.set("statistics/collectionLength", collectionLength);
    manifest.set("statistics/highestCollectionFrequency", this.highestCollectionFrequency);
    manifest.set("statistics/highestDocumentCount", this.highestDocumentCount);
    manifest.set("statistics/highestMaxDocumentFrequency", this.highestMaxDocumentFrequency);
    writer.close();
  }
}
