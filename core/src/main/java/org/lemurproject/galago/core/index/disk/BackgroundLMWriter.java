// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.lemurproject.galago.core.index.GenericElement;
import org.lemurproject.galago.core.index.KeyValueWriter;
import org.lemurproject.galago.core.parse.NumericParameterAccumulator;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.TypeReader;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ErrorHandler;
import org.lemurproject.galago.tupleflow.execution.Verification;
import org.lemurproject.galago.core.types.WordCount;
import org.lemurproject.galago.tupleflow.types.SerializedParameters;

/**
 *
 * @author sjh
 */
@InputClass(className = "org.lemurproject.galago.core.types.WordCount", order = {"+word"})
public class BackgroundLMWriter extends KeyValueWriter<WordCount> {

  DataOutputStream output;
  ByteArrayOutputStream bstream;
  DataOutputStream stream;
  long collectionLength = 0;
  long highestFrequency = 0;
  long highestDocumentCount = 0;
  long vocabCount = 0;

  /**
   * Creates a new instance of DocumentLengthsWriter
   */
  public BackgroundLMWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
    super(parameters, "Term Counts written");
    Parameters p = writer.getManifest();
    p.set("writerClass", BackgroundLMWriter.class.getName());
    p.set("readerClass", BackgroundLMReader.class.getName());
    p.copyFrom(parameters.getJSON());

    bstream = new ByteArrayOutputStream();
    stream = new DataOutputStream(bstream);
  }

  public GenericElement prepare(WordCount object) throws IOException {
    vocabCount++;
    collectionLength += object.count;
    highestDocumentCount = Math.max(object.documents, highestDocumentCount);
    highestFrequency = Math.max(object.count, highestFrequency);

    bstream.reset();
    Utility.compressLong(stream, object.count);
    Utility.compressLong(stream, object.documents);
    GenericElement element = new GenericElement(object.word, bstream.toByteArray());
    return element;
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!parameters.getJSON().isString("filename")) {
      handler.addError("KeyValueWriters require a 'filename' parameter.");
      return;
    }

    String index = parameters.getJSON().getString("filename");
    Verification.requireWriteableFile(index, handler);
  }

  @Override
  public void close() throws IOException {
    Parameters manifest = writer.getManifest();
    manifest.set("statistics/vocabCount", vocabCount);
    manifest.set("statistics/collectionLength", collectionLength);
    manifest.set("statistics/highestDocumentCount", this.highestDocumentCount);
    manifest.set("statistics/highestFrequency", this.highestFrequency);
    writer.close();
  }
}
