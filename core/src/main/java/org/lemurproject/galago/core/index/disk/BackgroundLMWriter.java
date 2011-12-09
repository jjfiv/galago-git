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
  long termCount = 0;
  boolean estimateDocumentCount = false;
  long longestPostingList = 0;

  /** Creates a new instance of DocumentLengthsWriter */
  public BackgroundLMWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
    super(parameters, "Term Counts written");
    Parameters p = writer.getManifest();
    p.set("writerClass", BackgroundLMWriter.class.getName());
    p.set("readerClass", BackgroundLMReader.class.getName());
    p.copyFrom(parameters.getJSON());

    bstream = new ByteArrayOutputStream();
    stream = new DataOutputStream(bstream);

    // Let's get those stats in there if we're receiving
    if (p.containsKey("statistics/documentCount")) {
      // great.
    } else if (p.isString("pipename")) {
      Parameters docCounts = NumericParameterAccumulator.accumulateParameters(parameters.getTypeReader(p.getString("pipename")));
      p.set("statistics/documentCount", docCounts.getMap("documentCount").getLong("global"));
    } else if (p.isBoolean("estimateDocumentCount")) {
      estimateDocumentCount = true;
      longestPostingList = 0;
    } else {
      throw new IOException("BackgroundLMWriter expects a 'statistics/documentCount parameter, or a tupleflow stream to read document count data from.");
    }
  }

  public GenericElement prepare(WordCount object) throws IOException {
    termCount++;
    collectionLength+=object.count;
    longestPostingList = Math.max(object.documents, longestPostingList);
    
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
    if (!manifest.isLong("statistics/termCount")) {
      manifest.set("statistics/termCount", termCount);
    }
    if (this.estimateDocumentCount) {
      manifest.set("statistics/documentCount", this.longestPostingList);
    }
    if (!manifest.isLong("statistics/collectionLength")) {
      manifest.set("statistics/collectionLength", collectionLength);
    }
    writer.close();
  }

  private Parameters collectStatistics(TypeReader<SerializedParameters> statsReader) throws IOException {
    SerializedParameters serial;
    Parameters stats = new Parameters();
    if (statsReader != null) {
      while ((serial = statsReader.read()) != null) {
        Parameters fragment = Parameters.parse(serial.parameters);
        for (String key : fragment.getKeys()) {
          if (fragment.isLong(key)) {
            long cumulativeStat = stats.get(key, 0L) + fragment.getLong(key);
            stats.set(key, cumulativeStat);
          } else if (fragment.isDouble(key)) {
            double cumulativeStat = stats.get(key, 0.0) + fragment.getDouble(key);
            stats.set(key, cumulativeStat);
          } else {
            throw new IOException("Unable to accumulate non numeric statistic: " + key);
          }
        }
      }
    }
    return stats;
  }
}
