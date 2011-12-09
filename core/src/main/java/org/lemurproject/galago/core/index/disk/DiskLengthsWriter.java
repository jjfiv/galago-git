// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import org.lemurproject.galago.core.index.GenericElement;
import org.lemurproject.galago.core.index.KeyValueWriter;
import org.lemurproject.galago.core.index.merge.DocumentLengthsMerger;
import org.lemurproject.galago.core.types.NumberedDocumentData;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ErrorHandler;
import org.lemurproject.galago.tupleflow.execution.Verification;

/**
 * Writes the document lengths file based on data in NumberedDocumentData tuples.
 * The document lengths data is used by StructuredIndex because it's a key
 * input to more scoring functions.
 * 
 * offset is the first document number (for sequential sharding purposes)
 *
 * (12/01/2010, irmarc): Rewritten to make use of the IndexWriter class. As it is, the memory-mapping is
 *                     fast, but its also dangerous due to lack of compression
 * 
 * @author trevor, sjh, irmarc
 */
@InputClass(className = "org.lemurproject.galago.core.types.NumberedDocumentData", order = {"+number"})
public class DiskLengthsWriter extends KeyValueWriter<NumberedDocumentData> {

  DataOutputStream output;
  int document = 0;
  int offset = 0;
  ByteArrayOutputStream bstream;
  DataOutputStream stream;

  /** Creates a new instance of DiskLengthsWriter */
  public DiskLengthsWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
    super(parameters, "Document lengths written");
    Parameters p = writer.getManifest();
    p.set("writerClass", DiskLengthsWriter.class.getName());
    p.set("mergerClass", DocumentLengthsMerger.class.getName());
    p.set("readerClass", DiskLengthsReader.class.getName());

    bstream = new ByteArrayOutputStream();
    stream = new DataOutputStream(bstream);
  }

  public GenericElement prepare(NumberedDocumentData object) throws IOException {
    bstream.reset();
    Utility.compressInt(stream, object.textLength);
    GenericElement element = new GenericElement(Utility.fromInt(object.number), bstream.toByteArray());
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
}
