// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.*;
import org.lemurproject.galago.core.index.IndexElement;
import org.lemurproject.galago.core.index.merge.DocumentLengthsMerger;
import org.lemurproject.galago.core.types.NumberedDocumentData;
import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.tupleflow.execution.ErrorHandler;
import org.lemurproject.galago.tupleflow.execution.Verification;

/**
 * Writes the document lengths file based on data in NumberedDocumentData
 * tuples. The document lengths data is used by StructuredIndex because it's a
 * key input to more scoring functions.
 *
 * offset is the first document number (for sequential sharding purposes)
 *
 * (12/01/2010, irmarc): Rewritten to make use of the IndexWriter class. As it
 * is, the memory-mapping is fast, but its also dangerous due to lack of
 * compression
 *
 * @author trevor, sjh, irmarc
 */
@InputClass(className = "org.lemurproject.galago.core.types.NumberedDocumentData", order = {"+number"})
public class DiskLengthsWriter implements Processor<NumberedDocumentData> {

  private DiskBTreeWriter writer;
  private LengthsList lengths;

  /**
   * Creates a new instance of DiskLengthsWriter
   */
  public DiskLengthsWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
    writer = new DiskBTreeWriter(parameters);
    Parameters p = this.writer.getManifest();
    p.set("writerClass", DiskLengthsWriter.class.getName());
    p.set("mergerClass", DocumentLengthsMerger.class.getName());
    p.set("readerClass", DiskLengthsReader.class.getName());

    lengths = new LengthsList("lengths");
  }

  @Override
  public void process(NumberedDocumentData object) throws IOException {
    lengths.add(object.number, object.textLength);
  }

  @Override
  public void close() throws IOException {
    writer.getManifest().set("firstDocument", lengths.firstDocument);
    writer.add(lengths);
    writer.close();
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!parameters.getJSON().isString("filename")) {
      handler.addError("KeyValueWriters require a 'filename' parameter.");
      return;
    }

    String index = parameters.getJSON().getString("filename");
    Verification.requireWriteableFile(index, handler);
  }

  public class LengthsList implements IndexElement {

    //private CompressedRawByteBuffer lengthsData;
    private ByteArrayOutputStream buffer;
    private DataOutputStream stream;
    private byte[] key;
    private int firstDocument;
    private int documentCount;
    private int prevDocument;

    public LengthsList(String key) {
      //this.lengthsData = new CompressedRawByteBuffer();
      buffer = new ByteArrayOutputStream();
      stream = new DataOutputStream(buffer);
      this.key = Utility.fromString(key);
      this.documentCount = 0;
      this.prevDocument = -1;
      this.firstDocument = -1;
    }

    public void add(int currentDocument, int textLength) throws IOException {
      assert prevDocument < currentDocument;

      if (prevDocument < 0) {
        firstDocument = currentDocument;
      } else {
        prevDocument++;
        while (prevDocument < currentDocument) {
          stream.writeInt(0);
          prevDocument++;
        }
      }
      
      stream.writeInt(textLength);

      prevDocument = currentDocument;
      documentCount++;
    }

    @Override
    public byte[] key() {
      return key;
    }

    @Override
    public long dataLength() {
      return stream.size() + 8;
    }

    @Override
    public void write(OutputStream fileStream) throws IOException {
      fileStream.write(Utility.fromInt(firstDocument));
      fileStream.write(Utility.fromInt(prevDocument + 1));
      stream.close();

      buffer.writeTo(fileStream);
    }
  }
}
