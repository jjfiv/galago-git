// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.*;

import org.lemurproject.galago.core.index.IndexElement;
import org.lemurproject.galago.core.index.merge.DocumentLengthsMerger;
import org.lemurproject.galago.core.types.FieldLengthData;
import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.tupleflow.execution.ErrorHandler;
import org.lemurproject.galago.tupleflow.execution.Verification;

/**
 * Writes the document lengths file,
 *  - stores the length data for each field, and for the entire document
 *  - note that 'document' is a special field for the entire document.
 *
 * data stored in each document 'field' lengths list:
 *
 *   stats:
 *  - number of non-zero document lengths (document count)
 *  - sum of document lengths (collection length)
 *  - average document length
 *  - maximum document length
 *  - minimum document length
 *
 *   utility values:
 *  - first document id
 *  - last document id (all documents inbetween have a value)
 *
 *   finally:
 *  - list of lengths (one per document)
 *
 * @author sjh
 */
@InputClass(className = "org.lemurproject.galago.core.types.FieldLengthData", order = {"+field", "+document"})
public class DiskLengthsWriter implements Processor<FieldLengthData> {

  private DiskBTreeWriter writer;
  private LengthsList fieldLengthData;
  private Counter recordsWritten;
  private Counter recordsRead;
  private Counter newFields;
  private Counter fieldCounter;
  private TupleFlowParameters tupleFlowParameters;

  /**
   * Creates a new instance of DiskLengthsWriter
   */
  public DiskLengthsWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
    writer = new DiskBTreeWriter(parameters);
    Parameters p = this.writer.getManifest();
    p.set("writerClass", DiskLengthsWriter.class.getName());
    p.set("mergerClass", DocumentLengthsMerger.class.getName());
    p.set("readerClass", DiskLengthsReader.class.getName());
    p.set("version", 3);
    recordsWritten = parameters.getCounter("records written");
    recordsRead = parameters.getCounter("records read");
    newFields = parameters.getCounter("new Fields");
    tupleFlowParameters = parameters;
    fieldLengthData = null;
  }

  @Override
  public void process(FieldLengthData ld) throws IOException {
    if (fieldLengthData == null) {
      fieldLengthData = new LengthsList(ld.field);
      fieldCounter = tupleFlowParameters.getCounter(Utility.toString(ld.field) + " count");

      if (newFields != null) {
        newFields.increment();
      }

    } else if (Utility.compare(fieldLengthData.field, ld.field) != 0) {

      if (newFields != null) {
        newFields.increment();
      }

      if (!fieldLengthData.isEmpty()) {
        writer.add(fieldLengthData);
      }

      fieldCounter = tupleFlowParameters.getCounter(Utility.toString(ld.field) + " count");
      fieldLengthData = new LengthsList(ld.field);
    }

    fieldLengthData.add(ld.document, ld.length);
    if (recordsWritten != null) {
      recordsWritten.increment();
    }
    if (fieldCounter != null) {
      fieldCounter.increment();
    }
  }

  @Override
  public void close() throws IOException {
    if (fieldLengthData != null && !fieldLengthData.isEmpty()) {
      writer.add(fieldLengthData);
    }
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

    private File tempFile;
    private DataOutputStream stream;
    private byte[] field;
    // stats
    private long totalDocumentCount;
    private long nonZeroDocumentCount;
    private long collectionLength;
    private long maxLength;
    private long minLength;
    private int firstDocument;
    private int prevDocument;

    public LengthsList(byte[] key) throws IOException {
      //this.lengthsData = new CompressedRawByteBuffer();
      tempFile = Utility.createTemporary();
      stream = StreamCreator.realOutputStream(tempFile.getAbsolutePath());
      this.field = key;

      this.totalDocumentCount = 0;
      this.nonZeroDocumentCount = 0;
      this.collectionLength = 0;
      this.maxLength = Integer.MIN_VALUE;
      this.minLength = Integer.MAX_VALUE;

      this.prevDocument = -1;
      this.firstDocument = -1;
    }

    public void add(int currentDocument, int length) throws IOException {
      totalDocumentCount++;
      // we ignore zero length documents, as this is the default returned value
      if (length == 0) {
        return;
      }

      // update stats
      this.nonZeroDocumentCount++;
      this.collectionLength += length;
      this.maxLength = Math.max(this.maxLength, length);
      this.minLength = Math.min(this.minLength, length);

      // the previous document should be less than the current document
      assert (this.prevDocument < currentDocument);
      if (this.prevDocument < 0) {
        this.firstDocument = currentDocument;
        this.prevDocument = currentDocument;
      } else {
        this.prevDocument++;
        while (this.prevDocument < currentDocument) {
          this.stream.writeInt(0);
          this.prevDocument++;
        }
      }

      // now check that we are ready to write the current document
      assert (this.prevDocument == currentDocument);
      this.stream.writeInt(length);
    }

    @Override
    public byte[] key() {
      return field;
    }

    @Override
    public long dataLength() {
      // data to be written is :
      //  4 bytes for each of 2 integer statistics
      //  8 bytes for each of 6 long/double stats
      //  and the stream data
      return (4 * 2) + (8 * 6) + stream.size();
    }

    public boolean isEmpty() {
      return nonZeroDocumentCount == 0;
    }

    @Override
    public void write(OutputStream fileStream) throws IOException {

      assert (totalDocumentCount > 0) : "Can not write an empty lengths file for field: " + Utility.toString(field);

      //  ensure the array of documents is at least totalDocumentCount long
      //(in case someone asks for the length of docId =totalDocumentCount)

      // close the length-list buffer
      stream.close();

      double avgLength = (double) collectionLength / (double) totalDocumentCount;


      fileStream.write(Utility.fromLong(totalDocumentCount));
      fileStream.write(Utility.fromLong(nonZeroDocumentCount));
      fileStream.write(Utility.fromLong(collectionLength));
      fileStream.write(Utility.fromDouble(avgLength));
      fileStream.write(Utility.fromLong(maxLength));
      fileStream.write(Utility.fromLong(minLength));

      fileStream.write(Utility.fromInt(firstDocument));
      fileStream.write(Utility.fromInt(prevDocument));

      // copy length data to index file
      Utility.copyFileToStream(tempFile, fileStream);

      // delete temp data
      tempFile.delete();
    }
  }
}
