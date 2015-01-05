// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import org.lemurproject.galago.core.btree.format.TupleflowDiskBTreeWriter;
import org.lemurproject.galago.utility.btree.IndexElement;
import org.lemurproject.galago.core.index.merge.DocumentLengthsMerger;
import org.lemurproject.galago.core.types.FieldLengthData;
import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;
import org.lemurproject.galago.tupleflow.execution.Verification;
import org.lemurproject.galago.utility.*;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.debug.Counter;
import org.lemurproject.galago.utility.debug.NullCounter;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;

/**
 * Writes the document lengths file, - stores the length data for each field,
 * and for the entire document - note that 'document' is a special field for the
 * entire document.
 *
 * data stored in each document 'field' lengths list:
 *
 * stats: - number of non-zero document lengths (document count) - sum of
 * document lengths (collection length) - average document length - maximum
 * document length - minimum document length
 *
 * utility values: - first document id - last document id (all documents
 * inbetween have a value)
 *
 * finally: - list of lengths (one per document)
 *
 * @author sjh
 */
@InputClass(className = "org.lemurproject.galago.core.types.FieldLengthData", order = {"+field", "+document"})
public class DiskLengthsWriter implements Processor<FieldLengthData> {

  private TupleflowDiskBTreeWriter writer;
  private LengthsList fieldLengthData;
  private Counter recordsWritten;
  private Counter newFields;
  private Counter fieldCounter;
  private TupleFlowParameters tupleFlowParameters;

  /**
   * Creates a new create of DiskLengthsWriter
   */
  public DiskLengthsWriter(TupleFlowParameters parameters) throws IOException {
    writer = new TupleflowDiskBTreeWriter(parameters);
    Parameters p = this.writer.getManifest();
    p.set("writerClass", DiskLengthsWriter.class.getName());
    p.set("mergerClass", DocumentLengthsMerger.class.getName());
    p.set("readerClass", DiskLengthsReader.class.getName());
    recordsWritten = parameters.getCounter("records written");
    newFields = parameters.getCounter("new Fields");
    tupleFlowParameters = parameters;
    fieldCounter = NullCounter.instance;
    fieldLengthData = null;
  }

  @Override
  public void process(FieldLengthData ld) throws IOException {
    if (fieldLengthData == null) {
      fieldLengthData = new LengthsList(ld.field);
      fieldCounter = tupleFlowParameters.getCounter(ByteUtil.toString(ld.field) + " count");

      if (newFields != null) {
        newFields.increment();
      }

    } else if (!CmpUtil.equals(fieldLengthData.field, ld.field)) {

      if (newFields != null) {
        newFields.increment();
      }

      if (!fieldLengthData.isEmpty()) {
        writer.add(fieldLengthData);
      }

      fieldCounter = tupleFlowParameters.getCounter(ByteUtil.toString(ld.field) + " count");
      fieldLengthData = new LengthsList(ld.field);
    }

    fieldLengthData.add(ld.document, ld.length);
    recordsWritten.increment();
    fieldCounter.increment();
  }

  @Override
  public void close() throws IOException {
    if (fieldLengthData != null && !fieldLengthData.isEmpty()) {
      writer.add(fieldLengthData);
    }
    writer.close();
  }

  public static void verify(TupleFlowParameters parameters, ErrorStore store) {
    if (!parameters.getJSON().isString("filename")) {
      store.addError("KeyValueWriters require a 'filename' parameter.");
      return;
    }

    String index = parameters.getJSON().getString("filename");
    Verification.requireWriteableFile(index, store);
  }

  public static final class LengthsList implements IndexElement {

    private File tempFile;
    private DataOutputStream stream;
    private byte[] field;
    // stats
    private long totalDocumentCount;
    private long nonZeroDocumentCount;
    private long collectionLength;
    private long maxLength;
    private long minLength;
    private long firstDocument;
    private long prevDocument;
    private long writtenIntegers;

    public LengthsList(byte[] key) throws IOException {
      //this.lengthsData = new CompressedRawByteBuffer();
      tempFile = FileUtility.createTemporary();
      stream = StreamCreator.realOutputStream(tempFile.getAbsolutePath());
      this.field = key;

      this.totalDocumentCount = 0;
      this.nonZeroDocumentCount = 0;
      this.collectionLength = 0;
      this.maxLength = Integer.MIN_VALUE;
      this.minLength = Integer.MAX_VALUE;

      this.prevDocument = -1;
      this.firstDocument = -1;

      this.writtenIntegers = 0;
    }

    public void add(long currentDocument, int length) throws IOException {
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
          this.writtenIntegers++;
          this.prevDocument++;
        }
      }

      // now check that we are ready to write the current document
      assert (this.prevDocument == currentDocument);
      this.stream.writeInt(length);
      this.writtenIntegers++;
    }

    @Override
    public byte[] key() {
      return field;
    }

    @Override
    public long dataLength() {
      // data to be written is :
      //  8 bytes for each of 8 long/double stats
      //  and 4 bytes per length value (integer)
      return (8 * 8) + (writtenIntegers * 4);
    }

    public boolean isEmpty() {
      return nonZeroDocumentCount == 0;
    }

    @Override
    public void write(OutputStream fileStream) throws IOException {

      assert (totalDocumentCount > 0) : "Can not write an empty lengths file for field: " + ByteUtil.toString(field);

      //  ensure the array of documents is at least totalDocumentCount long
      //(in case someone asks for the length of docId =totalDocumentCount)

      // close the length-list buffer
      stream.close();

      double avgLength = (double) collectionLength / (double) nonZeroDocumentCount;


      fileStream.write(Utility.fromLong(totalDocumentCount));
      fileStream.write(Utility.fromLong(nonZeroDocumentCount));
      fileStream.write(Utility.fromLong(collectionLength));
      fileStream.write(Utility.fromDouble(avgLength));
      fileStream.write(Utility.fromLong(maxLength));
      fileStream.write(Utility.fromLong(minLength));

      fileStream.write(Utility.fromLong(firstDocument));
      fileStream.write(Utility.fromLong(prevDocument));

      // copy length data to index file
      StreamUtil.copyFileToStream(tempFile, fileStream);

      // delete temp data
      tempFile.delete();
    }
  }
}
