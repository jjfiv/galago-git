// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import org.lemurproject.galago.core.index.BTreeWriter;
import org.lemurproject.galago.core.index.CompressedByteBuffer;
import org.lemurproject.galago.core.index.DiskSpillCompressedByteBuffer;
import org.lemurproject.galago.core.index.IndexElement;
import org.lemurproject.galago.core.types.NumberedField;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;
import org.lemurproject.galago.tupleflow.execution.Verification;
import org.lemurproject.galago.utility.CmpUtil;
import org.lemurproject.galago.utility.Parameters;

import java.io.IOException;
import java.io.OutputStream;

/**
 *
 * @author trevor, sjh
 */
@InputClass(className = "org.lemurproject.galago.core.types.NumberedField", order = {"+fieldName", "+number"})
@OutputClass(className = "org.lemurproject.galago.core.types.KeyValuePair", order = {"+key"})
public class FieldIndexWriter implements NumberedField.FieldNameNumberOrder.ShreddedProcessor {

  byte[] lastWord;
  BTreeWriter writer;
  ContentList invertedList;
  long documentCount = 0;
  Parameters header;
  TupleFlowParameters stepParameters;
  String filename;

  public FieldIndexWriter(TupleFlowParameters parameters) throws IOException {
    header = parameters.getJSON();
    stepParameters = parameters;
    header.set("readerClass", FieldIndexReader.class.getName());
    header.set("writerClass", getClass().toString());
    filename = header.getString("filename");
  }

  @Override
  public void processFieldName(byte[] wordBytes) throws IOException {
    if (writer == null) {
      writer = new DiskBTreeWriter(stepParameters);
    }

    if (invertedList != null) {
      invertedList.close();
      writer.add(invertedList);
      invertedList = null;
    }

    invertedList = new ContentList(wordBytes);

    assert lastWord == null || !CmpUtil.equals(lastWord, wordBytes) : "Duplicate word";
    lastWord = wordBytes;
  }

  @Override
  public void processNumber(long document) throws IOException {
    invertedList.addDocument(document);
  }

  @Override
  public void processTuple(byte[] content) throws IOException {
    invertedList.setContent(content);
  }

  @Override
  public void close() throws IOException {
    if (invertedList != null) {
      invertedList.close();
      writer.add(invertedList);
    }
    if (writer != null) {
      writer.close();
    }
  }

  public static void verify(TupleFlowParameters parameters, ErrorStore store) {
    if (!parameters.getJSON().isString("filename")) {
      store.addError("ExtentIndexWriter requires a 'filename' parameter.");
      return;
    }

    String index = parameters.getJSON().getString("filename");
    Verification.requireWriteableFile(index, store);
  }

  public static class ContentList implements IndexElement {

    CompressedByteBuffer header;
    DiskSpillCompressedByteBuffer data;
    long lastDocument;
    long documentCount;
    byte[] key;
    byte[] content = null;

    public ContentList(byte[] k) {
      key = k;
      documentCount = 0;
      lastDocument = 0;
      header = new CompressedByteBuffer();
      data = new DiskSpillCompressedByteBuffer();
    }

    @Override
    public byte[] key() {
      return key;
    }

    @Override
    public long dataLength() {
      long listLength = 0;

      listLength += header.length();
      listLength += data.length();

      return listLength;
    }

    @Override
    public void write(OutputStream stream) throws IOException {
      header.write(stream);
      header.clear();

      data.write(stream);
      data.clear();
    }

    public void setContent(byte[] s) {
      content = s;
    }

    public void addDocument(long document) {
      if (content != null) {
        data.add(content);
      }
      data.add(document - lastDocument);
      lastDocument = document;
      content = null;
      documentCount++;
    }

    public void close() throws IOException {
      if (content != null) {
        data.add(content);
      }
      header.add(documentCount);
    }
  }
}
