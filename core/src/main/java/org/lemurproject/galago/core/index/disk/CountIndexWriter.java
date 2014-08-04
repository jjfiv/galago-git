// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import org.lemurproject.galago.core.index.*;
import org.lemurproject.galago.core.types.NumberWordCount;
import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.tupleflow.error.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;
import org.lemurproject.galago.tupleflow.execution.Verification;
import org.lemurproject.galago.utility.CmpUtil;
import org.lemurproject.galago.utility.Parameters;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Count Indexes are similar to a Position or Window Index Writer, except that
 * extent positions are not written.
 *
 * Structure: mapping( term -> list(document-id), list(document-freq) )
 *
 * Skip lists are supported
 *
 * @author sjh
 */
@InputClass(className = "org.lemurproject.galago.core.types.NumberWordCount", order = {"+word", "+document"})
public class CountIndexWriter implements
        NumberWordCount.WordDocumentOrder.ShreddedProcessor {

  // writer variables //
  Parameters actualParams;
  BTreeWriter writer;
  CountsList invertedList;
  // statistics //
  byte[] lastWord;
  long vocabCount = 0;
  long collectionLength = 0;
  long highestFrequency = 0;
  long highestDocumentCount = 0;
  // skipping parameters
  int options = 0;
  int skipDistance;
  int skipResetDistance;

  /**
   * Creates a new instance of CountIndexWriter
   */
  public CountIndexWriter(TupleFlowParameters parameters) throws IOException {
    this.actualParams = parameters.getJSON();
    this.actualParams.setIfMissing("writerClass", CountIndexWriter.class.getName());
    this.actualParams.setIfMissing("readerClass", CountIndexReader.class.getName());
    this.actualParams.setIfMissing("defaultOperator", "counts");

    this.writer = new DiskBTreeWriter(parameters);

    // look for skips
    boolean skip = parameters.getJSON().get("skipping", true);
    this.skipDistance = (int) parameters.getJSON().get("skipDistance", 500);
    this.skipResetDistance = (int) parameters.getJSON().get("skipResetDistance", 20);

    this.options |= (skip ? BTreeValueIterator.HAS_SKIPS : 0x0);
  }

  @Override
  public void processWord(byte[] wordBytes) throws IOException {
    if (invertedList != null) {
      highestDocumentCount = Math.max(highestDocumentCount, invertedList.documentCount);
      highestFrequency = Math.max(highestFrequency, invertedList.totalInstanceCount);
      collectionLength += invertedList.totalInstanceCount;
      invertedList.close();
      writer.add(invertedList);

      invertedList = null;
    }

    invertedList = new CountsList();
    invertedList.setWord(wordBytes);
    assert lastWord == null || !CmpUtil.equals(lastWord, wordBytes) : "Duplicate word";
    lastWord = wordBytes;

    vocabCount++;
  }

  @Override
  public void processDocument(long document) throws IOException {
    invertedList.addDocument(document);
  }

  @Override
  public void processTuple(int count) throws IOException {
    invertedList.addCount(count);
  }

  @Override
  public void close() throws IOException {
    if (invertedList != null) {
      highestDocumentCount = Math.max(highestDocumentCount, invertedList.documentCount);
      highestFrequency = Math.max(highestFrequency, invertedList.totalInstanceCount);
      collectionLength += invertedList.totalInstanceCount;
      invertedList.close();
      writer.add(invertedList);
    }

    // Add stats to the manifest if needed
    Parameters manifest = writer.getManifest();
    manifest.set("statistics/collectionLength", collectionLength);
    manifest.set("statistics/vocabCount", vocabCount);
    manifest.set("statistics/highestDocumentCount", highestDocumentCount);
    manifest.set("statistics/highestFrequency", highestFrequency);

    writer.close();
  }

  public static void verify(TupleFlowParameters parameters, ErrorStore store) {
    if (!parameters.getJSON().isString("filename")) {
      store.addError("CountIndexWriter requires a 'filename' parameter.");
      return;
    }

    String index = parameters.getJSON().getString("filename");
    Verification.requireWriteableFile(index, store);
  }

  public void setProcessor(Step processor) throws IncompatibleProcessorException {
    writer.setProcessor(processor);
  }

  public class CountsList implements IndexElement {

    private long lastDocument;
    private long positionCount;
    private long documentCount;
    private long totalInstanceCount;
    private long maximumPositionCount;
    public byte[] word;
    public CompressedByteBuffer header;
    public DiskSpillCompressedByteBuffer documents;
    public DiskSpillCompressedByteBuffer counts;
    // to support skipping
    private long lastDocumentSkipped;
    private long lastSkipPosition;
    private long lastDocumentSkip;
    private long lastCountSkip;
    private long numSkips;
    private long docsSinceLastSkip;
    private DiskSpillCompressedByteBuffer skips;
    private DiskSpillCompressedByteBuffer skipPositions;

    public CountsList() {
      documents = new DiskSpillCompressedByteBuffer();
      counts = new DiskSpillCompressedByteBuffer();
      header = new CompressedByteBuffer();

      if ((options & BTreeValueIterator.HAS_SKIPS) == BTreeValueIterator.HAS_SKIPS) {
        skips = new DiskSpillCompressedByteBuffer();
        skipPositions = new DiskSpillCompressedByteBuffer();
      } else {
        skips = null;
      }
    }

    public void close() throws IOException {

      if (documents.length() > 0) {
        counts.add(positionCount);
        maximumPositionCount = Math.max(maximumPositionCount, positionCount);
      }

      if (skips != null && skips.length() == 0) {
        // not adding skip information b/c its empty
        options &= (0xffff - BTreeValueIterator.HAS_SKIPS);
        header.add(options);
      } else {
        header.add(options);
      }

      header.add(documentCount);
      header.add(totalInstanceCount);
      header.add(maximumPositionCount);
      
      if (skips != null && skips.length() > 0) {
        header.add(skipDistance);
        header.add(skipResetDistance);
        header.add(numSkips);
      }

      header.add(documents.length());
      header.add(counts.length());
      
      if (skips != null && skips.length() > 0) {
        header.add(skips.length());
        header.add(skipPositions.length());
      }
    }

    @Override
    public long dataLength() {
      long listLength = 0;

      listLength += header.length();
      listLength += counts.length();
      listLength += documents.length();
      if (skips != null) {
        listLength += skips.length();
        listLength += skipPositions.length();
      }

      return listLength;
    }

    @Override
    public void write(final OutputStream output) throws IOException {
      header.write(output);
      header.clear();

      documents.write(output);
      documents.clear();

      counts.write(output);
      counts.clear();

      if (skips != null && skips.length() > 0) {
        skips.write(output);
        skips.clear();
        skipPositions.write(output);
        skipPositions.clear();
      }
    }

    @Override
    public byte[] key() {
      return word;
    }

    public void setWord(byte[] word) {
      this.word = word;
      this.lastDocument = 0;

      this.totalInstanceCount = 0;
      this.positionCount = 0;
      this.maximumPositionCount = 0;
      if (skips != null) {
        this.docsSinceLastSkip = 0;
        this.lastSkipPosition = 0;
        this.lastDocumentSkipped = 0;
        this.lastDocumentSkip = 0;
        this.lastCountSkip = 0;
        this.numSkips = 0;
      }

    }

    public void addDocument(long documentID) throws IOException {
      // add the last document's counts
      if (documents.length() > 0) {
        counts.add(positionCount);
        maximumPositionCount = Math.max(maximumPositionCount, positionCount);

        // if we're skipping check that
        if (skips != null) {
          updateSkipInformation();
        }
      }
      documents.add(documentID - lastDocument);
      lastDocument = documentID;

      positionCount = 0;
      documentCount++;
    }

    public void addCount(int count) throws IOException {
      positionCount += count;
      totalInstanceCount += count;
    }

    private void updateSkipInformation() {
      // There are already docs entered and we've gone skipDistance docs -- make a skip
      docsSinceLastSkip = (docsSinceLastSkip + 1) % skipDistance;
      if (documents.length() > 0 && docsSinceLastSkip == 0) {
        skips.add(lastDocument - lastDocumentSkipped);
        skips.add(skipPositions.length() - lastSkipPosition);
        lastDocumentSkipped = lastDocument;
        lastSkipPosition = skipPositions.length();

        // Now we decide whether we're storing an abs. value d-gapped value
        if (numSkips % skipResetDistance == 0) {
          // absolute values
          skipPositions.add(documents.length());
          skipPositions.add(counts.length());
          lastDocumentSkip = documents.length();
          lastCountSkip = counts.length();
        } else {
          // d-gap skip
          skipPositions.add(documents.length() - lastDocumentSkip);
          skipPositions.add(counts.length() - lastCountSkip);
        }
        numSkips++;
      }
    }
  }

  public static Processor<NumberWordCount> makeSortedPipeline(Parameters config) throws IOException {
    try {
      Sorter<NumberWordCount> sorter = new Sorter<>(new NumberWordCount.WordDocumentOrder());
      CountIndexWriter writer = new CountIndexWriter(new FakeParameters(config));
      // set up mini processor pipeline
      sorter.setProcessor(new NumberWordCount.WordDocumentOrder.TupleShredder(writer));
      return sorter;
    } catch (IncompatibleProcessorException e) {
      throw new RuntimeException(e);
    }
  }
}
