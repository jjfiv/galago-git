// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;

import org.lemurproject.galago.core.btree.format.TupleflowDiskBTreeWriter;
import org.lemurproject.galago.core.index.BTreeValueIterator;
import org.lemurproject.galago.utility.buffer.CompressedByteBuffer;
import org.lemurproject.galago.tupleflow.buffer.DiskSpillCompressedByteBuffer;
import org.lemurproject.galago.core.btree.format.TupleflowBTreeWriter;
import org.lemurproject.galago.utility.btree.IndexElement;
import org.lemurproject.galago.core.index.merge.WindowIndexMerger;
import org.lemurproject.galago.core.types.NumberedExtent;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.utility.CmpUtil;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;
import org.lemurproject.galago.tupleflow.execution.Verification;

/**
 * 12/14/2010 (irmarc): Adding a skip list to this structure. It's pretty
 * basic - we have a predefined skip distance in terms of how many entries to
 * skip. A skip is a two-tier structure:
 *
 * 1st tier: [d-gap doc id, d-gap byte offset to tier 2]
 * 2nd tier: [docs byte pos, counts byte pos, positions byte pos]
 *
 * Documents are d-gapped, but we already have those in tier 1. Counts are not d-gapped b/c
 * they store the # of positions, so they don't monotonically track. Positions are self-contained
 * (reset at a new doc boundary), so we only need the byte information in tier 2.
 *
 * Some variable names:
 * skipDistance: the maximum number of documents we store generating a skip.
 * skipResetDisance: the number of skips we generate before we reset the offset
 * base. Instead of storing the absolute values in the 2nd tier, all entries that are
 * some factor x*skipResetDistance are absolute values, and all values until (x+1)*skipResetDistance
 * entries away are d-gapped off that absolute value so there are a few extra reads (or if you're clever
 * only one extra read), but it keeps the 2nd tier values from ballooning fast, and we don't need to
 * read them all in order to recover the original values.
 *
 * @author trevor, irmarc
 */
@InputClass(className = "org.lemurproject.galago.core.types.NumberedExtent", order = {"+extentName", "+number", "+begin"})
public class WindowIndexWriter implements
        NumberedExtent.ExtentNameNumberBeginOrder.ShreddedProcessor {

  // writer variables //
  Parameters actualParams;
  TupleflowBTreeWriter writer;
  WindowList invertedList;
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
  // store current begin - for several possible ends
  int currentBegin;

  /**
   * Creates a new create of PositionIndexWriter
   */
  public WindowIndexWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
    actualParams = parameters.getJSON();
    actualParams.set("writerClass", WindowIndexWriter.class.getName());
    actualParams.set("mergerClass", WindowIndexMerger.class.getName());
    actualParams.set("readerClass", WindowIndexReader.class.getName());
    actualParams.set("defaultOperator", "counts");

    writer = new TupleflowDiskBTreeWriter(parameters);

    // look for skips
    boolean skip = parameters.getJSON().get("skipping", true);
    skipDistance = (int) parameters.getJSON().get("skipDistance", 500);
    skipResetDistance = (int) parameters.getJSON().get("skipResetDistance", 20);
    options |= (skip ? BTreeValueIterator.HAS_SKIPS : 0x0);
    options |= BTreeValueIterator.HAS_MAXTF;
    // more options here?
  }

  @Override
  public void processExtentName(byte[] wordBytes) throws IOException {
    if (invertedList != null) {
      highestDocumentCount = Math.max(highestDocumentCount, invertedList.documentCount);
      highestFrequency = Math.max(highestFrequency, invertedList.totalWindowCount);
      collectionLength += invertedList.totalWindowCount;
      invertedList.close();
      writer.add(invertedList);

      invertedList = null;
    }

    invertedList = new WindowList();
    invertedList.setWord(wordBytes);
    assert lastWord == null || !CmpUtil.equals(lastWord, wordBytes) : "Duplicate word";
    lastWord = wordBytes;

    vocabCount++;
  }

  @Override
  public void processNumber(long document) throws IOException {
    invertedList.addDocument(document);
  }

  @Override
  public void processBegin(int begin) throws IOException {
    currentBegin = begin;
  }

  @Override
  public void processTuple(int end) throws IOException {
    invertedList.addWindow(currentBegin, end);
  }

  @Override
  public void close() throws IOException {
    if (invertedList != null) {
      highestDocumentCount = Math.max(highestDocumentCount, invertedList.documentCount);
      highestFrequency = Math.max(highestFrequency, invertedList.totalWindowCount);
      collectionLength += invertedList.totalWindowCount;
      invertedList.close();
      writer.add(invertedList);
    }

    // Add stats to the manifest if needed
    Parameters manifest = writer.getManifest();
    manifest.set("statistics/collectionLength", collectionLength);
    manifest.set("statistics/vocabCount", vocabCount);
    manifest.set("statistics/highestDocumentCount", this.highestDocumentCount);
    manifest.set("statistics/highestFrequency", this.highestFrequency);

    writer.close();
  }

  public static void verify(TupleFlowParameters parameters, ErrorStore store) {
    if (!parameters.getJSON().isString("filename")) {
      store.addError("WindowIndexWriter requires a 'filename' parameter.");
      return;
    }

    String index = parameters.getJSON().getString("filename");
    Verification.requireWriteableFile(index, store);
  }

  public class WindowList implements IndexElement {

    public byte[] word;
    private long lastDocument;
    private long lastBegin;
    private long positionCount;
    private long documentCount;
    private long maximumPositionCount;
    private long totalWindowCount;
    public CompressedByteBuffer header;
    public DiskSpillCompressedByteBuffer documents;
    public DiskSpillCompressedByteBuffer counts;
    public DiskSpillCompressedByteBuffer begins;
    public DiskSpillCompressedByteBuffer ends;
    // to support skipping
    private long lastDocumentSkipped;
    private long lastSkipPosition;
    private long lastDocumentSkip;
    private long lastCountSkip;
    private long lastBeginSkip;
    private long lastEndSkip;
    private long numSkips;
    private long docsSinceLastSkip;
    private DiskSpillCompressedByteBuffer skips;
    private DiskSpillCompressedByteBuffer skipPositions;

    public WindowList() {
      header = new CompressedByteBuffer();
      documents = new DiskSpillCompressedByteBuffer();
      counts = new DiskSpillCompressedByteBuffer();
      begins = new DiskSpillCompressedByteBuffer();
      ends = new DiskSpillCompressedByteBuffer();

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

      // aggregate stats
      header.add(documentCount);
      header.add(totalWindowCount);
      header.add(maximumPositionCount);
      
      if (skips != null && skips.length() > 0) {
        header.add(skipDistance);
        header.add(skipResetDistance);
        header.add(numSkips);
      }

      header.add(documents.length());
      header.add(counts.length());
      header.add(begins.length());
      header.add(ends.length());
      
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
      listLength += begins.length();
      listLength += ends.length();
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

      begins.write(output);
      begins.clear();

      ends.write(output);
      ends.clear();

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
      this.lastBegin = 0;
      this.totalWindowCount = 0;
      this.maximumPositionCount = 0;
      this.positionCount = 0;
      if (skips != null) {
        this.docsSinceLastSkip = 0;
        this.lastSkipPosition = 0;
        this.lastDocumentSkipped = 0;
        this.lastDocumentSkip = 0;
        this.lastCountSkip = 0;
        this.lastBeginSkip = 0;
        this.lastEndSkip = 0;
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

      lastBegin = 0;
      positionCount = 0;
      documentCount++;

    }

    public void addWindow(int begin, int end) throws IOException {
      positionCount++;
      totalWindowCount++;
      begins.add(begin - lastBegin);
      lastBegin = begin;
      ends.add(end - lastBegin);
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
          skipPositions.add(begins.length());
          skipPositions.add(ends.length());
          lastDocumentSkip = documents.length();
          lastCountSkip = counts.length();
          lastBeginSkip = begins.length();
          lastEndSkip = ends.length();
        } else {
          // d-gap skip
          skipPositions.add(documents.length() - lastDocumentSkip);
          skipPositions.add(counts.length() - lastCountSkip);
          skipPositions.add(begins.length() - lastBeginSkip);
          skipPositions.add(ends.length() - lastEndSkip);
        }
        numSkips++;
      }
    }
  }
}
