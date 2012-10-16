// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import gnu.trove.set.hash.TIntHashSet;
import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.util.logging.Logger;
import org.lemurproject.galago.core.index.CompressedByteBuffer;
import org.lemurproject.galago.core.index.CompressedRawByteBuffer;
import org.lemurproject.galago.core.index.BTreeWriter;
import org.lemurproject.galago.core.index.IndexElement;
import org.lemurproject.galago.core.index.KeyListReader;
import org.lemurproject.galago.core.index.merge.WindowIndexMerger;
import org.lemurproject.galago.core.parse.NumericParameterAccumulator;
import org.lemurproject.galago.core.types.NumberedExtent;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ErrorHandler;
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
  BTreeWriter writer;
  PositionsList invertedList;
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
   * Creates a new instance of PositionIndexWriter
   */
  public WindowIndexWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
    actualParams = parameters.getJSON();
    actualParams.set("writerClass", WindowIndexWriter.class.getName());
    actualParams.set("mergerClass", WindowIndexMerger.class.getName());
    actualParams.set("readerClass", WindowIndexReader.class.getName());
    actualParams.set("defaultOperator", "counts");

    writer = new DiskBTreeWriter(parameters);

    // look for skips
    boolean skip = parameters.getJSON().get("skipping", true);
    skipDistance = (int) parameters.getJSON().get("skipDistance", 500);
    skipResetDistance = (int) parameters.getJSON().get("skipResetDistance", 20);
    options |= (skip ? KeyListReader.ListIterator.HAS_SKIPS : 0x0);
    options |= KeyListReader.ListIterator.HAS_MAXTF;
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

    invertedList = new PositionsList();
    invertedList.setWord(wordBytes);
    assert lastWord == null || 0 != Utility.compare(lastWord, wordBytes) : "Duplicate word";
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

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!parameters.getJSON().isString("filename")) {
      handler.addError("WindowIndexWriter requires a 'filename' parameter.");
      return;
    }

    String index = parameters.getJSON().getString("filename");
    Verification.requireWriteableFile(index, handler);
  }

  public class PositionsList implements IndexElement {

    public PositionsList() {
      documents = new CompressedRawByteBuffer();
      counts = new CompressedRawByteBuffer();
      begins = new CompressedRawByteBuffer();
      ends = new CompressedRawByteBuffer();
      header = new CompressedByteBuffer();

      if ((options & KeyListReader.ListIterator.HAS_SKIPS) == KeyListReader.ListIterator.HAS_SKIPS) {
        skips = new CompressedRawByteBuffer();
        skipPositions = new CompressedRawByteBuffer();
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
        options &= (0xffff - KeyListReader.ListIterator.HAS_SKIPS);
        header.add(options);
      } else {
        header.add(options);
      }

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
          skipPositions.add((long) (begins.length() - lastBeginSkip));
          skipPositions.add((long) (ends.length() - lastEndSkip));
        }
        numSkips++;
      }
    }
    private long lastDocument;
    private int lastBegin;
    private int positionCount;
    private int documentCount;
    private int maximumPositionCount;
    private int totalWindowCount;
    public byte[] word;
    public CompressedByteBuffer header;
    public CompressedRawByteBuffer documents;
    public CompressedRawByteBuffer counts;
    public CompressedRawByteBuffer begins;
    public CompressedRawByteBuffer ends;
    // to support skipping
    private long lastDocumentSkipped;
    private long lastSkipPosition;
    private long lastDocumentSkip;
    private long lastCountSkip;
    private long lastBeginSkip;
    private long lastEndSkip;
    private long numSkips;
    private long lastCount;
    private int docsSinceLastSkip;
    private CompressedRawByteBuffer skips;
    private CompressedRawByteBuffer skipPositions;
  }
}
