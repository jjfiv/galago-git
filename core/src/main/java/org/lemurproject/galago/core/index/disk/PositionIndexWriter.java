// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import org.lemurproject.galago.core.index.*;
import org.lemurproject.galago.core.index.mem.MemoryPositionalIndex;
import org.lemurproject.galago.core.index.merge.PositionIndexMerger;
import org.lemurproject.galago.core.types.NumberWordPosition;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;
import org.lemurproject.galago.tupleflow.execution.Verification;
import org.lemurproject.galago.utility.Parameters;

import java.io.IOException;
import java.io.OutputStream;

/**
 * 12/14/2010 (irmarc): Adding a skip list to this structure. It's pretty basic
 * - we have a predefined skip distance in terms of how many entries to skip. A
 * skip is a two-tier structure:
 *
 * 1st tier: [d-gap doc id, d-gap byte offset to tier 2] 2nd tier: [docs byte
 * pos, counts byte pos, positions byte pos]
 *
 * Documents are d-gapped, but we already have those in tier 1. Counts are not
 * d-gapped b/c they store the # of positions, so they don't monotonically
 * track. Positions are self-contained (reset at a new doc boundary), so we only
 * need the byte information in tier 2.
 *
 * Some variable names: skipDistance: the maximum number of documents we store
 * generating a skip. skipResetDisance: the number of skips we generate before
 * we reset the offset base. Instead of storing the absolute values in the 2nd
 * tier, all entries that are some factor x*skipResetDistance are absolute
 * values, and all values until (x+1)*skipResetDistance entries away are
 * d-gapped off that absolute value so there are a few extra reads (or if you're
 * clever only one extra read), but it keeps the 2nd tier values from ballooning
 * fast, and we don't need to read them all in order to recover the original
 * values.
 *
 * @author trevor, irmarc, sjh
 */
@InputClass(className = "org.lemurproject.galago.core.types.NumberWordPosition", order = {"+word", "+document", "+position"})
public class PositionIndexWriter implements
        NumberWordPosition.WordDocumentPositionOrder.ShreddedProcessor {

  static final int MARKER_MINIMUM = 2;
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

  /**
   * Creates a new instance of the PositionIndexWriter.
   */
  public PositionIndexWriter(TupleFlowParameters parameters) throws IOException {
    actualParams = parameters.getJSON();
    actualParams.set("writerClass", getClass().getName());
    actualParams.set("readerClass", PositionIndexReader.class.getName());
    actualParams.set("mergerClass", PositionIndexMerger.class.getName());
    actualParams.set("memoryClass", MemoryPositionalIndex.class.getName());
    actualParams.set("defaultOperator", "counts");

    writer = new DiskBTreeWriter(parameters);

    // look for skips
    boolean skip = parameters.getJSON().get("skipping", true);
    skipDistance = (int) parameters.getJSON().get("skipDistance", 500);
    skipResetDistance = (int) parameters.getJSON().get("skipResetDistance", 20);
    options |= (skip ? BTreeValueIterator.HAS_SKIPS : 0x0);
    options |= BTreeValueIterator.HAS_MAXTF;
    options |= BTreeValueIterator.HAS_INLINING;
  }

  private void closeList() throws IOException {
    if (invertedList != null) {
      highestDocumentCount = Math.max(highestDocumentCount, invertedList.documentCount);
      highestFrequency = Math.max(highestFrequency, invertedList.totalPositionCount);
      collectionLength += invertedList.totalPositionCount;
      invertedList.close();
      writer.add(invertedList);

      invertedList = null;
    }
  }
  
  @Override
  public void processWord(byte[] wordBytes) throws IOException {
    closeList();

    invertedList = new PositionsList(wordBytes);
    assert lastWord == null || 0 != Utility.compare(lastWord, wordBytes) : "Duplicate word";
    lastWord = wordBytes;
    vocabCount++;
  }

  @Override
  public void processDocument(long document) throws IOException {
    invertedList.addDocument(document);
  }

  @Override
  public void processPosition(int position) throws IOException {
    invertedList.addPosition(position);
  }

  @Override
  public void processTuple() {
    // does nothing - this means we ignore duplicate postings.
  }

  @Override
  public void close() throws IOException {
    closeList();

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
      store.addError("PositionIndexWriter requires a 'filename' parameter.");
      return;
    }

    String index = parameters.getJSON().getString("filename");
    Verification.requireWriteableFile(index, store);
  }

  /**
   * The IndexElement for the PositionIndex. This is Galago's primary
   * implementation of a postings list with positions.
   */
  public class PositionsList implements IndexElement {

    private long lastDocument;
    private long lastPosition;
    private long lastPositionCount;
    private long documentCount;
    private long maximumPositionCount;
    private long totalPositionCount;
    public final byte[] word;
    public CompressedByteBuffer header;
    public DiskSpillCompressedByteBuffer documents;
    public DiskSpillCompressedByteBuffer counts;
    public DiskSpillCompressedByteBuffer positions;
    public CompressedByteBuffer positionBlock;
    // to support skipping
    private long lastDocumentSkipped;
    private long lastSkipPosition;
    private long lastDocumentSkip;
    private long lastCountSkip;
    private long lastPositionSkip;
    private long numSkips;
    private long docsSinceLastSkip;
    private DiskSpillCompressedByteBuffer skips;
    private DiskSpillCompressedByteBuffer skipPositions;

    public PositionsList(byte[] word) {
      documents = new DiskSpillCompressedByteBuffer();
      counts = new DiskSpillCompressedByteBuffer();
      positions = new DiskSpillCompressedByteBuffer();
      positionBlock = new CompressedByteBuffer();
      header = new CompressedByteBuffer();

      if ((options & BTreeValueIterator.HAS_SKIPS) == BTreeValueIterator.HAS_SKIPS) {
        skips = new DiskSpillCompressedByteBuffer();
        skipPositions = new DiskSpillCompressedByteBuffer();
      } else {
        skips = null;
      }
      
      this.word = word;
      this.lastDocument = 0;
      this.lastPosition = 0;
      this.totalPositionCount = 0;
      this.maximumPositionCount = 0;
      this.lastPositionCount = 0;
      if (skips != null) {
        this.docsSinceLastSkip = 0;
        this.lastSkipPosition = 0;
        this.lastDocumentSkipped = 0;
        this.lastDocumentSkip = 0;
        this.lastCountSkip = 0;
        this.lastPositionSkip = 0;
        this.numSkips = 0;
      }
    }

    private void finishDocument() {
      if (documents.length() > 0) {
        counts.add(lastPositionCount);

        // Now conditionally add in the skip marker and the array of position bytes
        if (lastPositionCount > MARKER_MINIMUM) {
          positions.add(positionBlock.length());
        }
        positions.add(positionBlock);
        maximumPositionCount = Math.max(maximumPositionCount, lastPositionCount);
      }
    }
    
    /**
     * Close the posting list by finishing off counts and completing header
     * data.
     *
     * @throws IOException
     */
    public void close() throws IOException {
      finishDocument();

      if (skips != null && skips.length() == 0) {
        // not adding skip information b/c its empty
        options &= (0xffff - BTreeValueIterator.HAS_SKIPS);
        header.add(options);
      } else {
        header.add(options);
      }

      // Start with the inline length
      header.add(MARKER_MINIMUM);

      header.add(documentCount);
      header.add(totalPositionCount);
      header.add(maximumPositionCount);
      if (skips != null && skips.length() > 0) {
        header.add(skipDistance);
        header.add(skipResetDistance);
        header.add(numSkips);
      }

      header.add(documents.length());
      header.add(counts.length());
      header.add(positions.length());
      if (skips != null && skips.length() > 0) {
        header.add(skips.length());
        header.add(skipPositions.length());
      }
    }

    /**
     * The length of the posting list. This is the sum of the docid, count, and
     * position buffers plus the skip buffers (if they exist).
     *
     * @return
     */
    @Override
    public long dataLength() {
      long listLength = 0;

      listLength += header.length();
      listLength += counts.length();
      listLength += positions.length();
      listLength += documents.length();
      if (skips != null) {
        listLength += skips.length();
        listLength += skipPositions.length();
      }

      return listLength;
    }

    /**
     * Write this PositionsList to the provided OutputStream object.
     *
     * @param output
     * @throws IOException
     */
    @Override
    public void write(final OutputStream output) throws IOException {
      header.write(output);
      header.clear();

      documents.write(output);
      documents.clear();

      counts.write(output);
      counts.clear();

      positions.write(output);
      positions.clear();

      if (skips != null && skips.length() > 0) {
        skips.write(output);
        skips.clear();
        skipPositions.write(output);
        skipPositions.clear();
      }
    }

    /**
     * Return the key for this PositionsList. This will be the set of bytes used
     * to access this posting list after the index is completed.
     *
     * @return
     */
    @Override
    public byte[] key() {
      return word;
    }

    /**
     * Add a new document id to the PositionsList. Assumes there will be at
     * least one position added afterwards (otherwise why add the docid?).
     *
     * @param documentID
     * @throws IOException
     */
    public void addDocument(long documentID) throws IOException {
      // add the last document's counts
      finishDocument();
      
      // TODO should this be in finishDocument()?
      if (documents.length() > 0) {
        // if we're skipping check that
        if (skips != null) {
          updateSkipInformation();
        }
      }
      
      documents.add(documentID - lastDocument);
      lastDocument = documentID;

      lastPosition = 0;
      lastPositionCount = 0;
      positionBlock.clear();
      documentCount++;
    }

    /**
     * Adds a single position for the latest document added in the
     * PositionsList.
     *
     * @param position
     * @throws IOException
     */
    public void addPosition(int position) throws IOException {
      lastPositionCount++;
      totalPositionCount++;
      positionBlock.add(position - lastPosition);
      lastPosition = position;
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
          skipPositions.add(positions.length());
          lastDocumentSkip = documents.length();
          lastCountSkip = counts.length();
          lastPositionSkip = positions.length();
        } else {
          // d-gap skip
          skipPositions.add(documents.length() - lastDocumentSkip);
          skipPositions.add(counts.length() - lastCountSkip);
          skipPositions.add((positions.length() - lastPositionSkip));
        }
        numSkips++;
      }
    }
  }
}
