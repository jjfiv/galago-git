// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import org.lemurproject.galago.core.index.CompressedByteBuffer;
import org.lemurproject.galago.core.index.DiskSpillCompressedByteBuffer;
import org.lemurproject.galago.core.index.IndexElement;
import org.lemurproject.galago.core.types.DocumentWordPositionScore;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author jfoley
 */
@InputClass(className = "org.lemurproject.galago.core.types.NumberWordPosition", order = {"+word", "+document", "+position"})
public class PositionScoreWriter implements DocumentWordPositionScore.WordDocumentPositionOrder.ShreddedProcessor {
  Parameters conf;
  DiskBTreeWriter writer;
  PositionScoreList data;
  
  // statistics
  long highestDocumentCount = 0;
  long highestFrequency = 0;
  long collectionLength = 0;
  long vocabCount = 0;
  
  byte[] prevWord;
  
  public PositionScoreWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
    conf = parameters.getJSON();
    conf.set("writerClass", getClass().getName());
    conf.set("readerClass", "TBD");
    conf.set("mergerClass", "TBD");
    conf.set("memoryClass", "TBD");
    conf.set("defaultOperator", "scores");
    
    writer = new DiskBTreeWriter(parameters);
    
    //TODO skipping
  }
  
  private void finishWord() throws IOException {
    if(data != null) {
      highestDocumentCount = Math.max(highestDocumentCount, data.documentCount);
      highestFrequency = Math.max(highestFrequency, data.positionCount);
      collectionLength = Math.max(collectionLength, data.positionCount);
      
      data.close();
      writer.add(data);
      
      data = null;
    }
  }
    
  @Override
  public void processWord(byte[] word) throws IOException {
    finishWord();
    
    data = new PositionScoreList(word);
    assert(prevWord == null || 0 != Utility.compare(prevWord, word)) : "Duplicate Word";
    prevWord = word;
    vocabCount++;
  }

  @Override
  public void processDocument(long document) throws IOException {
    data.addDocument(document);
  }

  @Override
  public void processPosition(int position) throws IOException {
    data.addPosition(position);
  }

  @Override
  public void processTuple(double score) throws IOException {
    data.addScore(score);
  }

  @Override
  public void close() throws IOException {
    finishWord();
    
    // Add stats to the manifest if needed
    Parameters manifest = writer.getManifest();
    manifest.set("statistics/collectionLength", collectionLength);
    manifest.set("statistics/vocabCount", vocabCount);
    manifest.set("statistics/highestDocumentCount", highestDocumentCount);
    manifest.set("statistics/highestFrequency", highestFrequency);
    
    writer.close();
  }
  
  
  public static class PositionScoreList implements IndexElement {
    public CompressedByteBuffer header;
    public DiskSpillCompressedByteBuffer documents;
    public DiskSpillCompressedByteBuffer positions;
    public DiskSpillCompressedByteBuffer scores;
    
    private byte[] word;
    
    // header info
    public long documentCount;
    public long positionCount;
    private long maxPositionCount;
    private long totalPositionCount;
    
    // to support dgapping
    private long lastDocument;
    private long lastPosition;
    
    public PositionScoreList(byte[] word) {
      this.word = word;
      header = new CompressedByteBuffer();
      documents = new DiskSpillCompressedByteBuffer();
      positions = new DiskSpillCompressedByteBuffer();
      scores = new DiskSpillCompressedByteBuffer();
    }
    
    public void close() throws IOException {
      assert(header.length() == 0);
      //dgapped stream locations
      header.add(header.length()); // document start
      header.add(documents.length()); // positions start
      header.add(positions.length()); //scores start
    }
    
    @Override
    public byte[] key() {
      return word;
    }

    @Override
    public long dataLength() {
      return header.length() + documents.length() + positions.length() + scores.length();
    }

    @Override
    public void write(OutputStream stream) throws IOException {
      header.write(stream);
      header = null;
      
      documents.write(stream);
      documents = null;
      
      scores.write(stream);
      scores = null;
      
      positions.write(stream);
      positions = null;
  
    }

    private void addDocument(long document) {
      documents.add(document - lastDocument);
      lastDocument = document;
      
      lastPosition = 0;
      documentCount++;
    }

    private void addPosition(int position) {
      positions.add(position - lastPosition);
      lastPosition = position;
      
      positionCount++;
    }

    private void addScore(double score) {
      scores.addDouble(score);
    }
    
  }
}
