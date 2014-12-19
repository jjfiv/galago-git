// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import org.junit.After;
import org.junit.Test;
import org.lemurproject.galago.core.index.corpus.CorpusFolderWriter;
import org.lemurproject.galago.core.index.corpus.CorpusReader;
import org.lemurproject.galago.core.btree.format.SplitBTreeKeyWriter;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.*;
import org.lemurproject.galago.tupleflow.error.IncompatibleProcessorException;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.Parameters;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.*;

/**
 *
 * @author trevor
 */
public class IndexReaderSplitParserTest {

  Document document;
  String temporaryName = "";

  @After
  public void tearDown() throws IOException {
    FSUtil.deleteDirectory(new File(temporaryName));
  }

  @Test
  public void buildIndex() throws IOException, IncompatibleProcessorException {
    File temporary = FileUtility.createTemporary();
    assertTrue(temporary.delete());
    assertTrue(temporary.mkdirs());

    temporaryName = temporary.getAbsolutePath();

    // Build an encoded document:
    document = new Document();
    document.identifier = 10;
    document.name = "doc-identifier";
    document.text = "This is the text part.";
    document.metadata.put("Key", "Value");
    document.metadata.put("Something", "Else");

    Parameters corpusWriterParameters = Parameters.instance();
    corpusWriterParameters.set("readerClass", CorpusReader.class.getName());
    corpusWriterParameters.set("writerClass", CorpusFolderWriter.class.getName());
    corpusWriterParameters.set("filename", temporary.getAbsolutePath());
    CorpusFolderWriter valueWriter = new CorpusFolderWriter(new FakeParameters(corpusWriterParameters.clone()));
    Sorter sorter = new Sorter(new KeyValuePair.KeyOrder());
    SplitBTreeKeyWriter keyWriter = new SplitBTreeKeyWriter(new FakeParameters(corpusWriterParameters.clone()));

    valueWriter.setProcessor(sorter);
    sorter.setProcessor(keyWriter);
    
    valueWriter.process(document);
    valueWriter.close();
  }
   
  /**
   * Test of nextDocument method, of class IndexReaderSplitParser.
   */
  @Test
  public void testNextDocument() throws Exception {
    buildIndex();

    DocumentSplit split = new DocumentSplit();
    split.fileName = temporaryName;
    split.fileType = "corpus";
    split.startKey = new byte[0];
    split.endKey = new byte[0];

    // Open up the file:
    CorpusSplitParser parser = new CorpusSplitParser(split, Parameters.instance());

    // Check the document:
    Document actual = parser.nextDocument();
    assertNotNull(actual);
    assertEquals(document.name, actual.name);
    assertEquals(document.text, actual.text);
    assertEquals(2, actual.metadata.size());
    assertNotNull(document.metadata.get("Key"));
    assertNotNull(document.metadata.get("Something"));
    assertEquals("Value", document.metadata.get("Key"));
    assertEquals("Else", document.metadata.get("Something"));

    // Make sure there aren't any left:
    assertNull(parser.nextDocument());
  }

  @Test
  public void testStartKey() throws IOException, IncompatibleProcessorException {
    buildIndex();

    DocumentSplit split = new DocumentSplit();
    split.fileName = temporaryName;
    split.fileType = "corpus";
    split.startKey = Utility.fromLong(11);
    split.endKey = new byte[0];

    // Open up the file:
    CorpusSplitParser parser = new CorpusSplitParser(split, Parameters.instance());
    assertNull(parser.nextDocument());
  }

  @Test
  public void testEndKey() throws IOException, IncompatibleProcessorException {
    buildIndex();

    DocumentSplit split = new DocumentSplit();
    split.fileName = temporaryName;
    split.fileType = "corpus";
    split.startKey = new byte[0];
    split.endKey = Utility.fromLong(9);

    // Open up the file:
    CorpusSplitParser parser = new CorpusSplitParser(split, Parameters.instance());
    assertNull(parser.nextDocument());
  }
}
