// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.corpus.CorpusFolderWriter;
import org.lemurproject.galago.core.index.corpus.CorpusReader;
import org.lemurproject.galago.core.index.corpus.SplitBTreeKeyWriter;
import org.lemurproject.galago.tupleflow.FakeParameters;
import org.lemurproject.galago.tupleflow.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Sorter;

/**
 *
 * @author trevor
 */
public class IndexReaderSplitParserTest extends TestCase {

  Document document;
  String temporaryName = "";

  public IndexReaderSplitParserTest(String testName) {
    super(testName);
  }

  @Override
  public void tearDown() {
    if (temporaryName.length() != 0) {
      try {
        Utility.deleteDirectory(new File(temporaryName));
      } catch (IOException e) {
      }
    }
  }

  public void buildIndex() throws FileNotFoundException, IOException, IncompatibleProcessorException {
    File temporary = Utility.createTemporary();
    temporary.delete();
    temporary.mkdirs();

    temporaryName = temporary.getAbsolutePath();

    // Build an encoded document:
    document = new Document();
    document.identifier = 10;
    document.name = "doc-identifier";
    document.text = "This is the text part.";
    document.metadata.put("Key", "Value");
    document.metadata.put("Something", "Else");

    Parameters corpusWriterParameters = new Parameters();
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
  public void testNextDocument() throws Exception {
    buildIndex();

    DocumentSplit split = new DocumentSplit();
    split.fileName = temporaryName;
    split.fileType = "corpus";
    split.startKey = new byte[0];
    split.endKey = new byte[0];

    // Open up the file:
    CorpusSplitParser parser = new CorpusSplitParser(split);

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

  public void testStartKey() throws FileNotFoundException, IOException, IncompatibleProcessorException {
    buildIndex();

    DocumentSplit split = new DocumentSplit();
    split.fileName = temporaryName;
    split.fileType = "corpus";
    split.startKey = Utility.fromInt(11);
    split.endKey = new byte[0];

    // Open up the file:
    CorpusSplitParser parser = new CorpusSplitParser(split);
    assertNull(parser.nextDocument());
  }

  public void testEndKey() throws FileNotFoundException, IOException, IncompatibleProcessorException {
    buildIndex();

    DocumentSplit split = new DocumentSplit();
    split.fileName = temporaryName;
    split.fileType = "corpus";
    split.startKey = new byte[0];
    split.endKey = Utility.fromInt(9);

    // Open up the file:
    CorpusSplitParser parser = new CorpusSplitParser(split);
    assertNull(parser.nextDocument());
  }
}
