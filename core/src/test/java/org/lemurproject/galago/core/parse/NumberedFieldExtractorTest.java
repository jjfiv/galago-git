/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.parse;

import java.io.File;
import java.util.ArrayList;
import junit.framework.TestCase;
import org.lemurproject.galago.core.index.disk.DiskIndex;
import org.lemurproject.galago.core.index.disk.FieldIndexReader;
import org.lemurproject.galago.core.index.disk.FieldIndexReader.KeyIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author sjh
 */
public class NumberedFieldExtractorTest extends TestCase {

  public NumberedFieldExtractorTest(String testName) {
    super(testName);
  }

  public void testFieldExtraction() throws Exception {
    StringBuilder sb = new StringBuilder();
    sb.append("<DOC>\n<DOCNO>1</DOCNO>\n<TEXT>\n");
    sb.append("<intField>1</intField>\n");
    sb.append("</TEXT>\n</DOC>\n<DOC>\n<DOCNO>2</DOCNO>\n<TEXT>\n");
    sb.append("<intField>10</intField>\n");
    sb.append("</TEXT>\n</DOC>\n<DOC>\n<DOCNO>3</DOCNO>\n<TEXT>\n");
    sb.append("<intField>-1</intField>\n");
    sb.append("</TEXT>\n</DOC>\n<DOC>\n<DOCNO>4</DOCNO>\n<TEXT>\n");
    sb.append("<longField>1</longField>\n");
    sb.append("</TEXT>\n</DOC>\n<DOC>\n<DOCNO>5</DOCNO>\n<TEXT>\n");
    sb.append("<longField>10</longField>\n");
    sb.append("</TEXT>\n</DOC>\n<DOC>\n<DOCNO>6</DOCNO>\n<TEXT>\n");
    sb.append("<longField>-1</longField>\n");
    sb.append("</TEXT>\n</DOC>\n<DOC>\n<DOCNO>7</DOCNO>\n<TEXT>\n");
    sb.append("<floatField>1</floatField>\n");
    sb.append("</TEXT>\n</DOC>\n<DOC>\n<DOCNO>8</DOCNO>\n<TEXT>\n");
    sb.append("<floatField>1.0</floatField>\n");
    sb.append("</TEXT>\n</DOC>\n<DOC>\n<DOCNO>9</DOCNO>\n<TEXT>\n");
    sb.append("<floatField>-1</floatField>\n");
    sb.append("</TEXT>\n</DOC>\n<DOC>\n<DOCNO>10</DOCNO>\n<TEXT>\n");
    sb.append("<floatField>1e6</floatField>\n");
    sb.append("</TEXT>\n</DOC>\n<DOC>\n<DOCNO>11</DOCNO>\n<TEXT>\n");
    sb.append("<doubleField>1</doubleField>\n");
    sb.append("</TEXT>\n</DOC>\n<DOC>\n<DOCNO>12</DOCNO>\n<TEXT>\n");
    sb.append("<doubleField>1.0</doubleField>\n");
    sb.append("</TEXT>\n</DOC>\n<DOC>\n<DOCNO>13</DOCNO>\n<TEXT>\n");
    sb.append("<doubleField>-1</doubleField>\n");
    sb.append("</TEXT>\n</DOC>\n<DOC>\n<DOCNO>14</DOCNO>\n<TEXT>\n");
    sb.append("<doubleField>1e6</doubleField>\n");
    sb.append("</TEXT>\n</DOC>\n<DOC>\n<DOCNO>15</DOCNO>\n<TEXT>\n");
    sb.append("<dateField>1/1/2000</dateField>\n");
    sb.append("</TEXT>\n</DOC>\n<DOC>\n<DOCNO>16</DOCNO>\n<TEXT>\n");
    sb.append("<dateField>1/1/00</dateField>\n");
    // sb.append("</TEXT>\n</DOC>\n<DOC>\n<DOCNO>1</DOCNO>\n<TEXT>\n");
    // sb.append("<dateField>January 1 2000</dateField>\n");
    sb.append("</TEXT>\n</DOC>\n");

    File input = Utility.createTemporary();
    File index = Utility.createTemporaryDirectory();
    try {
      Utility.copyStringToFile(sb.toString(), input);
      Parameters p = new Parameters();
      p.set("indexPath", index.getAbsolutePath());
      p.set("inputPath", input.getAbsolutePath());
      p.set("corpus", false);
      p.set("tokenizer", new Parameters());
      p.getMap("tokenizer").set("fields", new ArrayList());
      p.getMap("tokenizer").getList("fields").add("intfield");
      p.getMap("tokenizer").getList("fields").add("longfield");
      p.getMap("tokenizer").getList("fields").add("floatfield");
      p.getMap("tokenizer").getList("fields").add("doublefield");
      p.getMap("tokenizer").getList("fields").add("datefield");

      p.getMap("tokenizer").set("formats", new Parameters());
      p.getMap("tokenizer").getMap("formats").set("intfield", "int");
      p.getMap("tokenizer").getMap("formats").set("longfield", "long");
      p.getMap("tokenizer").getMap("formats").set("floatfield", "float");
      p.getMap("tokenizer").getMap("formats").set("doublefield", "double");
      p.getMap("tokenizer").getMap("formats").set("datefield", "date");

      App.run("build", p, System.err);
      // Retrieval r = RetrievalFactory.instance(index.getAbsolutePath(), new Parameters());

      FieldIndexReader fieldReader = (FieldIndexReader) DiskIndex.openIndexComponent(new File(index, "fields").getAbsolutePath());

      ScoringContext context = new ScoringContext();

      KeyIterator iterator = fieldReader.getIterator();
      assertEquals(iterator.getKeyString(), "datefield");
      FieldIndexReader.ListIterator valueIterator = (FieldIndexReader.ListIterator) iterator.getValueIterator();
      valueIterator.setContext(context);
      context.document = valueIterator.currentCandidate();
      // assertEquals(valueIterator.dateValue());
      valueIterator.movePast(valueIterator.currentCandidate());
      context.document = valueIterator.currentCandidate();
      // assertEquals(valueIterator.dateValue());
      valueIterator.movePast(valueIterator.currentCandidate());
      assertEquals(valueIterator.isDone(), true);
      
      assertEquals(iterator.nextKey(), true);
      assertEquals(iterator.getKeyString(), "doublefield");
      valueIterator = (FieldIndexReader.ListIterator) iterator.getValueIterator();
      valueIterator.setContext(context);
      context.document = valueIterator.currentCandidate();
      assertEquals(valueIterator.doubleValue(), 1.0, 0.0000001);
      valueIterator.movePast(valueIterator.currentCandidate());
      context.document = valueIterator.currentCandidate();
      assertEquals(valueIterator.doubleValue(), 1.0, 0.0000001);
      valueIterator.movePast(valueIterator.currentCandidate());
      context.document = valueIterator.currentCandidate();
      assertEquals(valueIterator.doubleValue(), -1.0, 0.0000001);
      valueIterator.movePast(valueIterator.currentCandidate());
      context.document = valueIterator.currentCandidate();
      assertEquals(valueIterator.doubleValue(), 1000000.0, 0.0000001);
      valueIterator.movePast(valueIterator.currentCandidate());
      assertEquals(valueIterator.isDone(), true);

      assertEquals(iterator.nextKey(), true);
      assertEquals(iterator.getKeyString(), "floatfield");
      valueIterator = (FieldIndexReader.ListIterator) iterator.getValueIterator();
      valueIterator.setContext(context);
      context.document = valueIterator.currentCandidate();
      assertEquals(valueIterator.floatValue(), 1.0, 0.0000001);
      valueIterator.movePast(valueIterator.currentCandidate());
      context.document = valueIterator.currentCandidate();
      assertEquals(valueIterator.floatValue(), 1.0, 0.0000001);
      valueIterator.movePast(valueIterator.currentCandidate());
      context.document = valueIterator.currentCandidate();
      assertEquals(valueIterator.floatValue(), -1.0, 0.0000001);
      valueIterator.movePast(valueIterator.currentCandidate());
      context.document = valueIterator.currentCandidate();
      assertEquals(valueIterator.floatValue(), 1000000.0, 0.0000001);
      valueIterator.movePast(valueIterator.currentCandidate());
      assertEquals(valueIterator.isDone(), true);

      assertEquals(iterator.nextKey(), true);
      assertEquals(iterator.getKeyString(), "intfield");
      valueIterator = (FieldIndexReader.ListIterator) iterator.getValueIterator();
      valueIterator.setContext(context);
      context.document = valueIterator.currentCandidate();
      assertEquals(valueIterator.intValue(), 1);
      valueIterator.movePast(valueIterator.currentCandidate());
      context.document = valueIterator.currentCandidate();
      assertEquals(valueIterator.intValue(), 10);
      valueIterator.movePast(valueIterator.currentCandidate());
      context.document = valueIterator.currentCandidate();
      assertEquals(valueIterator.intValue(), -1);
      valueIterator.movePast(valueIterator.currentCandidate());
      assertEquals(valueIterator.isDone(), true);

      assertEquals(iterator.nextKey(), true);
      assertEquals(iterator.getKeyString(), "longfield");
      valueIterator = (FieldIndexReader.ListIterator) iterator.getValueIterator();
      valueIterator.setContext(context);
      context.document = valueIterator.currentCandidate();
      assertEquals(valueIterator.longValue(), 1);
      valueIterator.movePast(valueIterator.currentCandidate());
      context.document = valueIterator.currentCandidate();
      assertEquals(valueIterator.longValue(), 10);
      valueIterator.movePast(valueIterator.currentCandidate());
      context.document = valueIterator.currentCandidate();
      assertEquals(valueIterator.longValue(), -1);
      valueIterator.movePast(valueIterator.currentCandidate());
      assertEquals(valueIterator.isDone(), true);

      assertEquals(iterator.nextKey(), false);


    } finally {
      input.delete();
      Utility.deleteDirectory(index);
    }

  }
}
