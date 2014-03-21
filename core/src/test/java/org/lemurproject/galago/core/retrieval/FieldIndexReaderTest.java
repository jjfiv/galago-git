// BSD License (http://lemurproject.org/galago-license)
/*
 * FieldIndexReaderTest.java
 * JUnit based test
 *
 * Created on August 21, 2011
 */
package org.lemurproject.galago.core.retrieval;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.lemurproject.galago.core.index.disk.DiskBTreeReader;
import org.lemurproject.galago.core.index.disk.FieldIndexReader;
import org.lemurproject.galago.core.index.disk.FieldIndexWriter;
import org.lemurproject.galago.core.retrieval.iterator.EqualityIterator;
import org.lemurproject.galago.core.retrieval.iterator.GreaterThanIterator;
import org.lemurproject.galago.core.retrieval.iterator.InBetweenIterator;
import org.lemurproject.galago.core.retrieval.iterator.LessThanIterator;
import org.lemurproject.galago.core.retrieval.processing.ScoringContext;
import org.lemurproject.galago.core.retrieval.query.NodeParameters;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.text.DateFormat;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 *
 * @author irmarc
 */
public class FieldIndexReaderTest {

  File tempPath;

  @Before
  public void setUp() throws Exception {
    // make a spot for the index
    tempPath = FileUtility.createTemporary();

    Parameters tokenizer = new Parameters();
    Parameters formats = new Parameters();
    formats.set("title", "string");
    formats.set("date", "date");
    formats.set("version", "int");
    tokenizer.set("formats", formats);
    String[] fields = {"title", "date", "version"};
    tokenizer.set("fields", Arrays.asList(fields));

    Parameters params = new Parameters();
    params.set("filename", tempPath.toString());
    params.set("tokenizer", tokenizer);

    FieldIndexWriter writer =
            new FieldIndexWriter(new org.lemurproject.galago.tupleflow.FakeParameters(params));

    // Dates are... dates
    writer.processFieldName(Utility.fromString("date"));
    DateFormat df = DateFormat.getDateInstance(DateFormat.SHORT);

    writer.processNumber(15);
    writer.processTuple(Utility.fromLong(df.parse("6/12/1980").getTime()));

    writer.processNumber(25);
    writer.processTuple(Utility.fromLong(df.parse("6/12/1949").getTime()));


    writer.processNumber(47);
    writer.processTuple(Utility.fromLong(df.parse("1/1/1663").getTime()));

    // Titles are strings
    writer.processFieldName(Utility.fromString("title"));

    writer.processNumber(1);
    writer.processTuple(convertStringToBytes("doc1"));

    writer.processNumber(9);
    writer.processTuple(convertStringToBytes("doc2"));

    writer.processNumber(34);
    writer.processTuple(convertStringToBytes("doc9"));

    // We consider versions to be ints
    writer.processFieldName(Utility.fromString("version"));

    writer.processNumber(1);
    writer.processTuple(Utility.fromInt(1));

    writer.processNumber(2);
    writer.processTuple(Utility.fromInt(12));

    writer.processNumber(3);
    writer.processTuple(Utility.fromInt(4));

    writer.close();
  }

  @After
  public void tearDown() throws Exception {
    tempPath.delete();
  }

  protected byte[] convertStringToBytes(String s) throws Exception {
    byte[] strBytes = Utility.fromString(s);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    baos.write(Utility.compressInt(strBytes.length));
    baos.write(strBytes);
    return (baos.toByteArray());
  }

  @Test
  public void testReadTitle() throws Exception {
    FieldIndexReader reader = new FieldIndexReader(new DiskBTreeReader(tempPath.toString()));
    FieldIndexReader.ListIterator fields = reader.getField("title");
    ScoringContext sc = new ScoringContext();

    assertFalse(fields.isDone());
    assertEquals(fields.currentCandidate(), 1);
    sc.document = fields.currentCandidate();
    assertEquals(fields.stringValue(sc), "doc1");

    fields.movePast(fields.currentCandidate());

    assertFalse(fields.isDone());
    assertEquals(fields.currentCandidate(), 9);
    sc.document = fields.currentCandidate();
    assertEquals(fields.stringValue(sc), "doc2");

    fields.movePast(fields.currentCandidate());

    assertFalse(fields.isDone());
    assertEquals(fields.currentCandidate(), 34);
    sc.document = fields.currentCandidate();
    assertEquals(fields.stringValue(sc), "doc9");

    fields.movePast(fields.currentCandidate());
    assertTrue(fields.isDone());
    reader.close();
  }

  @Test
  public void testReadDate() throws Exception {
    FieldIndexReader reader = new FieldIndexReader(new DiskBTreeReader(tempPath.toString()));
    FieldIndexReader.ListIterator fields = reader.getField("date");
    ScoringContext sc = new ScoringContext();

    DateFormat df = DateFormat.getDateInstance(DateFormat.SHORT);

    assertFalse(fields.isDone());
    assertEquals(fields.currentCandidate(), 15);
    sc.document = fields.currentCandidate();
    assertEquals(fields.dateValue(sc), df.parse("6/12/1980").getTime());

    fields.movePast(fields.currentCandidate());

    assertFalse(fields.isDone());
    assertEquals(fields.currentCandidate(), 25);
    sc.document = fields.currentCandidate();
    assertEquals(fields.dateValue(sc), df.parse("6/12/1949").getTime());

    fields.movePast(fields.currentCandidate());

    assertFalse(fields.isDone());
    assertEquals(fields.currentCandidate(), 47);
    sc.document = fields.currentCandidate();
    assertEquals(fields.dateValue(sc), df.parse("1/1/1663").getTime());

    fields.movePast(fields.currentCandidate());
    assertTrue(fields.isDone());
    reader.close();
  }

  @Test
  public void testReadVersion() throws Exception {
    FieldIndexReader reader = new FieldIndexReader(new DiskBTreeReader(tempPath.toString()));
    FieldIndexReader.ListIterator fields = reader.getField("version");
    ScoringContext sc = new ScoringContext();

    assertFalse(fields.isDone());
    assertEquals(fields.currentCandidate(), 1);
    sc.document = fields.currentCandidate();
    assertEquals(fields.intValue(sc), 1);

    fields.movePast(fields.currentCandidate());

    assertFalse(fields.isDone());
    assertEquals(fields.currentCandidate(), 2);
    sc.document = fields.currentCandidate();
    assertEquals(fields.intValue(sc), 12);

    fields.movePast(fields.currentCandidate());

    assertFalse(fields.isDone());
    assertEquals(fields.currentCandidate(), 3);
    sc.document = fields.currentCandidate();
    assertEquals(fields.intValue(sc), 4);

    fields.movePast(fields.currentCandidate());
    assertTrue(fields.isDone());
    reader.close();
  }

  @Test
  public void testGreaterThan() throws Exception {
    FieldIndexReader reader = new FieldIndexReader(new DiskBTreeReader(tempPath.toString()));
    FieldIndexReader.ListIterator fields = reader.getField("version");
    ScoringContext sc = new ScoringContext();

    NodeParameters p = new NodeParameters();
    p.set("0", 5);
    GreaterThanIterator gti = new GreaterThanIterator(p, fields);

    assertFalse(gti.isDone());
    assertEquals(gti.currentCandidate(), 1);
    sc.document = gti.currentCandidate();
    assertTrue(gti.hasMatch(gti.currentCandidate()));
    assertFalse(gti.indicator(sc));
    gti.movePast(gti.currentCandidate());

    assertFalse(gti.isDone());
    assertEquals(gti.currentCandidate(), 2);
    sc.document = gti.currentCandidate();
    assertTrue(gti.hasMatch(gti.currentCandidate()));
    assertTrue(gti.indicator(sc));
    gti.movePast(gti.currentCandidate());

    assertFalse(gti.isDone());
    assertEquals(gti.currentCandidate(), 3);
    sc.document = gti.currentCandidate();
    assertTrue(gti.hasMatch(gti.currentCandidate()));
    assertFalse(gti.indicator(sc));
    gti.movePast(gti.currentCandidate());

    assertTrue(gti.isDone());
    reader.close();
  }

  @Test
  public void testLessThan() throws Exception {
    FieldIndexReader reader = new FieldIndexReader(new DiskBTreeReader(tempPath.toString()));
    FieldIndexReader.ListIterator fields = reader.getField("version");
    ScoringContext sc = new ScoringContext();

    NodeParameters p = new NodeParameters();
    p.set("0", 5);
    LessThanIterator lti = new LessThanIterator(p, fields);

    assertFalse(lti.isDone());
    assertEquals(lti.currentCandidate(), 1);
    sc.document = lti.currentCandidate();
    assertTrue(lti.hasMatch(lti.currentCandidate()));
    assertTrue(lti.indicator(sc));
    lti.movePast(lti.currentCandidate());

    assertFalse(lti.isDone());
    assertEquals(lti.currentCandidate(), 2);
    sc.document = lti.currentCandidate();
    assertTrue(lti.hasMatch(lti.currentCandidate()));
    assertFalse(lti.indicator(sc));
    lti.movePast(lti.currentCandidate());

    assertFalse(lti.isDone());
    assertEquals(lti.currentCandidate(), 3);
    sc.document = lti.currentCandidate();
    assertTrue(lti.hasMatch(lti.currentCandidate()));
    assertTrue(lti.indicator(sc));
    lti.movePast(lti.currentCandidate());

    assertTrue(lti.isDone());
    reader.close();
  }

  @Test
  public void testInBetween() throws Exception {
    FieldIndexReader reader = new FieldIndexReader(new DiskBTreeReader(tempPath.toString()));
    FieldIndexReader.ListIterator fields = reader.getField("date");
    ScoringContext sc = new ScoringContext();

    NodeParameters p = new NodeParameters();
    p.set("0", "12/25/1939");
    p.set("1", "4/12/1984");
    InBetweenIterator ibi = new InBetweenIterator(p, fields);

    assertFalse(ibi.isDone());
    assertEquals(ibi.currentCandidate(), 15);
    sc.document = ibi.currentCandidate();
    assertTrue(ibi.hasMatch(ibi.currentCandidate()));
    assertTrue(ibi.indicator(sc));
    ibi.movePast(ibi.currentCandidate());

    assertFalse(ibi.isDone());
    assertEquals(ibi.currentCandidate(), 25);
    sc.document = ibi.currentCandidate();
    assertTrue(ibi.hasMatch(ibi.currentCandidate()));
    assertTrue(ibi.indicator(sc));
    ibi.movePast(ibi.currentCandidate());

    assertFalse(ibi.isDone());
    assertEquals(ibi.currentCandidate(), 47);
    sc.document = ibi.currentCandidate();
    assertTrue(ibi.hasMatch(ibi.currentCandidate()));
    assertFalse(ibi.indicator(sc));
    ibi.movePast(ibi.currentCandidate());

    assertTrue(ibi.isDone());
    reader.close();
  }

  @Test
  public void testEquality() throws Exception {
    FieldIndexReader reader = new FieldIndexReader(new DiskBTreeReader(tempPath.toString()));
    FieldIndexReader.ListIterator fields = reader.getField("title");
    ScoringContext sc = new ScoringContext();

    NodeParameters p = new NodeParameters();
    p.set("0", "doc9");
    EqualityIterator ei = new EqualityIterator(p, fields);

    assertFalse(ei.isDone());
    assertEquals(ei.currentCandidate(), 1);
    sc.document = ei.currentCandidate();
    assertTrue(ei.hasMatch(ei.currentCandidate()));
    assertFalse(ei.indicator(sc));
    ei.movePast(ei.currentCandidate());

    assertFalse(ei.isDone());
    assertEquals(ei.currentCandidate(), 9);
    sc.document = ei.currentCandidate();
    assertTrue(ei.hasMatch(ei.currentCandidate()));
    assertFalse(ei.indicator(sc));
    ei.movePast(ei.currentCandidate());

    assertFalse(ei.isDone());
    assertEquals(ei.currentCandidate(), 34);
    sc.document = ei.currentCandidate();
    assertTrue(ei.hasMatch(ei.currentCandidate()));
    assertTrue(ei.indicator(sc));
    ei.movePast(ei.currentCandidate());

    assertTrue(ei.isDone());
    reader.close();
  }
}
