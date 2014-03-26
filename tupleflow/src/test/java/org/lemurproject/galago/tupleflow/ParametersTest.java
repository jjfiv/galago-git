/*
 * BSD License (http://lemurproject.org/galago-license)

 */
package org.lemurproject.galago.tupleflow;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.util.*;

import static org.junit.Assert.*;

/**
 *
 * @author irmarc
 */
public class ParametersTest {
  @Test
  public void testCreation() {
    Parameters p = new Parameters();
    assertNotNull(p);
  }

  @Test
  public void testAddSimpleData() {
    Parameters p = new Parameters();

    // add boolean data
    p.set("testa", false);
    assertTrue(p.isBoolean("testa"));
    assertFalse(p.getBoolean("testa"));

    // remove
    p.remove("testa");
    assertFalse(p.isBoolean("testa"));

    // add long data
    p.set("t3", 50);
    assertTrue(p.isLong("t3"));
    assertEquals(50L, p.getLong("t3"));
    assertEquals(50, (int) p.getLong("t3"));

    // add double data
    p.set("tf", Math.PI);
    assertTrue(p.isDouble("tf"));
    assertFalse(p.isLong("tf"));
    assertFalse(p.isMap("tf"));
    assertEquals(Math.PI, p.getDouble("tf"), 0.001);

    // add String data
    p.set("str", "TestString");
    assertTrue(p.isString("str"));
    assertFalse(p.isLong("str"));
    assertEquals("TestString", p.getString("str"));
  }

  @Test
  public void testAddLists() throws Exception {
    ArrayList<String> a = new ArrayList<String>();
    a.add("woot");
    a.add("yeah");

    Parameters p = new Parameters();
    p.set("list", a);
    assertTrue(p.isList("list"));
    assertTrue(p.isList("list", String.class));
    assertFalse(p.isList("list", Parameters.class));

    List<String> recv = p.getList("list", String.class);
    assertEquals("woot", recv.get(0));
    assertEquals("yeah", recv.get(1));

    p.remove("list");
    assertFalse(p.isList("list"));
  }

  @Test
  public void testAddParameters() throws Exception {
    Parameters inner = new Parameters();

    inner.set("ib", true);
    inner.set("ii", 5L);

    assertEquals(true, inner.getBoolean("ib"));
    assertEquals(5L, inner.getLong("ii"));

    Parameters outer = new Parameters();
    outer.set("inside", inner);

    assertTrue(outer.isMap("inside"));
    Parameters recv = outer.getMap("inside");

    assertEquals(true, recv.getBoolean("ib"));
  }

  @Test
  public void testWritingAndReading() throws IOException {
    File tempPath = null;
    try {
      Parameters tokenizer = new Parameters();
      Parameters formats = new Parameters();
      formats.set("title", "string");
      formats.set("date", "date");
      formats.set("version", "int");
      tokenizer.set("formats", formats);
      String[] fields = {"title", "date", "version"};
      tokenizer.set("fields", Arrays.asList(fields));

      Parameters params = new Parameters();
      params.set("filename", "fictional/path");
      params.set("tokenizer", tokenizer);

      tempPath = FileUtility.createTemporary();
      params.write(tempPath.getAbsolutePath());

      // Now read it in.
      Parameters newParams = Parameters.parseFile(tempPath);
      assertEquals(params.toString(), newParams.toString());
      assertEquals(params, newParams);

      Parameters fromStringPath = Parameters.parseFile(tempPath.getAbsolutePath());
      assertEquals(params.toString(), fromStringPath.toString());
      assertEquals(params, fromStringPath);

    } finally {
      if (tempPath != null) {
        Assert.assertTrue(tempPath.delete());
      }
    }
  }

  @Test
  public void testParseAndGenerateParameters() throws Exception {
    StringBuilder json = new StringBuilder();
    json.append("{ \"inner\" : { \"inner1\" : 1 , \"inner2\" : \"jackal\" } ");
    json.append(", \"key1\" : true ");
    json.append(", \"key2\" : false , \"key3\" : null ");
    json.append(", \"key4\" : [ 0 , 1 , 2 , 3 , 4 , 5 , 6 ] , \"key5\" : -4.56 }");


    Parameters p = Parameters.parseString(json.toString());

    assertTrue(p.isBoolean("key1"));
    assertTrue(p.getBoolean("key1"));
    assertTrue(p.isBoolean("key2"));
    assertFalse(p.getBoolean("key2"));

    assertTrue(p.getString("key3") == null);
    assertTrue(p.isList("key4"));
    List l = p.getList("key4");
    for (int i = 0; i < l.size(); i++) {
      assertEquals((long) i, ((Long) l.get(i)).longValue());
    }

    String output = p.toString();
    assertEquals(output, json.toString());

    Parameters clone = p.clone();
    assertEquals(p.toString(), clone.toString());
  }

  @Test
  public void testCommandLineArgs() throws Exception {
    String[] args = new String[10];
    args[0] = "--arrayKey+val1";
    args[1] = "--arrayKey+val2";
    args[2] = "--arrayKey+val3";

    args[3] = "--intKey=4";
    args[4] = "--mapKey/list+7";
    args[5] = "--mapKey/list+8";
    args[6] = "--mapKey/list+9";
    args[7] = "--mapKey/innerVal=bob";
    args[8] = "--mapKey/isTrue";
    args[9] = "--mapKey/innerMap/wayInnerMap/buriedKey=absolutely";

    Parameters p = Parameters.parseArgs(args);
    System.err.flush();
    List<String> list = p.getList("arrayKey", String.class);
    assertEquals("val1", list.get(0));
    assertEquals("val2", list.get(1));
    assertEquals("val3", list.get(2));
    assertEquals(4L, p.getLong("intKey"));
    Parameters inner = p.getMap("mapKey");
    List<Long> ints = inner.getList("list", Long.class);
    assertEquals(7L, ints.get(0).longValue());
    assertEquals(8L, ints.get(1).longValue());
    assertEquals(9L, ints.get(2).longValue());
    assertEquals("bob", inner.getString("innerVal"));
    assertTrue(inner.getBoolean("isTrue"));
    Parameters innerAgain = inner.getMap("innerMap");
    Parameters innerOnceMore = innerAgain.getMap("wayInnerMap");
    assertEquals("absolutely", innerOnceMore.getString("buriedKey"));
  }

  @Test
  public void testPrettyPrinter() throws Exception {
    Parameters tokenizer = new Parameters();
    Parameters formats = new Parameters();
    formats.set("title", "string");
    formats.set("date", "date");
    formats.set("version", "int");
    tokenizer.set("formats", formats);
    String[] fields = {"title", "date", "version"};
    tokenizer.set("fields", Arrays.asList(fields));
    ArrayList<Parameters> pList = new ArrayList<Parameters>();
    pList.add(Parameters.parseString("{\"text\":\"query text one\", \"number\":\"10\"}"));
    pList.add(Parameters.parseString("{\"text\":\"query text two\", \"number\":\"11\"}"));

    Parameters params = new Parameters();
    params.set("filename", "fictional/path");
    params.set("tokenizer", tokenizer);
    params.set("paramList", pList);

    String prettyString = params.toPrettyString();

    Parameters reParsed = Parameters.parseString(prettyString);
    assert (reParsed.equals(params));
  }

  @Test
  public void testTrailingCommas() throws Exception {
    Parameters test = Parameters.parseString(" { \"foo\" : [1, 2,3,\t],\n}");
    assertTrue(test.isList("foo"));
    assertEquals(3, test.getList("foo").size());
  }

  @Test
  public void testParseMap() {
    Map<String,String> data = new HashMap<String,String>();
    data.put("keyA", "0");
    data.put("keyB", "1");
    Parameters test = Parameters.parseMap(data);

    assertEquals(0, test.getLong("keyA"));
    assertEquals(1, test.getLong("keyB"));
    assertEquals("0", test.getAsString("keyA"));
    assertEquals("1", test.getAsString("keyB"));
    assertEquals(data.size(), test.size());
  }

  @Test
  public void testWriteAndRead() throws IOException {
    Parameters truth = complicated();
    Parameters same0 = Parameters.parseReader(new StringReader(truth.toString()));
    assertEquals(truth.toString(), same0.toString());
    assertEquals(truth, same0);
    Parameters same1 = Parameters.parseString(same0.toString());
    assertEquals(truth.toString(), same1.toString());
    assertEquals(truth, same1);

  }

  @Test
  public void testCopyTo() throws IOException {
    Parameters truth = complicated();
    Parameters newP = new Parameters();
    truth.copyTo(newP);
    assertEquals(truth.toString(), newP.toString());
    assertEquals(truth, newP);
  }

  @Test
  public void testEscaping() throws IOException {
    Parameters truth = new Parameters();
    truth.set("withAQuote!", "here it comes \" to wreck the day...");
    truth.set("withANewline!", "here it comes \n to wreck the day...");
    truth.set("withABackslash!", "here it comes \\ to wreck the day...");
    truth.set("too much!", "\\\r\n\t\b\f \\hmm\\ \f\b\n\r\\");
    truth.set("C:\\", "busted keys \f\b\n\r\\");

    Parameters same = Parameters.parseString(truth.toString());
    for(String key : truth.keySet()) {
      assertEquals(truth.get(key), same.get(key));
    }
  }

  public static Parameters complicated() {
    Parameters p = new Parameters();
    p.set("bool-t", true);
    p.set("bool-f", false);
    p.set("long-a", 120L);
    p.set("long-b", 0xdeadbeefL);
    p.set("double-pi", Math.PI);
    p.set("double-neg-e", -Math.exp(1));
    p.set("list-a", Arrays.asList(true, false, "bar", "foo", Math.PI, -Math.exp(1), p.clone()));
    p.set("list-b", Collections.EMPTY_LIST);
    p.set("map-a", p.clone());
    p.set("map-b", new Parameters());

    return p;
  }

}

