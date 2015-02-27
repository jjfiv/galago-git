package org.lemurproject.galago.utility.json;

import org.junit.Test;

import static org.junit.Assert.*;
import static org.lemurproject.galago.utility.json.JSONUtil.escape;
import static org.lemurproject.galago.utility.json.JSONUtil.unescape;

public class JSONUtilTest {

  @Test
  public void testUnescape() throws Exception {

    String testShort = "Eating a piece of \u03c0 (pi)";
    assertEquals("Eating a piece of \\u03c0 (pi)", escape(testShort));

    String testLong = "I stole this guy from wikipedia: \ud83d\ude02"; // emoji "face with tears of joy"
    assertEquals("I stole this guy from wikipedia: \\ud83d\\ude02", escape(testLong));

    String testQuote = "here it comes \" to wreck the day...";
    assertEquals("here it comes \\\" to wreck the day...", escape(testQuote));
    String testNewline = "here it comes \n to wreck the day...";
    assertEquals("here it comes \\n to wreck the day...", escape(testNewline));
    String testBackslash = "here it comes \\ to wreck the day...";
    assertEquals("here it comes \\\\ to wreck the day...", escape(testBackslash));

    String testAsciiEscapes = "\\\r\n\t\b\f \\hmm\\ \f\b\n\r\\";
    assertEquals(testAsciiEscapes, unescape(escape(testAsciiEscapes)));
    assertEquals(testLong, unescape(escape(testLong)));
    assertEquals(testShort, unescape(escape(testShort)));

  }
}