package org.lemurproject.galago.utility.tools;

import org.junit.Test;
import org.lemurproject.galago.utility.Parameters;

import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

public class ArgumentsTest {

	@Test
	public void testLooksLikeKey() throws Exception {
		assertTrue(Arguments.looksLikeKey("--foo"));
		assertTrue(Arguments.looksLikeKey("--bar=baz"));
		assertFalse(Arguments.looksLikeKey("bar=baz"));
	}

	@Test
	public void testEndsWithJoinOperator() throws Exception {
		assertFalse(Arguments.endsWithJoinOperator("--foo"));
		assertTrue(Arguments.endsWithJoinOperator("--bar="));
		assertTrue(Arguments.endsWithJoinOperator("bar+"));
		assertTrue(Arguments.endsWithJoinOperator("bar="));
		assertFalse(Arguments.endsWithJoinOperator("bar/"));
		assertFalse(Arguments.endsWithJoinOperator("bar?"));
		assertFalse(Arguments.endsWithJoinOperator("bar "));
	}

	@Test
	public void testCombineAdjacentIfReasonable() throws Exception {
		assertEquals(
			Arrays.asList(
				"--foo=1", "--bar=2", "--baz+3", "--zed+"),
			Arguments.combineAdjacentIfReasonable(new String[] {
				"--foo=", "1", "--bar=2", "--baz+", "3", "--zed+"}));

		assertEquals(
			Arrays.asList(
				"--bar=", "--baz"),
			Arguments.combineAdjacentIfReasonable(new String[] {
				"--bar=", "--baz"}));
	}

	@Test
	public void testIndexOrMax() throws Exception {
		assertEquals(Integer.MAX_VALUE, Arguments.indexOrMax("foo", 'Z'));
		assertEquals(0, Arguments.indexOrMax("foo", 'f'));
	}

	@Test
	public void testCommandLineArgs() throws Exception {
		String[] args = new String[]{
			"--arrayKey+val1",
			"--arrayKey+val2",
			"--arrayKey+val3",

			"--intKey=4",
			"--mapKey/list+7",
			"--mapKey/list+8",
			"--mapKey/list+9",
			"--mapKey/innerVal=bob",
			"--mapKey/isTrue",
			"--mapKey/innerMap/wayInnerMap/buriedKey=absolutely",
		};

		Parameters p = Arguments.parse(args);
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
		assertNotNull(innerAgain);
		Parameters innerOnceMore = innerAgain.getMap("wayInnerMap");
		assertNotNull(innerOnceMore);
		assertEquals("absolutely", innerOnceMore.getString("buriedKey"));
	}
}