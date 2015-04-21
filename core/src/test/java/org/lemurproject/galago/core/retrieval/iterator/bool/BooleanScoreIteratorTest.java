package org.lemurproject.galago.core.retrieval.iterator.bool;

import org.junit.Test;
import org.lemurproject.galago.core.index.mem.MemoryIndex;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.TagTokenizer;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.tokenize.Tokenizer;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.utility.Parameters;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;


/**
 * Testing the new #bool() operation.
 * @author jfoley.
 */
public class BooleanScoreIteratorTest {
	static Tokenizer tok = TagTokenizer.create(Parameters.create());

	static Document makeBooleanDocument(String name, String... terms) {
		Document doc = new Document();
		doc.text = Utility.join(terms);
		doc.name = name;
		tok.tokenize(doc);
		return doc;
	}

	static Set<String> matchingDocuments(LocalRetrieval ret, String query) throws Exception {
		Node sq = StructuredQuery.parse(query);
		System.err.println(sq);
		return ret.transformAndExecuteQuery(sq).resultSet();
	}

	static Set<String> mkSet(String... data) {
		return new HashSet<>(Arrays.asList(data));
	}

	@Test
	public void testBooleanAnd() throws Exception {
		MemoryIndex index = new MemoryIndex();
		index.process(makeBooleanDocument("1", "a", "b", "c"));
		index.process(makeBooleanDocument("2",      "b", "c", "d"));
		index.process(makeBooleanDocument("3",           "c", "d", "e"));

		LocalRetrieval ret = new LocalRetrieval(index);
		assertEquals(mkSet("1", "2"), matchingDocuments(ret, "#bool(#band(b c))"));
		assertEquals(mkSet("2", "3"), matchingDocuments(ret, "#bool(#band(c d))"));
		assertEquals(mkSet("3"), matchingDocuments(ret, "#bool(#band(c d e))"));
		assertEquals(Collections.<String>emptySet(), matchingDocuments(ret, "#bool(#band(a d))"));
	}

	@Test
	public void testBooleanOr() throws Exception {
		MemoryIndex index = new MemoryIndex();
		index.process(makeBooleanDocument("1", "a", "b", "c"));
		index.process(makeBooleanDocument("2",      "b", "c", "d"));
		index.process(makeBooleanDocument("3",           "c", "d", "e"));

		LocalRetrieval ret = new LocalRetrieval(index);
		assertEquals(mkSet("1", "2", "3"), matchingDocuments(ret, "#bool(#bor(b c))"));
		assertEquals(mkSet("1", "2", "3"), matchingDocuments(ret, "#bool(#bor(c d))"));
		assertEquals(mkSet("1", "3"), matchingDocuments(ret, "#bool(#bor(a e))"));
		assertEquals(mkSet("2", "3"), matchingDocuments(ret, "#bool(#bor(d e))"));
		assertEquals(mkSet("1", "2"), matchingDocuments(ret, "#bool(#bor(a b))"));
		assertEquals(Collections.<String>emptySet(), matchingDocuments(ret, "#bool(#bor(z x))"));
	}
}