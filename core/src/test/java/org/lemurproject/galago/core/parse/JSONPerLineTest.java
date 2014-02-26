// BSD License (http://lemurproject.org/galago-license)

package org.lemurproject.galago.core.parse;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import static org.junit.Assert.assertEquals;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;
import org.lemurproject.galago.core.retrieval.Results;
import org.lemurproject.galago.core.retrieval.Retrieval;
import org.lemurproject.galago.core.retrieval.RetrievalFactory;
import org.lemurproject.galago.core.retrieval.ScoredDocument;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.tokenize.Tokenizer;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author jfoley
 */
@RunWith(JUnit4.class)
public class JSONPerLineTest {
	
	public static int NumDocuments = 10;
	public static String commonWord = "foo";
	public static String countWord = "bar";
	
	private static ArrayList<Parameters> fakeJSONDocs() {
		ArrayList<Parameters> pdocs = new ArrayList<Parameters>();
		
		for(int i=0; i<NumDocuments; i++) {
			Parameters doc = new Parameters();
			doc.set("id", i);
			StringBuilder text = new StringBuilder();
			text.append(commonWord);
			for(int j=0; j<i; j++) {
				text.append(' ').append(countWord);
			}
			doc.set("text", text.toString());
			pdocs.add(doc);
		}
		return pdocs;
	}
	
	private static String fakeJSONText(List<Parameters> jsonDocs) {
		StringBuilder inputText = new StringBuilder();
		for(Parameters p : jsonDocs) {
			inputText.append(p.toString()).append('\n').append('\n');
		}
		return inputText.toString();
	}
	
	@Test
	public void simpleParse() throws IOException {
		ArrayList<Parameters> pdocs = fakeJSONDocs();
		String toParseText = fakeJSONText(pdocs);
		
		Parameters parseParams = new Parameters();
		parseParams.set("documentNameField", "id");
		
		Tokenizer tokenizer =	Tokenizer.instance(parseParams);
		
		ArrayList<Document> docs = new ArrayList<Document>();
		
		File tmp = null;
		try {
			tmp = FileUtility.createTemporary();
			Utility.copyStringToFile(toParseText, tmp);

			DocumentSplit docsplit = new DocumentSplit();
			docsplit.fileName = tmp.getAbsolutePath();
			
			JSONPerLineParser jsonPerLineParser = new JSONPerLineParser(docsplit, parseParams);
			while(true) {
				Document d = jsonPerLineParser.nextDocument();
				if(d == null) {
					break;
				}
				tokenizer.process(d);
				docs.add(d);
			}
		} finally {
			if(tmp != null) {
				tmp.delete();
			}
		}
		
		assertEquals(docs.size(), pdocs.size());
		for(int i=0; i<docs.size(); i++) {
			Document di = docs.get(i);
			String idstr = Integer.toString(i);
			assertEquals(idstr, di.name);
			// System.out.println(Utility.join(di.terms, ", "));
			// id#, foo, bar*i
			assertEquals(i+2, di.terms.size());
			assertEquals(idstr, di.terms.get(0));
			assertEquals(commonWord, di.terms.get(1));
			for(int j=2; j<di.terms.size(); j++) {
				assertEquals(countWord, di.terms.get(2));
			}
		}
		
	}
	
	private static List<ScoredDocument> rankingForTerm(Retrieval ret, String which) throws Exception {
		Node query = ret.transformQuery(Node.Text(which), new Parameters());
		Results results = ret.executeQuery(query, new Parameters());
		return results.scoredDocuments;
	}

	@Test
	public void buildRoutineTest() throws IOException, Exception {
		ArrayList<Parameters> pdocs = fakeJSONDocs();
		String toParseText = fakeJSONText(pdocs);
		
		List externalParsers = new ArrayList();
		Parameters kind = new Parameters();
		kind.set("filetype", "linejson");
		kind.set("class", JSONPerLineParser.class.getCanonicalName());
		externalParsers.add(kind);
		
		Parameters buildParams = new Parameters();
		buildParams.set("parser", new Parameters());
		buildParams.getMap("parser").set("externalParsers", externalParsers);
		
		buildParams.set("filetype", "linejson");
		buildParams.getMap("parser").set("documentNameField", "id");
		
		File tmp = null;
		File tmpIndexDir = null;

		try {
			tmp = FileUtility.createTemporary();
			Utility.copyStringToFile(toParseText, tmp);
			tmpIndexDir = FileUtility.createTemporaryDirectory();

			buildParams.set("inputPath", tmp.getAbsolutePath());
			buildParams.set("indexPath", tmpIndexDir.getAbsolutePath());
			App.run("build", buildParams, System.out);
			
			Retrieval ret = RetrievalFactory.instance(tmpIndexDir.getAbsolutePath());
			List<ScoredDocument> countWordRanking = rankingForTerm(ret, countWord);
			List<ScoredDocument> commonWordRanking = rankingForTerm(ret, commonWord);
			
			assertEquals(countWordRanking.size(), NumDocuments-1);
			assertEquals(commonWordRanking.size(), NumDocuments);

			
		} finally {
			if(tmp != null) {
				tmp.delete();
			}
			if(tmpIndexDir != null) {
				tmpIndexDir.delete();
			}
		}
		
		
		
	}


}
