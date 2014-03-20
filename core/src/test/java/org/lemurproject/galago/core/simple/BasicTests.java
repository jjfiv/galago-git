// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.simple;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import junit.framework.Assert;
import static junit.framework.Assert.assertTrue;
import junit.framework.TestCase;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.retrieval.LocalRetrieval;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.core.tools.AppTest;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Utility;

/**
 *
 * @author jfoley
 */
public class BasicTests extends TestCase {
	public File inputFile;
	public File indexPath;
	public LocalRetrieval retrieval;
	
	public static final Map<String,String> data = new HashMap<String,String>();
	static {
		data.put("1","This is a sample document");
    data.put("2", "The cat jumped over the moon");
    data.put("3", "If the shoe fits, it's ugly");
    data.put("4", "Though a program be but three lines long, someday it will have to be maintained.");
    data.put("5", "To be trusted is a greater compliment than to be loved");
    data.put("6", "Just because everything is different doesn't mean anything has changed.");
    data.put("7", "everything everything jumped sample ugly");
    data.put("8", "though cat moon cat cat cat");
    data.put("9", "document document document document");
    data.put("10", "program fits");
	}
	
	public void createIndex() throws IOException, Exception {
    // create a simple doc file, trec format:
    StringBuilder trecCorpus = new StringBuilder();
		for(String name : data.keySet()) {
			trecCorpus.append(AppTest.trecDocument(name, data.get(name)));
		}
    
    inputFile = FileUtility.createTemporary();
    Utility.copyStringToFile(trecCorpus.toString(), inputFile);

		assertTrue(inputFile.exists());

    // now, try to build an index from that
    indexPath = FileUtility.createTemporaryDirectory();
    App.main(new String[]{"build", "--stemmedPostings=false", "--indexPath=" + indexPath.getAbsolutePath(),
              "--inputPath=" + inputFile.getAbsolutePath()});
		
		assertTrue(indexPath.exists());

    AppTest.verifyIndexStructures(indexPath);
		
		this.retrieval = new LocalRetrieval(indexPath.getAbsolutePath(), new Parameters());
	}
	
	@Override
	public void setUp() throws Exception {
		createIndex();
	}
	
	@Override
	public void tearDown() throws IOException {
		if(inputFile.exists()) inputFile.delete();
		if(indexPath.exists()) indexPath.delete();
		retrieval.close();
	}
	
	
	public void testGetDocument() throws IOException {
		Document doc = retrieval.getDocument("10", new Document.DocumentComponents());
  	Assert.assertNotNull(doc);
		Assert.assertEquals("10", doc.name);
		Assert.assertNull(doc.terms); // it shouldn't tokenize by default
		
		doc = retrieval.getDocument("10", new Document.DocumentComponents(true, true, true));
		Assert.assertNotNull(doc);
		Assert.assertEquals(2, doc.terms.size());
	}
	
	public void testGetDocuments() throws IOException {
		List<String> documents = Arrays.asList("6","1","3","5","9");
		Map<String, Document> fromCorpus = retrieval.getDocuments(documents, new Document.DocumentComponents(true, true, true));
		
		Assert.assertEquals(fromCorpus.size(), documents.size());
		
		for(String name : fromCorpus.keySet()) {
			Document pulled = fromCorpus.get(name);
			Assert.assertTrue(documents.contains(name));
			Assert.assertEquals(pulled.text, data.get(name));
		}
	}
}
