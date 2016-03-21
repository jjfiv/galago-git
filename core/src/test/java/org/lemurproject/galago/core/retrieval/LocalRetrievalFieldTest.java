// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.lemurproject.galago.core.retrieval.query.Node;
import org.lemurproject.galago.core.retrieval.query.StructuredQuery;
import org.lemurproject.galago.core.tools.App;
import org.lemurproject.galago.core.tools.AppTest;
import org.lemurproject.galago.tupleflow.FileUtility;
import org.lemurproject.galago.utility.FSUtil;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.utility.StreamUtil;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 *
 * @author MichaelZ
 */
public class LocalRetrievalFieldTest {

    File tempPath;

    public static File make10DocIndexWithFields(Boolean includeFormat) throws Exception {
        File trecCorpusFile, corpusFile, indexFile;

        // create a simple doc file, trec format:
        trecCorpusFile = FileUtility.createTemporary();
        StreamUtil.copyStringToFile(
                AppTest.trecDocument("1", "<title>document one</title><author>Max D Cat</author>This is a sample document")
                        + AppTest.trecDocument("2", "<title>document two</title><author>Max D Cat</author>The cat jumped over the moon")
                        + AppTest.trecDocument("3", "<title>document three</title><author>Bill Shakespeare</author>If the shoe fits, it's ugly")
                        + AppTest.trecDocument("4", "<title>document four</title>Though a program be but three lines long, someday it will have to be maintained.")
                        + AppTest.trecDocument("5", "<title>document five</title>To be trusted is a greater compliment than to be loved")
                        + AppTest.trecDocument("6", "<title>document six</title> Just because everything is different doesn't mean anything has changed.")
                        + AppTest.trecDocument("7", "<title>document seven</title> everything everything jumped sample ugly")
                        + AppTest.trecDocument("8", "<title>document eight</title> though cat moon cat cat cat")
                        + AppTest.trecDocument("9", "<title>document nine</title> document document document document")
                        + AppTest.trecDocument("10", "<title>document ten</title> program fits"),
                trecCorpusFile);

        indexFile = FileUtility.createTemporaryDirectory();

       Parameters params = Parameters.create();

        params.set("indexPath", indexFile.getAbsolutePath());
        params.set("inputPath", trecCorpusFile.getAbsolutePath());
        params.set("indexPath", indexFile.getAbsolutePath());
        params.set("tokenizer", Parameters.create());
        params.getMap("tokenizer").set("fields", Arrays.asList(new String[]{"title", "author"}));
        params.getMap("tokenizer").set("formats", Parameters.create());

        if (includeFormat){
            params.getMap("tokenizer").getMap("formats").set("title", "string");
            params.getMap("tokenizer").getMap("formats").set("author", "string");
        }

        App.run("build", params, System.out);

        AppTest.verifyIndexStructures(indexFile);

        return indexFile;
    }

    @Before
    public void setUp() throws Exception {
        this.tempPath = make10DocIndexWithFields(false);
    }

    @After
    public void tearDown() throws IOException {
        FSUtil.deleteDirectory(tempPath);
    }

    @Test
    public void testFields() throws Exception {

        Parameters p = Parameters.create();

        LocalRetrieval retrieval = new LocalRetrieval(tempPath.toString(), p);

        // test ordered window
        String query = "#combine( #ordered:1( moon cat ) )";
        Node root = StructuredQuery.parse(query);
        root = retrieval.transformQuery(root, p);

        p.set("requested", 10);
        List<ScoredDocument> results = retrieval.executeQuery(root, p).scoredDocuments;

        assertEquals(1, results.size());
        assertEquals("8", results.get(0).documentName);

        // odered window in a field
        query = "#combine( #inside( #ordered:1( #text:max() #text:d() #text:cat() ) #field:author() ) )";
        root = StructuredQuery.parse(query);
        root = retrieval.transformQuery(root, p);

        results = retrieval.executeQuery(root, p).scoredDocuments;

        assertEquals(2, results.size());
        assertEquals("1", results.get(0).documentName);
        assertEquals("2", results.get(1).documentName);

        // ordered window in a field where the query terms don't exist
        query = "#combine( #inside( #ordered:1( #text:max() #text:d() #text:cat() ) #field:title() ) )";
        root = StructuredQuery.parse(query);
        root = retrieval.transformQuery(root, p);

        results = retrieval.executeQuery(root, p).scoredDocuments;

        assertEquals(0, results.size());

        // term in a field that occurs in text
        query = "#combine( #inside( #text:cat() #field:author() ) )";
        root = StructuredQuery.parse(query);
        root = retrieval.transformQuery(root, p);

        results = retrieval.executeQuery(root, p).scoredDocuments;

        assertEquals(2, results.size());
        assertEquals("1", results.get(0).documentName);
        assertEquals("2", results.get(1).documentName);

    }

}
