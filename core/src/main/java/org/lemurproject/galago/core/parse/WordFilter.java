// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.lemurproject.galago.tupleflow.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Linkage;
import org.lemurproject.galago.tupleflow.NullProcessor;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.Source;
import org.lemurproject.galago.tupleflow.Step;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;

/**
 * WordFilter filters out unnecessary words from documents.  Typically this object
 * takes a stopword list as parameters and removes all the listed words.  However, 
 * this can also be used to keep only the specified list of words in the index, which
 * can be used to create an index that is tailored for only a small set
 * of experimental queries.
 * 
 * @author trevor
 */
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
@OutputClass(className = "org.lemurproject.galago.core.parse.Document")
public class WordFilter implements Processor<Document>, Source<Document> {
    Set<String> stopwords = new HashSet<String>();
    boolean keepListWords = false;
    public Processor<Document> processor = new NullProcessor(Document.class);

    public WordFilter(HashSet<String> words) {
        stopwords = words;
    }

    public WordFilter(TupleFlowParameters params) throws IOException {
        if (params.getJSON().containsKey("filename")) {
            String filename = params.getJSON().getString("filename");
            stopwords = Utility.readFileToStringSet(new File(filename));
        } else {
            stopwords = new HashSet(params.getJSON().getList("stopwords"));
        }

        keepListWords = params.getJSON().get("keepListWords", false);
    }

    public void process(Document document) throws IOException {
        List<String> words = document.terms;

        for (int i = 0; i < words.size(); i++) {
            String word = words.get(i);
            boolean wordInList = stopwords.contains(word);
            boolean removeWord = wordInList != keepListWords;

            if (removeWord) {
                words.set(i, null);
            }
        }

        processor.process(document);
    }

    public void close() throws IOException {
        processor.close();
    }

    public static void verify(TupleFlowParameters parameters, ErrorStore store) {
        if (parameters.getJSON().containsKey("filename")) {
            return;
        }
        if (parameters.getJSON().getList("word").isEmpty()) {
            store.addWarning("Couldn't find any words in the stopword list.");
        }
    }

    @Override
    public void setProcessor(Step processor) throws IncompatibleProcessorException {
        Linkage.link(this, processor);
    }
}
