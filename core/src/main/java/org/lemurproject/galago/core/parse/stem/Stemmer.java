/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.parse.stem;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.tupleflow.error.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.Linkage;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.utility.Parameters;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.Source;
import org.lemurproject.galago.tupleflow.Step;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.execution.ErrorStore;
import org.lemurproject.galago.utility.reflection.ReflectUtil;

/**
 * 
 * 
 * @author sjh
 */
public abstract class Stemmer implements Source<Document>, Processor<Document> {

  // each create of Stemmer should have it's own lock
  final Object lock = new Object();
  
  long cacheLimit = 50000;
  HashMap<String, String> cache = new HashMap<>();
  public Processor<Document> processor;

  @Override
  public void process(Document document) throws IOException {
    processor.process(stem(document));
  }

  @Override
  public void close() throws IOException {
    processor.close();
  }

  @Override
  public void setProcessor(Step processor) throws IncompatibleProcessorException {
    Linkage.link(this, processor);
  }

  public static void verify(TupleFlowParameters fullParameters, ErrorStore store) {
  }

  public static String getInputClass(TupleFlowParameters parameters) {
    return Document.class.getCanonicalName();
  }

  public static String getOutputClass(TupleFlowParameters parameters) {
    return Document.class.getCanonicalName();
  }

  public static String[] getOutputOrder(TupleFlowParameters parameters) {
    return new String[0];
  }

  public Document stem(Document document) {
    // new document is necessary - stemmed terms were being propagated unintentially
    Document newDocument = new Document(document);
    List<String> words = newDocument.terms;
    for (int i = 0; i < words.size(); i++) {
      String word = words.get(i);
      words.set(i, stem(word));
    }
    return newDocument;
  }

  public String stem(String term) {
    if (cache.containsKey(term)) {
      return cache.get(term);
    }
    String stemmedTerm;
    
    synchronized (lock) {
      stemmedTerm = stemTerm(term);
    }
    
    if (!cache.containsKey(stemmedTerm)) {
      cache.put(term, stemmedTerm);
    }
    if (cache.size() > cacheLimit) {
      cache.clear();
    }
    return stemmedTerm;
  }
  
  public final String stemAsRequired(String term) {
    if(term.contains("~")) {
      return this.stemWindow(term);
    } else {
      return this.stem(term);
    }
  }

  // This function should only be use synchronously (see 'lock')
  protected abstract String stemTerm(String term);

  /**
   * allows stemming of windows.
   */
  public String stemWindow(String term) {
    StringBuilder window = new StringBuilder();
    for (String t : term.split("~")) {
      if (window.length() > 0) {
        window.append("~");
      }
      window.append(stem(t));
    }
    return window.toString();
  }

  /**
   * @deprecated use create instead.
   */
  @Deprecated
  public static Stemmer instance(Parameters p) throws ReflectiveOperationException {
    return create(p);
  }
  public static Stemmer create(Parameters p) throws ReflectiveOperationException {
    if(p.containsKey("stemmer")) {
      return ReflectUtil.instantiate(p.getString("stemmer"));
    }
    return new NullStemmer();
  }
}
