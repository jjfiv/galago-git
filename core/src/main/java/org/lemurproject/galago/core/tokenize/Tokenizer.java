// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.tokenize;

import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.TagTokenizer;
import org.lemurproject.galago.tupleflow.*;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author jfoley
 */
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
@OutputClass(className = "org.lemurproject.galago.core.parse.Document")
public abstract class Tokenizer implements Source<Document>, Processor<Document> {

  public Tokenizer(TupleFlowParameters parameters) { }
  
  /**
   * Parses the text in the document.text attribute and fills in the
   * document.terms and document.tags arrays.
   *
   * @param input Document with text and possibly metadata filled out
   */
  public abstract void tokenize(Document input);
  
  /**
   * For generating documents from strings
   */
  private long gensym = 0;
  
  /**
   * Create a document from an input string.
   * @param input
   * @return 
   */
  public Document tokenize(String input) {
    Document d = new Document();
    d.text = input;
    d.name = "unkn-"+gensym;
    d.identifier = gensym;
    gensym++;
    tokenize(d);
    return d;
  }
  
  
  /**
   * Tupleflow-specific information.
   */
  public Processor<Document> processor = new NullProcessor(Document.class);

  /**
   * Setup for Tupleflow running.
   * @param processor The next stage for Tupleflow, to pass the Document onward.
   * @throws IncompatibleProcessorException 
   */
  @Override
  public void setProcessor(Step processor) throws IncompatibleProcessorException {
    Linkage.link(this, processor);
  }

  /**
   * Parses the text in the document.text attribute and fills in the
   * document.terms and document.tags arrays, then passes that document
   * to the next processing stage.
   *
   * @param input
   * @throws java.io.IOException
   */
  @Override
  public void process(Document input) throws IOException {
    tokenize(input);
    processor.process(input);
  }

  @Override
  public void close() throws IOException {
    processor.close();
  }
  
  /**
   * Try to convince Tupleflow that this accepts Documents as input.
   * @return 
   */
  public Class<Document> getInputClass() {
    return Document.class;
  }
  
  /**
   * Try to convince Tupleflow that this produces Documents as output.
   * @return 
   */
  public Class<Document> getOutputClass() {
    return Document.class;
  }
  
  public static Class<? extends Tokenizer> getTokenizerClass(Parameters p) {
    if (p.isString("tokenizerClass")) {
      try {
        return (Class<? extends Tokenizer>) Class.forName(p.getString("tokenizerClass"));
      } catch (ClassNotFoundException ex) {
        Logger.getLogger(Tokenizer.class.getName()).log(Level.SEVERE, null, ex);
      }
    } else if (p.isMap("tokenizer")) {
      return getTokenizerClass(p.getMap("tokenizer"));
    }
    return TagTokenizer.class;
  }
  
    
  public static Tokenizer instance(Parameters p) {
    return instance(new FakeParameters(p));
  }

  public static Tokenizer instance(TupleFlowParameters tp) {
    Tokenizer tokenizer = null;
    Parameters inputParms = tp.getJSON();
    Parameters tokenizerParms = new Parameters();

    //--- pull out tokenizer options if available
    if(inputParms.isMap("tokenizer")) {
      tokenizerParms = inputParms.getMap("tokenizer");
    }

    try {
      Class<? extends Tokenizer> tokenizerClass = getTokenizerClass(inputParms);      
      Constructor[] constructors = tokenizerClass.getConstructors();

      for (Constructor c : constructors) {
        java.lang.reflect.Type[] parameters = c.getGenericParameterTypes();

        if(parameters.length == 1 && parameters[0] == TupleFlowParameters.class) {
          tokenizer = (Tokenizer) c.newInstance(new FakeParameters(tokenizerParms));
          break;
        }
      }
    } catch (Exception ex) {
      Logger.getLogger(Tokenizer.class.getName()).log(Level.SEVERE, null, ex);
    }

    //--- fall back to TagTokenizer if we couldn't find anything
    if(tokenizer == null) {
      tokenizer = new TagTokenizer(new FakeParameters(tokenizerParms));
    }
    return tokenizer;
  }
}
