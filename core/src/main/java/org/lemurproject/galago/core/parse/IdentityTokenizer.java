// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Pattern;
import org.lemurproject.galago.tupleflow.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Linkage;
import org.lemurproject.galago.tupleflow.NullProcessor;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.Processor;
import org.lemurproject.galago.tupleflow.Source;
import org.lemurproject.galago.tupleflow.Step;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 * A small class to echo the Document as is down the pipeline.
 * This is useful when the parser in fact does all the work already.
 * @author irmarc
 */

@Verified
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
@OutputClass(className = "org.lemurproject.galago.core.parse.Document")
public class IdentityTokenizer implements Source<Document>, Processor<Document> {

    public Processor<Document> processor = new NullProcessor(Document.class);    

  public IdentityTokenizer(TupleFlowParameters parameters) {
  }
    
  public void process(Document document) throws IOException {
    processor.process(document);
  }

  public void setProcessor(final Step processor) throws IncompatibleProcessorException {
    Linkage.link(this, processor);
  }

  public void close() throws IOException {
    processor.close();
  }

  public Class<Document> getInputClass() {
    return Document.class;
  }

  public Class<Document> getOutputClass() {
    return Document.class;
  }
}