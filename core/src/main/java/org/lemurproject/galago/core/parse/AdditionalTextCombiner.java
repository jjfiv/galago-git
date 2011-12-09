// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.IOException;
import org.lemurproject.galago.core.types.AdditionalDocumentText;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.TypeReader;
import org.lemurproject.galago.tupleflow.execution.ErrorHandler;
import org.lemurproject.galago.tupleflow.execution.Verification;

/**
 * Adds tuples of type AdditionalDocumentText to the end of the text field in
 * a document.  The AdditionalDocumentText stream is specified in the
 * textSource parameter.  This stage should be used before document tokenizing.
 * 
 * @author trevor
 */
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
@OutputClass(className = "org.lemurproject.galago.core.parse.Document")
public class AdditionalTextCombiner extends StandardStep<Document, Document> {

  TupleFlowParameters parameters;
  String readerName;
  int currentFileId;
  TypeReader<AdditionalDocumentText> text;
  AdditionalDocumentText last;

  @SuppressWarnings("unchecked")
  public AdditionalTextCombiner(TupleFlowParameters parameters) throws IOException {
    this.parameters = parameters;
    this.readerName = parameters.getJSON().getString("textSource");
    this.currentFileId = -1;
  }

  @Override
  public void process(Document document) throws IOException {
    // if we have a new file input - reset the links reader to ensure we catch any links
    if(document.fileId != currentFileId){
      text = parameters.getTypeReader(readerName);
      last = text.read();
    }
    
    while (last != null && last.identifier < document.identifier) {
      last = text.read();
    }

    if (last != null && last.identifier == document.identifier) {
      document.text += last.text;
    }

    processor.process(document);
  }
  
  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    if (!Verification.requireParameters(new String[]{"textSource"}, parameters.getJSON(), handler)) {
      return;
    }

    String readerName = parameters.getJSON().getString("textSource");
    Verification.verifyTypeReader(readerName, AdditionalDocumentText.class, parameters, handler);
  }
}
