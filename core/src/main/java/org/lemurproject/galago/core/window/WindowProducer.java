// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.window;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.tupleflow.Counter;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.TupleFlowParameters;
import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 * <p> Produces windows consisting of n words. </p>
 *
 * <p> Windows
 * 
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
@OutputClass(className = "org.lemurproject.galago.core.window.Window")
public class WindowProducer extends StandardStep<Document, Window> {

  int n;
  int width;
  boolean ordered;
  LinkedList<String> window;
  int currentDocument;
  int currentBegin;
  int file = -1;
  long filePosition;

  Counter windows;

  public WindowProducer(TupleFlowParameters parameters) throws IOException {
    this.n = (int) parameters.getJSON().getLong("n");
    this.width = (int) parameters.getJSON().getLong("width");
    this.ordered = parameters.getJSON().getBoolean("ordered");

    windows = parameters.getCounter("Windows Produced");
  }

  /*
   * n-gram format:
   *   "w1~w2~w3"
   * 
   */
  public void process(Document doc) throws IOException {

    //if this document is not big enough to contain any ngrams
    if (doc.terms.size() < n) {
      return;
    }

    if (file != doc.fileId) {
      file = doc.fileId;
      filePosition = 0;
    }

    window = new LinkedList();
    currentDocument = doc.identifier;
    currentBegin = 0;

    if (ordered) {
      for (currentBegin = 0; currentBegin < doc.terms.size(); currentBegin++) {
        window.push(doc.terms.get(currentBegin));
        extractOrderedWindows(doc.terms, currentBegin);
        window.pop();
      }
    } else {
      for (currentBegin = 0; currentBegin < doc.terms.size(); currentBegin++) {
        window.push(doc.terms.get(currentBegin));
        extractUnorderedWindows(doc.terms, currentBegin);
        window.pop();
      }
    }
  }

  private void extractOrderedWindows(List<String> terms, int currentEnd) throws IOException {
    // print(window);
    if (window.size() == n) {
      processor.process(new Window(file, filePosition, currentDocument, currentBegin, currentEnd, ConvertToBytes(window)));
      if (windows != null) windows.increment();
      filePosition++;

    } else {
      for (int i = currentEnd + 1; i < (currentEnd + this.width + 1); i++) {
        if (i < terms.size()) {
          window.push(terms.get(i));
          extractOrderedWindows(terms, i);
          window.pop();
        }
      }
    }
  }

  private void extractUnorderedWindows(List<String> terms, int currentEnd) throws IOException {
    if (window.size() == n) {
      LinkedList<String> sortedWindow = new LinkedList(window);
      Collections.sort(sortedWindow, Collections.reverseOrder());
      processor.process(new Window(file, filePosition, currentDocument, currentBegin, currentEnd, ConvertToBytes(sortedWindow)));
      if (windows != null) windows.increment();
      filePosition++;

    } else {
      for (int i = currentEnd + 1; i < currentBegin + width; i++) {
        if (i < terms.size()) {
          window.push(terms.get(i));
          extractUnorderedWindows(terms, i);
          window.pop();
        }
      }
    }
  }

  private static byte[] ConvertToBytes(List<String> windowData) {
    StringBuilder sb = new StringBuilder();
    sb.append(windowData.get(windowData.size() - 1));
    for (int i = (windowData.size() - 2); i >= 0; i--) {
      sb.append("~").append(windowData.get(i));
    }
    return Utility.fromString(sb.toString());
  }
}
