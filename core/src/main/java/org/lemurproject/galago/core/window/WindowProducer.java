// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.window;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.core.parse.Tag;
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
  long currentDocument;
  int file = -1;
  long filePosition;
  Set<String> fields;
  Counter windows;
  int windowBegin;
  
  public WindowProducer(TupleFlowParameters parameters) throws IOException {
    this.n = (int) parameters.getJSON().getLong("n");
    this.width = (int) parameters.getJSON().getLong("width");
    this.ordered = parameters.getJSON().getBoolean("ordered");
    
    windows = parameters.getCounter("Windows Produced");
    
    if (parameters.getJSON().isList("fields")) {
      fields = new HashSet(parameters.getJSON().getList("fields"));
    } else {
      fields = new HashSet();
      fields.add("document");
    }
  }

  /*
   * n-gram format:
   *   "w1~w2~w3"
   * 
   */
  @Override
  public void process(Document doc) throws IOException {

    //if this document is not big enough to contain any ngrams
    if (doc.terms.size() < n) {
      return;
    }
    
    if (file != doc.fileId) {
      file = doc.fileId;
      filePosition = 0; // incremented each time an ngram is processed
    }
    
    window = new LinkedList();
    currentDocument = doc.identifier;
    
    List<Tag> tagList = collectNonOverlappingTags(doc);
    
    for (Tag tag : tagList) {
      // check that the tag is desired & can contain at least one window
      if ((tag.end - tag.begin) > n) {
        // get a sub-list of terms
        List<String> terms = doc.terms.subList(tag.begin, tag.end);
        for (windowBegin = 0; windowBegin < terms.size(); windowBegin++) {
          if (ordered) {
            extractOrderedWindows(terms, windowBegin);
          } else {
            extractUnorderedWindows(terms, windowBegin);
          }
        }
      }
    }
  }
  
  private void extractOrderedWindows(List<String> terms, int currentEnd) throws IOException {
    window.addLast(terms.get(currentEnd));

    // print(window);
    if (window.size() == n) {
      processor.process(new Window(file, filePosition, currentDocument, windowBegin, currentEnd + 1, ConvertToBytes(window)));
      if (windows != null) {
        windows.increment();
      }
      filePosition++;
      
    } else {
      for (int i = 1; i <= this.width; i++) {
        if ((currentEnd + i) < terms.size()) {
          extractOrderedWindows(terms, currentEnd + i);
        }
      }
    }
    
    window.removeLast();
  }
  
  private void extractUnorderedWindows(List<String> terms, int currentEnd) throws IOException {
    window.addLast(terms.get(currentEnd));
    
    if (window.size() == n) {
      LinkedList<String> sortedWindow = new LinkedList(window);
      Collections.sort(sortedWindow);
      
      processor.process(new Window(file, filePosition, currentDocument, windowBegin, currentEnd + 1, ConvertToBytes(sortedWindow)));
      if (windows != null) {
        windows.increment();
        
      }
      filePosition++;
      
    } else {
      for (int i = currentEnd + 1; i < windowBegin + width; i++) {
        if (i < terms.size()) {
          extractUnorderedWindows(terms, i);
        }
      }
    }
    
    window.removeLast();
  }
  
  private static byte[] ConvertToBytes(List<String> windowData) {
    StringBuilder sb = new StringBuilder();
    sb.append(windowData.get(0));
    for (int i = 1; i < windowData.size(); i++) {
      sb.append("~").append(windowData.get(i));
    }
    return Utility.fromString(sb.toString());
  }
  
  private List<Tag> collectNonOverlappingTags(Document doc) {
    // input
    ArrayList<Tag> tags = new ArrayList((doc.tags == null) ? new ArrayList() : doc.tags);
    // ensure there is a 'document' tag
    Tag docTag = new Tag("document", null, 0, doc.terms.size());
    tags.add(docTag);

    // sort the tags by begins (break ties using ends)
    Collections.sort(tags);

    // output
    ArrayList<Tag> selectedTags = new ArrayList();
    
    
    for (int i = 0; i < tags.size(); i++) {
      Tag t = tags.get(i);
      if (fields.contains(t.name)) {
        boolean ignore = false;
        for (int j = 0; j < selectedTags.size(); j++) {
          Tag s = selectedTags.get(j);

          // for tags with identical names:
          // if s surrounds t - t can be ignored
          if (s.begin <= t.begin && t.end <= s.end) {
            ignore = true;
            break;
          }

          // if t surrounds s
          if (t.begin <= s.begin && s.end <= t.end) {
            selectedTags.remove(j);
            j--;
          }
          
        }
        if (!ignore) {
          selectedTags.add(t);
        }
      }
    }
    return selectedTags;
  }
}
