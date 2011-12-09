// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.IOException;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 * <p>Sequentially numbers document data objects.</p>
 *
 * <p>The point of this class is to assign small numbers to each document.  This
 * would be simple if only one process was parsing documents, but in fact there are many
 * of them doing the job at once.  So, we extract DocumentData records from each document,
 * put them into a single list, and assign numbers to them.  These NumberedDocumentData
 * records are then used to assign numbers to index postings.
 * </p>
 * 
 * @author trevor
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.parse.Document")
@OutputClass(className = "org.lemurproject.galago.core.parse.Document")
public class DocumentNumberer extends StandardStep<Document, Document> {

  int fileId = -1;
  int curNum = -1;
  int increment = -1;

  public void process(Document doc) throws IOException {
    if (fileId != doc.fileId) {
      fileId = doc.fileId;
      increment = doc.totalFileCount;
      curNum = doc.fileId;
    }

    if (doc.identifier < 0) {
      doc.identifier = curNum;
    }
    curNum += increment;
    processor.process(doc);
  }
}
