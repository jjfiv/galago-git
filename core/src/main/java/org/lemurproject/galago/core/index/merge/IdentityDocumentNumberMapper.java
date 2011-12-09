/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.merge;

import java.io.IOException;
import org.lemurproject.galago.core.types.DocumentMappingData;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.InputClass;
import org.lemurproject.galago.tupleflow.OutputClass;
import org.lemurproject.galago.tupleflow.StandardStep;
import org.lemurproject.galago.tupleflow.execution.Verified;

/**
 *
 * @author sjh
 */
@Verified
@InputClass(className = "org.lemurproject.galago.core.types.DocumentSplit", order = {"+fileId"})
@OutputClass(className = "org.lemurproject.galago.core.types.DocumentMappingData", order = {"+indexId"})
public class IdentityDocumentNumberMapper extends StandardStep<DocumentSplit, DocumentMappingData> {

  public void process(DocumentSplit index) throws IOException {
    processor.process(new DocumentMappingData(index.fileId, 0));
  }
}
