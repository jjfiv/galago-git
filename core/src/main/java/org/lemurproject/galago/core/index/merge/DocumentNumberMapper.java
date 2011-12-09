/*
 *  BSD License (http://lemurproject.org/galago-license)
 */
package org.lemurproject.galago.core.index.merge;

import java.io.File;
import java.io.IOException;
import org.lemurproject.galago.core.index.disk.DiskNameReader;
import org.lemurproject.galago.core.index.disk.DiskIndex;
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
public class DocumentNumberMapper extends StandardStep<DocumentSplit, DocumentMappingData> {

  int nextIndexStartNumber = 0;

  public void process(DocumentSplit index) throws IOException {
    
    processor.process(new DocumentMappingData(index.fileId, nextIndexStartNumber));

    DiskNameReader namesReader = (DiskNameReader) DiskIndex.openIndexPart(index.fileName + File.separator + "names");
    DiskNameReader.KeyIterator iterator = namesReader.getIterator();
    int lastDocId = iterator.getCurrentIdentifier();
    while (iterator.nextKey()) {
      lastDocId = iterator.getCurrentIdentifier();
    }
    nextIndexStartNumber += lastDocId + 1;
  }
}
