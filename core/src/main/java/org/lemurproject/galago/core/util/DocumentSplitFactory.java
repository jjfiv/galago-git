package org.lemurproject.galago.core.util;

import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.utility.ByteUtil;

import java.io.File;

/**
 * @author jfoley.
 */
public class DocumentSplitFactory {
  public static DocumentSplit file(String path) {
    DocumentSplit split = new DocumentSplit();
    split.fileName = path;
    split.fileType = "";
    split.innerName = "";
    split.startKey = ByteUtil.EmptyArr;
    split.endKey = ByteUtil.EmptyArr;
    return split;
  }
  public static DocumentSplit file(File fp) {
    return file(fp.getAbsolutePath());
  }
  public static DocumentSplit numberedFile(String path, int fileId, int totalFileCount) {
    DocumentSplit split = file(path);
    split.fileId = fileId;
    split.totalFileCount = totalFileCount;
    return split;
  }

  public static DocumentSplit file(String name, String fileType) {
    DocumentSplit split = file(name);
    split.fileType = fileType;
    return split;
  }

  public static DocumentSplit file(File fp, String fileType) {
    return file(fp.getAbsolutePath(), fileType);
  }
}
