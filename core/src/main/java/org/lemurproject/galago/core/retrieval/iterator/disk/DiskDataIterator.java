// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.retrieval.iterator.disk;

import java.io.IOException;
import org.lemurproject.galago.core.index.source.DataSource;

/**
 *
 * @author jfoley
 */
public abstract class DiskDataIterator<DataType> extends SourceIterator {
  DataSource<DataType> dataSource;
  
  DiskDataIterator(DataSource<DataType> src) {
    super(src);
    this.dataSource = src;
  }
  
  @Override
  public String getValueString() throws IOException {
    DataType dt = getData();
    if(dt == null) {
      return "null-value";
    }
    return dt.toString();
  }
  
  public DataType getData() throws IOException {
    return dataSource.getData(context.document);
  }

}
