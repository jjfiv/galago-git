// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow;

import java.io.File;
import java.io.IOException;
import java.util.*;

/**
 *
 * @author trevor
 */
public class OrderedCombiner<T> implements ReaderSource<T> {

  TypeReader<T>[] inputs;
  FileOrderedReader<T>[] files;
  Order<T> order;
  public Step processor;
  boolean closeOnExit;
  static int defaultBufferSize = 1000;
  ReaderSource<T> source = null;

  public static class SortPair<T> {

    public SortPair(T object, TypeReader<T> more) {
      this.object = object;
      this.more = more;
    }
    public T object;
    public TypeReader<T> more;
  }

  private Comparator<SortPair<T>> sortComparator(Comparator<T> compare) {
    final Comparator<T> c = compare;
    return new Comparator<SortPair<T>>() {

      public int compare(SortPair<T> one, SortPair<T> two) {
        return c.compare(one.object, two.object);
      }
    };
  }

  public OrderedCombiner(TypeReader<T>[] inputs, FileOrderedReader<T>[] files, Order<T> order, Processor<T> processor, boolean closeOnExit) {
    this.inputs = inputs;
    this.files = files;
    this.order = order;
    this.processor = processor;
    this.closeOnExit = closeOnExit;
  }

  @SuppressWarnings(value = "unchecked")
  public OrderedCombiner(TypeReader<T>[] inputs, Order<T> order, Processor<T> processor) {
    this(inputs, new FileOrderedReader[0], order, processor, true);
  }

  @SuppressWarnings(value = "unchecked")
  public OrderedCombiner(TypeReader<T>[] inputs, Order<T> order) {
    this(inputs, new FileOrderedReader[0], order, null, true);
  }

  public Class<T> getOutputClass() {
    return order.getOrderedClass();
  }

  public void setProcessor(final Step processor) throws IncompatibleProcessorException {
    this.processor = processor;
  }

  public static <S> OrderedCombiner combineFromFiles(List<String> filenames, Order<S> order) throws IOException {
    return combineFromFiles(filenames, order, null, true, defaultBufferSize);
  }

  public static <S> OrderedCombiner<S> combineFromFileObjs(List<File> filenames, Order<S> order) throws IOException {
    List<String> paths = new ArrayList<String>();
    for (File f : filenames) {
      paths.add(f.getAbsolutePath());
    }
    return combineFromFiles(paths, order, null, true, defaultBufferSize);
  }

  @SuppressWarnings(value = "unchecked")
  public static <S> OrderedCombiner<S> combineFromFiles(List<String> filenames, Order<S> order, Processor<S> processor, boolean closeOnExit, int bufferSize) throws IOException {
    TypeReader[] inputs = new TypeReader[filenames.size()];
    FileOrderedReader[] readers = new FileOrderedReader[filenames.size()];

    for (int i = 0; i < filenames.size(); i++) {
      readers[i] = new FileOrderedReader<S>(filenames.get(i), bufferSize / filenames.size());
      inputs[i] = readers[i].getOrderedReader();
    }

    return new OrderedCombiner<S>(inputs, readers, order, processor, closeOnExit);
  }

  @SuppressWarnings(value = "unchecked")
  public static <S> OrderedCombiner<S> combineFromFiles(List<String> filenames) throws IOException {
    TypeReader[] inputs = new TypeReader[filenames.size()];
    FileOrderedReader[] readers = new FileOrderedReader[filenames.size()];
    assert filenames.size() > 0;

    for (int i = 0; i < filenames.size(); i++) {
      readers[i] = new FileOrderedReader<S>(filenames.get(i));
      inputs[i] = readers[i].getOrderedReader();
    }

    return new OrderedCombiner<S>(inputs, readers, readers[0].getOrder(), null, true);
  }

  public static <S> OrderedCombiner<S> combineFromFiles(List<String> filenames, Order<S> order, Processor<S> processor) throws IOException {
    return combineFromFiles(filenames, order, processor, true, defaultBufferSize);
  }

  public T read() throws IOException {
    if (source == null) {
      source = order.orderedCombiner(Arrays.asList(inputs), false);
    }

    T result = source.read();

    if (result == null) {
      close();
    }

    return result;
  }

  @SuppressWarnings("unchecked")
  public void close() throws IOException {
    for (FileOrderedReader reader : files) {
      reader.close();
    }

    files = (FileOrderedReader<T>[]) new FileOrderedReader[0];
  }

  public void run() throws IOException {
    if (inputs.length == 0) {
      return;
    }
    source = order.orderedCombiner(Arrays.asList(inputs), false);

    try {
      source.setProcessor(processor);
    } catch (IncompatibleProcessorException e) {
      throw (IOException) new IOException("Wasn't able to link to this processor object.").initCause(e);
    }

    source.run();

    if (closeOnExit) {
      Linkage.close(processor);
    }

    close();
  }


  public CompressionType getCompression(){
    return files[0].getCompression();
  }

}
