// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;

public class Splitter<T> implements Processor<T> {
    private Processor<T>[] processors;
    private Order<T> typeOrder;

    public Splitter(Processor<T>[] processors, Order<T> order) {
        this.processors = processors;
        this.typeOrder = order;
    }

    public static <S> Splitter<S> splitToFiles(String[] filenames, Order<S> sortOrder, Order<S> hashOrder) throws IOException, IncompatibleProcessorException {
        return splitToFiles(filenames, sortOrder, hashOrder, null);
    }

    @SuppressWarnings("unchecked")
    public static <S> Splitter<S> splitToFiles(String[] filenames, Order<S> sortOrder, Order<S> hashOrder, Class reducerClass) throws IOException, IncompatibleProcessorException {
        assert sortOrder != null;
        assert hashOrder != null;

        Processor[] processors = new Processor[filenames.length];

        try {
            for (int i = 0; i < filenames.length; i++) {
                FileOrderedWriter<S> writer = new FileOrderedWriter<S>(filenames[i], sortOrder);
                Sorter sorter;
                if (reducerClass != null) {
                    sorter = new Sorter<S>(sortOrder, (Reducer<S>) reducerClass.getConstructor().
                                           newInstance());
                } else {
                    sorter = new Sorter<S>(sortOrder);
                }
                sorter.setProcessor(writer);
                processors[i] = sorter;
            }
        } catch (NoSuchMethodException e) {
            throw new IOException(e.getMessage());
        } catch (InvocationTargetException e) {
            throw new IOException(e.getMessage());
        } catch (InstantiationException e) {
            throw new IOException(e.getMessage());
        } catch (IllegalAccessException e) {
            throw new IOException(e.getMessage());
        }

        return new Splitter<S>(processors, hashOrder);
    }

    @SuppressWarnings("unchecked")
    public static <S> Splitter splitToFiles(String prefix, Order<S> order, int count) throws IOException, FileNotFoundException, IncompatibleProcessorException {
        assert order != null;

        Processor[] processors = new Processor[count];

        for (int i = 0; i < count; i++) {
            String filename = prefix + i;
            FileOrderedWriter<S> writer = new FileOrderedWriter<S>(filename, order);
            Sorter<S> sorter = new Sorter<S>(order);
            sorter.setProcessor(writer);
            processors[i] = sorter;
        }

        return new Splitter<S>(processors, order);
    }

    public void process(T object) throws IOException {
        int hash = typeOrder.hash(object);
        if (hash < 0) {
            hash = ~hash; // using bitwise complement, because -Integer.MIN_VALUE is still negative
        }
        assert hash >= 0 : "Just absed the hash value, so this should always be true";
        hash = hash % processors.length;
        assert hash >= 0 : "Mod operation made it negative!";
        processors[hash].process(object);
    }

    public void close() throws IOException {
        for (Processor<T> processor : processors) {
            processor.close();
        }
        processors = null;
    }
}
