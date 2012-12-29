// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow.execution;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.HashMap;
import java.util.Map;
import org.lemurproject.galago.tupleflow.Utility;

/**
 * Represents a multi block in a TupleFlow stage.
 * A multi-block is a set of pipelines that are all
 * fed the same input tuple, but will produce separate outputs
 * (even outputting to different target stages).
 *
 * The children of a MultiStep must be lists of TupleFlow steps.
 * 
 * @author trevor, irmarc
 */
public class MultiStep extends Step implements Iterable<String> {
    private Map<String, List<Step>> groups;
    private String name;

    /**
     * Create a named MultiStep. The name is for descriptive purposes only.
     */
    public MultiStep(String name) {
	this.name = name;
	groups = new HashMap<String, List<Step>>();
    }

    /**
     * Create an anonymous MultiStep.
     */
    public MultiStep(){
	this("none");
    }

    public int size() {
	return groups.size();
    }

    /**
     * Returns true if there is only one inner pipeline, making
     * this multi block useless.
     */
    public boolean isSingleton() {
	return groups.size() == 1;
    }

    public List<Step> singleton() {
	assert (groups.size() == 1) : "Cannot call singleton if more than 1 inner pipeline exists.";
	String key = groups.keySet().toArray(new String[0])[0];
	return groups.get(key);
    }

    /**
     * Add a new empty inner pipeline to the multi step.
     */
    public MultiStep addGroup(String groupName) {
	groups.put(groupName, new ArrayList<Step>());
	return this;
    }

    /**
     * Add an anonymous existing inner pipeline to the multi step.
     * The name is inferred using the first and last steps in the 
     * inner pipeline. Due to stage construction constraints, this is
     * guaranteed to be unique.
     */
    public MultiStep addGroup(List<Step> steps) {
	String groupName = steps.get(0).toString() + "-" +
	    steps.get(steps.size()-1).toString();
	return addGroup(groupName, steps);
    }

    /**
     * Add a named existing inner pipeline to the multi step.
     */
    public MultiStep addGroup(String groupName, List<Step> steps) {
	groups.put(groupName, steps);
	return this;
    }
    
    public List<Step> getGroup(String name) {
	return groups.get(name);
    }

    /**
     * Remove an inner pipeline.
     */
    public MultiStep removeGroup(String groupName) {
	groups.remove(groupName);
	return this;
    }
    
    /**
     * Add a step to an existing inner pipeline.
     */
    public MultiStep addToGroup(String name, Step s) {
	groups.get(name).add(s);
	return this;
    }   

    /**
     * Output the multi step and inner pipelines a (mildly) readable format.
     */
    public String toString() {
	StringBuilder sb = new StringBuilder();
	sb.append("multi: ").append(name).append("\n");
	for (String name : groups.keySet()) {
	    sb.append(" ").append(name).append(": ");
	    sb.append(Utility.join(groups.get(name).toArray(new Step[0]), " -> ")).append("\n");
	}
	return sb.toString();
    }
    
    /**
     * Produce an iterator capable of iterating over the names of the inner pipelines.
     */
    public java.util.Iterator<String> iterator() {
	return new Iterator();
    }
    
    /**
     * A standard pre-order traversal. Returns the element it's on, then increments the counter.
     * See remove method for removal semantics.
     */
    public class Iterator implements java.util.Iterator<String> {
	private int position;
	ArrayList<String> elements;
	
	public Iterator() {
	    position = 0;
	    elements = new ArrayList<String>(groups.keySet());
	    Collections.sort(elements);
	}

	public boolean hasNext() {
	    return position < elements.size();
	}

	public String next() {	    
	    return elements.get(position++);
	}

	
	public void remove() {
	    int toRemove = position-1;
	    if (toRemove < 0) return;
	    // Delete the element from the groups
	    groups.remove(elements.get(toRemove));
	    // Delete it from our key set
	    elements.remove(toRemove);
	    // reset the pointer
	    --position;
	}
    }
}
