package org.lemurproject.galago.core.retrieval.traversal;

import org.lemurproject.galago.core.retrieval.query.Node;

public class AnnotateFieldLengths extends Traversal {

	@Override
	public Node afterNode(Node newNode) throws Exception {
		// if node is a feature node
		// if the child is atomic + has a part name
		// pull the child's part 
		// if part == field.XXX
		// modify node to include: "lengths" : XXX
		// return
		
		//else return original node
		return null;
		
	}

	@Override
	public void beforeNode(Node object) throws Exception {

	}

}
