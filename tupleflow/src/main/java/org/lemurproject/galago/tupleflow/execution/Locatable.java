// BSD License (http://galagosearch.org)

package org.lemurproject.galago.tupleflow.execution;

import java.io.Serializable;

/**
 * This is a mixin class that allows a Job stage/step class to be associated
 * with a file location.  This is used to provide helpful error messages
 * when users make errors in the XML parameter files.
 * 
 * @author trevor
 */
public class Locatable implements Serializable {
    public FileLocation location;

    public Locatable(FileLocation location) {
        setLocation(location);
    }

    public void setLocation(FileLocation location) {
        this.location = location;
    }

    public FileLocation getLocation() {
        return location;
    }
}
