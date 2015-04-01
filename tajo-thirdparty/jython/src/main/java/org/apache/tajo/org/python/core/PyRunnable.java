// Copyright (c) Corporation for National Research Initiatives
package org.apache.tajo.org.python.core;

/**
 * Interface implemented by compiled modules which allow access to
 * to the module code object.
 */

public interface PyRunnable {
    /**
     * Return the modules code object.
     */
    abstract public PyCode getMain();
}
