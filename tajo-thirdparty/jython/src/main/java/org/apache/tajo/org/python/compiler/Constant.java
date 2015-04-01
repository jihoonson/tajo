// Copyright (c) Corporation for National Research Initiatives

package org.apache.tajo.org.python.compiler;

import org.objectweb.asm.Opcodes;

import java.io.IOException;

abstract class Constant implements Opcodes{
    Module module;
    static int access = ACC_STATIC | ACC_FINAL;
    String name;

    abstract void get(Code mv) throws IOException;

    abstract void put(Code mv) throws IOException;
}
