// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.tupleflow;

import java.io.DataInput;
import java.io.IOException;

/**
 *
 * @author trevor
 */
public class ArrayInput {
    private DataInput input;

    public ArrayInput(DataInput i) {
        input = i;
    }

    public int readInt() throws IOException {
        return input.readInt();
    }

    public int[] readInts() throws IOException {
        int count = readInt();
        int[] result = new int[count];

        for (int i = 0; i < count; i++) {
            result[i] = readInt();
        }

        return result;
    }

    public long readLong() throws IOException {
        return input.readLong();
    }

    public long[] readLongs() throws IOException {
        int count = readInt();
        long[] result = new long[count];

        for (int i = 0; i < count; i++) {
            result[i] = readLong();
        }

        return result;
    }

    public char readChar() throws IOException {
        return input.readChar();
    }

    public char[] readChars() throws IOException {
        int count = readInt();
        char[] result = new char[count];

        for (int i = 0; i < count; i++) {
            result[i] = readChar();
        }

        return result;
    }

    public boolean readBoolean() throws IOException {
        return input.readByte() != 0 ? true : false;
    }

    public byte readByte() throws IOException {
        return input.readByte();
    }

    public byte[] readBytes() throws IOException {
        int count = readInt();
        byte[] result = new byte[count];
        input.readFully(result);
        return result;
    }

    public short readShort() throws IOException {
        return input.readShort();
    }

    public short[] readShorts() throws IOException {
        int count = readInt();
        short[] result = new short[count];

        for (int i = 0; i < count; i++) {
            result[i] = readShort();
        }

        return result;
    }

    public double readDouble() throws IOException {
        return input.readDouble();
    }

    public double[] readDoubles() throws IOException {
        int count = readInt();
        double[] result = new double[count];

        for (int i = 0; i < count; i++) {
            result[i] = readDouble();
        }

        return result;
    }

    public float readFloat() throws IOException {
        return input.readFloat();
    }

    public float[] readFloats() throws IOException {
        int count = readInt();
        float[] result = new float[count];

        for (int i = 0; i < count; i++) {
            result[i] = readFloat();
        }

        return result;
    }

    public String readString() throws IOException {
        byte[] bytes = readBytes();
        char[] chars = new char[bytes.length];

        for (int i = 0; i < bytes.length; i++) {
            if (bytes[i] < 0) {
                return new String(bytes, "UTF-8");
            }
            chars[i] = (char) bytes[i];
        }

        return new String(chars);
    }

    public String[] readStrings() throws IOException {
        int count = readInt();
        String[] result = new String[count];
        for (int i = 0; i < count; i++) {
            result[i] = readString();
        }
        return result;
    }

    public DataInput getDataInput() {
        return input;
    }
}

/*

#
# The following Python code can autogenerate these Java methods and
# those in ArrayOutput.
#

arrayMethod = """
public %s[] read%s() throws IOException {
int count = readInt();
%s[] result = new %s[count];

for(int i=0; i<count; i++) {
result[i] = read%s();
}                        

return result;
}
"""

basicMethod = """
public %s read%s() throws IOException {
return input.read%s();
}                         
"""
stringMethod = """
public String readString() throws IOException {
char[] chars = readChars();
return new String(chars);
}
"""             

arrayWriter = """
public void write%s(%s[] out) throws IOException {
output.writeInt(out.length);
for(int i=0; i<out.length; i++) {
output.write%s(out[i]);
}
}"""                                              

basicWriter = """
public void write%s(%s out) throws IOException {
output.write%s(out);
}
"""       

stringWriter = """
public void writeString(String out) throws IOException {
output.writeInt(out.length());
output.writeChars(out);
}
"""

basicTypes = [ 'int', 'long', 'char', 'byte', 'short', 'double', 'float' ]

def caps(s):
return s[0].upper() + s[1:]

def plural(s):
return s + 's'

for type in basicTypes:
print basicMethod % (type, caps(type), caps(type))
print arrayMethod % (type, plural(caps(type)), type, type, caps(type))    
print stringMethod                                                             

print "// writers ----"

for type in basicTypes:
print basicWriter % (caps(type), type, caps(type))
print arrayWriter % (plural(caps(type)), type, caps(type))        
print stringWriter

 */
