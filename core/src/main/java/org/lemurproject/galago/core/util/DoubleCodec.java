// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.util;

import java.io.*;
import java.util.*;

public class DoubleCodec {

    // This is arbitrary - you really need to reset to the first value you're scanning in
    public static double DEFAULT = -6.0;

    private double seed;
    private double prediction;
    public int used = 0;
    public byte[] buffer = new byte[9];

    public DoubleCodec() { reset(DEFAULT); }
    public DoubleCodec(double p) { reset(p); }
    public void reset() { reset(DEFAULT); }

    public void reset(double p) {
	seed = prediction = p;
    }

    public double getSeed() { return seed; }

    public int writeVDouble(byte[] external, double d) throws IOException {
	assert(external.length >= 9);

	// Write directly to the external buffer	
	byte[] tmp = buffer;
	buffer = external;
	compress(d);
	buffer = tmp;
	return used;
    }

    public void writeVDouble(DataOutput out, double d) throws IOException {
	compress(d);
	out.write(buffer, 0, used);
    }

    public void compress(double d) {
	long l = Double.doubleToLongBits(prediction) ^ Double.doubleToLongBits(d);
	pack(l);
	double diff = Double.longBitsToDouble(l);
	prediction = d; // Update to make a closer prediction
    }

    private void pack(long l) {
	Arrays.fill(buffer, (byte) 0x00);
	int lzc = lead_zeroes(l);
	if (lzc == 64) { // GOLD
	    buffer[0] = (byte) 0x80;
	    used = 1;
	    return;
	}

	buffer[0] = (byte) (lzc << 2);
	
	if (lzc < 14) { // SAD, no savings
	    buffer[1] = (byte)(l >>> 56);
	    buffer[2] = (byte)(l >>> 48);
	    buffer[3] = (byte)(l >>> 40);
	    buffer[4] = (byte)(l >>> 32);
	    buffer[5] = (byte)(l >>> 24);
	    buffer[6] = (byte)(l >>> 16);
	    buffer[7] = (byte)(l >>>  8);
	    buffer[8] = (byte)(l >>>  0);
	    used = 9;
	    return;
	}
	
	// This is the case where we can at least pack down by 1 byte, but not to 64.
	// How many bytes we use
	used = ((70-lzc)/8) + (((70-lzc)%8)>0 ? 1 : 0);	

	// Get the first 2 bits set;
	buffer[0] |= (byte) (l >>> (62-lzc));
	
	// Now set the rest
	l <<= (lzc+2);
	int pos = 56;
	for (int i = 1; i < used; i++) {
	    buffer[i] = (byte) (l >>> pos);
	    pos -= 8;
	}	
    }

    private int lead_zeroes(long l) {
	long mask = 0x8000000000000000L;
	int count = 0;
	while (((mask & l) == 0L) && (count < 64)) {
	    mask >>= 1;
	    count++;
	}
	return count;
    }

    public double readVDouble(DataInput in) throws IOException {
	// Get the length used
	int lead = 0;
	lead |= in.readUnsignedByte();

	if (lead == 128) { // perfectly compressed - return predicted value
	    return prediction;
	}

	// We need to shift to get the right value out
	int lzc = (lead >>> 2);
	if (lzc < 14) { // No compression used, just read a double in
	    long xord = in.readLong();
	    prediction = Double.longBitsToDouble(xord ^ Double.doubleToLongBits(prediction));
	    return prediction;
	}

	int used = ((70-lzc)/8) + (((70-lzc)%8)>0 ? 1 : 0);	
	in.readFully(buffer, 0, used-1); // Skip one since we already read it
	
	// Reconstruct
	long mask = lead & 0x03;
	for (int i = 0; i < (used-1); i++) {
	    mask <<= 8;
	    mask |= (int) (0xFF & buffer[i]);
	}
	// Shift back to the right position
	mask >>>= (lzc-(62-(8*(used-1))));
	
	// De-mask	
	long value = Double.doubleToLongBits(prediction) ^ mask;	
	prediction = Double.longBitsToDouble(value);
	return prediction;
    }

    public static void main(String[] args) throws IOException {
	BufferedReader br = new BufferedReader(new FileReader(args[0]));
	ArrayList<Double> nums = new ArrayList<Double>();
	while (br.ready()) {
	    nums.add(Double.parseDouble(br.readLine()));
	}
	br.close();
	//System.out.println("Read " + nums.size() + " inputs.");
	DoubleCodec dc = new DoubleCodec(nums.get(0));
	
	// Open the output
	DataOutputStream dos = new DataOutputStream(new FileOutputStream(args[1]));

	for (Double d : nums) {
	    dc.writeVDouble(dos, d);
	}
	dos.writeDouble(dc.getSeed());
	dos.close();

	// Now read it back in
	RandomAccessFile raf = new RandomAccessFile(new File(args[1]), "r");
	raf.seek(raf.length()-8);
	double seed = raf.readDouble();
	//System.out.println("Seed recovered: " + seed);
	raf.seek(0);
	dc = new DoubleCodec(seed);
	while (raf.getFilePointer() < (raf.length()-8)) {
	    double value = dc.readVDouble(raf);
	    //System.out.println("Read: " + value);
	}
	raf.close();
    }
}