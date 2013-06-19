package com.tencent.qqlive.streaming.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;

public class Utils {
	public static String stringifyException(Throwable e) {
		StringWriter stm = new StringWriter();
		PrintWriter wrt = new PrintWriter(stm);
		e.printStackTrace(wrt);
		wrt.close();
		return stm.toString();
	}
	
	public static final byte[] longToByteArray(long value) {
	    return new byte[] {
	    		(byte)(value >>> 56),
	    		(byte)(value >>> 48),
	    		(byte)(value >>> 40),
	    		(byte)(value >>> 32),
	            (byte)(value >>> 24),
	            (byte)(value >>> 16),
	            (byte)(value >>> 8),
	            (byte)value};
	}
	
	public static InetAddress longToInetAddr(long value) throws Exception {
		byte[] bytes = longToByteArray(value);
		
		byte[] validBytes = new byte[] {
			bytes[4],
			bytes[5],
			bytes[6],
			bytes[7]
		};
		
		return InetAddress.getByAddress(validBytes);
	}
	
	public static long inetAddrToLong(InetAddress addr) {
		byte[] bytes = addr.getAddress();
		return (bytes[3] & 0xff) | (bytes[2]<<8 & 0xff00) 
				| (bytes[1]<<16 & 0xff0000) | (bytes[0]<<24 & 0xff000000);
	}
}
