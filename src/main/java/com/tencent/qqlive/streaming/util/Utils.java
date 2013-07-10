package com.tencent.qqlive.streaming.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetAddress;
import java.util.Collection;

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
		return ((((long) bytes[ 0] & 0xff) << 24) 
	               | (((long) bytes[ 1] & 0xff) << 16) 
	               | (((long) bytes[ 2] & 0xff) << 8) 
	               | (((long) bytes[ 3] & 0xff) << 0)); 
	}
	
	public static Integer isNumeric(String value) {
		Integer ret = null;
		
		try {
			ret = Integer.valueOf(value);
		} catch (NumberFormatException e) {
			return null;
		}
		
		return ret;
	}
	
	public static String getHostByUrl(String url) {
		if (!url.startsWith("http://")) {
			return url;
		} else {
			int bIdx = url.indexOf("http://");
			if (bIdx == -1)
				bIdx = -7;
			int eIdx = url.indexOf("/", bIdx + 7);
			if (eIdx == -1)
				eIdx = url.length();
			
			return url.substring(bIdx + 7, eIdx);
		}
	}
	
	public static String getProgVid(String url) {
		if (url.length() < 1)
			return "";
		
		int eIdx = url.indexOf("?");
		if (eIdx == -1)
			eIdx = url.length();
		
		int bIdx = url.lastIndexOf("/", eIdx);
		if (bIdx == -1)
			bIdx = 0;
		
		return url.substring(bIdx + 1, eIdx);
	}
	
	public static String getURLVKey(String url) {
		int eIdx = url.indexOf("?");
		if (eIdx == -1)
			eIdx = url.length();
		
		return url.substring(0, eIdx);
	}
	
	public static String join(String[] strArray, String delimiter) {
		StringBuilder sb = new StringBuilder();
		
		for (int i = 0; i<strArray.length; i++) {
			sb.append(strArray[i]);
			if (i != strArray.length - 1)
				sb.append(delimiter);
		}
		
		return sb.toString();
	}
	
	public static String join(Collection<String> strColl, String delimiter) {
		String[] strArray = strColl.toArray(new String[0]);
		return join(strArray, delimiter);
	}
	
	public static void main(String[] args) {
		System.out.println(getHostByUrl("file://a.b.c.d"));
	}
}
