
package org.apache.hadoop.hdfs.util;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class ByteUtils {
    private static ByteBuffer longBuffer = ByteBuffer.allocate(Long.BYTES);

    public static byte[] long2Bytes(long x) {
        longBuffer.putLong(0, x);
        return longBuffer.array();
    }

    public static long bytes2Long(byte[] bytes) {
        longBuffer.put(bytes, 0, bytes.length);
        longBuffer.flip();
        return longBuffer.getLong();
    }

    public static byte[] string2Bytes(String s) {
        return s.getBytes(Charset.forName("UTF-8"));
    }

    public static String bytes2String(byte[] bytes) {
        return new String(bytes, Charset.forName("UTF-8"));
    }
}