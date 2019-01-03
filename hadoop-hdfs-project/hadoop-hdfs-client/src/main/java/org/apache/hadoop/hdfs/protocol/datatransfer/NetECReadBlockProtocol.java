package org.apache.hadoop.hdfs.protocol.datatransfer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;

import org.apache.hadoop.hdfs.protocol.ExtendedBlock;
import org.apache.hadoop.hdfs.util.ByteUtils;

/**
 * Simplified information about reading a block
 * via NetEC
 */
public class NetECReadBlockProtocol {
  /**
   *  dnIP |poolId|blkId|blkLen|genStamp|clientName|readOffset|readLen
   * 15byte|10byte|8byte|8 byte| 8 byte | 40 byte  |  8 byte  | 8 byte
   */

  private final String datanodeIP;
  /* ExtendedBlock */
  private final String poolId;
  private final long blkId;
  private final long blkLen;
  private final long genStamp;

  private final String clientName;
  private final long readOffset;
  private final long readLen;
  /* byte size */
  private static final int PACKET_SIZE = 128;
  private static final int LONG_BYTE_SIZE = Long.BYTES;
  private static final int DNIP_BYTE_SIZE = 15;
  private static final int POOLID_BYTE_SIZE = 10;
  private static final int CLIENTNAME_BYTE_SIZE = 40;
  private static final int PROTO_LEN =
    DNIP_BYTE_SIZE + POOLID_BYTE_SIZE + CLIENTNAME_BYTE_SIZE + 5 * LONG_BYTE_SIZE;
  private static final int PADDING_LEN = PACKET_SIZE - PROTO_LEN;




  public NetECReadBlockProtocol(final String datanodeIP,
    final String poolId,
    final long blkId, final long blkLen,
    final long genStamp, final String clientName,
    final long readOffset, final long readLen) {
    this.datanodeIP = datanodeIP;
    this.poolId = poolId;
    this.blkId = blkId;
    this.blkLen = blkLen;
    this.genStamp = genStamp;
    this.clientName = clientName;
    this.readOffset = readOffset;
    this.readLen = readLen;
  }

  public static NetECReadBlockProtocol parseFrom(InputStream in) throws IOException {
    /* read all data into buffer */
    byte[] buf = new byte[PROTO_LEN];
    int byteRead = in.read(buf);
    if (byteRead != PROTO_LEN) {
      // error
      return null;
    }
    int bufPos = 0;
    /* dnIP */
    final String pDatanodeIP = ByteUtils.bytes2String(
      Arrays.copyOfRange(buf, bufPos,
      bufPos + DNIP_BYTE_SIZE));
    bufPos += DNIP_BYTE_SIZE;
    /* poolId */
    final String pPoolId = ByteUtils.bytes2String(
      Arrays.copyOfRange(buf, bufPos,
      bufPos + POOLID_BYTE_SIZE));
    bufPos += POOLID_BYTE_SIZE;
    /* blkId */
    final long pBlkId = ByteUtils.bytes2Long(
      Arrays.copyOfRange(buf, bufPos,
      bufPos + LONG_BYTE_SIZE));
    bufPos += LONG_BYTE_SIZE;
    /* blkLen */
    final long pBlkLen = ByteUtils.bytes2Long(
      Arrays.copyOfRange(buf, bufPos,
      bufPos + LONG_BYTE_SIZE));
    bufPos += LONG_BYTE_SIZE;
    /* genStamp */
    final long pGenStamp = ByteUtils.bytes2Long(
      Arrays.copyOfRange(buf, bufPos,
      bufPos + LONG_BYTE_SIZE));
    bufPos += LONG_BYTE_SIZE;
    /* clientName */
    final String pClientName = ByteUtils.bytes2String(
      Arrays.copyOfRange(buf, bufPos,
      bufPos + CLIENTNAME_BYTE_SIZE));
    bufPos += CLIENTNAME_BYTE_SIZE;
    /* readOffset */
    final long pReadOffset = ByteUtils.bytes2Long(
      Arrays.copyOfRange(buf, bufPos,
      bufPos + LONG_BYTE_SIZE));
    bufPos += LONG_BYTE_SIZE;
    /* readLen */
    final long pReadLen = ByteUtils.bytes2Long(
      Arrays.copyOfRange(buf, bufPos,
      bufPos + LONG_BYTE_SIZE));
    bufPos += LONG_BYTE_SIZE;

    return new NetECReadBlockProtocol(pDatanodeIP, pPoolId,
      pBlkId, pBlkLen, pGenStamp, pClientName, pReadOffset, pReadLen);
  }

  public void write(OutputStream out) throws IOException {
    /* Write all data into buffer */
    byte[] buf = new byte[PACKET_SIZE];
    int bufPos = 0;
    /* datanodeIP */
    /* arraycopy(src, srcPos, dest, destPos, length) */
    System.arraycopy(ByteUtils.string2Bytes(datanodeIP), 0,
      buf, bufPos, poolId.length());
    bufPos += DNIP_BYTE_SIZE;
    /* poolId */
    System.arraycopy(ByteUtils.string2Bytes(poolId), 0,
      buf, bufPos, poolId.length());
    bufPos += POOLID_BYTE_SIZE;
    /* blkId */
    System.arraycopy(ByteUtils.long2Bytes(blkId), 0,
      buf, bufPos, LONG_BYTE_SIZE);
    bufPos += LONG_BYTE_SIZE;
    /* blkLen */
    System.arraycopy(ByteUtils.long2Bytes(blkLen), 0,
      buf, bufPos, LONG_BYTE_SIZE);
    bufPos += LONG_BYTE_SIZE;
    /* genStamp */
    System.arraycopy(ByteUtils.long2Bytes(genStamp), 0,
      buf, bufPos, LONG_BYTE_SIZE);
    bufPos += LONG_BYTE_SIZE;
    /* clientName */
    System.arraycopy(ByteUtils.string2Bytes(clientName), 0,
      buf, bufPos, clientName.length());
    bufPos += CLIENTNAME_BYTE_SIZE;
    /* readOffset */
    System.arraycopy(ByteUtils.long2Bytes(readOffset), 0,
      buf, bufPos, LONG_BYTE_SIZE);
    bufPos += LONG_BYTE_SIZE;
    /* readLen */
    System.arraycopy(ByteUtils.long2Bytes(readLen), 0,
      buf, bufPos, LONG_BYTE_SIZE);
    bufPos += LONG_BYTE_SIZE;

    /* write out */
    out.write(buf);
    // out.flush();
  }

  public String getDatanodeIP() {
    return this.datanodeIP;
  }

  public ExtendedBlock getBlock() {
    return new ExtendedBlock(poolId, blkId, blkLen, genStamp);
  }

  public String getClientName() {
    return this.clientName;
  }
  public long getReadOffset() {
    return this.readOffset;
  }
  public long getReadLen() {
    return this.readLen;
  }
}