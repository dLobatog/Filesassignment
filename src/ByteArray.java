//*
// * dbXML - Native XML Database
// * Copyright (C) 1999-2004  The dbXML Group, L.L.C.
// *
// * This program is free software; you can redistribute it and/or
// * modify it under the terms of the GNU General Public License
// * as published by the Free Software Foundation; either version 2
// * of the License, or (at your option) any later version.
// *
// * This program is distributed in the hope that it will be useful,
// * but WITHOUT ANY WARRANTY; without even the implied warranty of
// * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// * GNU General Public License for more details.
// *
// * You should have received a copy of the GNU General Public License
// * along with this program; if not, write to the Free Software
// * Foundation, Inc., 59 Temple Place - Suite 330, Boston, MA  02111-1307, USA.
// *
// * $Id: ByteArray.java,v 1.3 2004/07/20 20:19:42 bradford Exp $
// */

/**
 * ByteArray manages fixed-length byte arrays
 */

public final class ByteArray {
   private byte[] data;
   private int offset;
   private int length;
   private int pos;

   ByteArray() {
   }

   public ByteArray(int size) {
      data = new byte[size];
      offset = 0;
      length = size;
      pos = 0;
   }

   public ByteArray(byte[] data, int offset, int length) {
      this.data = data;
      this.offset = offset;
      this.length = length;
      this.pos = 0;
   }

   public ByteArray(byte[] data) {
      this.data = data;
      offset = 0;
      length = data.length;
      pos = 0;
   }

   public byte[] getData() {
      return data;
   }

   public int getLength() {
      return length;
   }

   public int getOffset() {
      return offset;
   }

   public int getPos() {
      return pos;
   }

   public void setPos(int pos) {
      if ( pos >= 0 && pos < length )
         this.pos = pos;
   }

   public void resetPos() {
      pos = offset;
   }

   public static long readLong(byte[] data, int offset) {
      int ch1 = data[offset] & 0xff;
      int ch2 = data[offset + 1] & 0xff;
      int ch3 = data[offset + 2] & 0xff;
      int ch4 = data[offset + 3] & 0xff;
      int ch5 = data[offset + 4] & 0xff;
      int ch6 = data[offset + 5] & 0xff;
      int ch7 = data[offset + 6] & 0xff;
      int ch8 = data[offset + 7] & 0xff;
      return (ch1 << 56) + (ch2 << 48) + (ch3 << 40) + (ch4 << 32)
         + (ch5 << 24) + (ch6 << 16) + (ch7 << 8) + (ch8 << 0);
   }

   public long readLong() {
      long result = readLong(data, pos);
      pos += 8;
      return result;
   }

   public static int readInt(byte[] data, int offset) {
      int ch1 = data[offset] & 0xff;
      int ch2 = data[offset + 1] & 0xff;
      int ch3 = data[offset + 2] & 0xff;
      int ch4 = data[offset + 3] & 0xff;
      return (ch1 << 24) + (ch2 << 16) + (ch3 << 8) + (ch4 << 0);
   }

   public int readInt() {
      int result = readInt(data, pos);
      pos += 4;
      return result;
   }

   public static short readShort(byte[] data, int offset) {
      int ch1 = data[offset] & 0xff;
      int ch2 = data[offset + 1] & 0xff;
      return (short)((ch1 << 8) + (ch2 << 0));
   }

   public short readShort() {
      short result = readShort(data, pos);
      pos += 2;
      return result;
   }

   public static char readChar(byte[] data, int offset) {
      int ch1 = data[offset] & 0xff;
      int ch2 = data[offset + 1] & 0xff;
      return (char)((ch1 << 8) + (ch2 << 0));
   }

   public char readChar() {
      char result = readChar(data, pos);
      pos += 2;
      return result;
   }

   public static byte readByte(byte[] data, int offset) {
      return data[offset];
   }

   public byte readByte() {
      return data[pos++];
   }

   public static void readBytes(byte[] data, int offset, byte[] buffer, int pos, int length) {
      System.arraycopy(data, offset, buffer, pos, length);
   }

   public ByteArray readBytes(byte[] buffer, int offset, int length) {
      System.arraycopy(data, pos, buffer, offset, length);
      pos += length;
      return this;
   }

   public static void readBytes(byte[] data, int offset, byte[] buffer) {
      readBytes(data, offset, buffer, 0, buffer.length);
   }

   public ByteArray readBytes(byte[] buffer) {
      return readBytes(buffer, 0, buffer.length);
   }

   public static void writeLong(byte[] data, int offset, long value) {
      data[offset] = (byte)((value >>> 56) & 0xFF);
      data[offset + 1] = (byte)((value >>> 48) & 0xFF);
      data[offset + 2] = (byte)((value >>> 40) & 0xFF);
      data[offset + 3] = (byte)((value >>> 32) & 0xFF);
      data[offset + 4] = (byte)((value >>> 24) & 0xFF);
      data[offset + 5] = (byte)((value >>> 16) & 0xFF);
      data[offset + 6] = (byte)((value >>> 8) & 0xFF);
      data[offset + 7] = (byte)((value >>> 0) & 0xFF);
   }

   public ByteArray writeLong(long value) {
      writeLong(data, pos, value);
      pos += 8;
      return this;
   }

   public static void writeInt(byte[] data, int offset, int value) {
      data[offset] = (byte)((value >>> 24) & 0xFF);
      data[offset + 1] = (byte)((value >>> 16) & 0xFF);
      data[offset + 2] = (byte)((value >>> 8) & 0xFF);
      data[offset + 3] = (byte)((value >>> 0) & 0xFF);
   }

   public ByteArray writeInt(int value) {
      writeInt(data, pos, value);
      pos += 4;
      return this;
   }

   public static void writeShort(byte[] data, int offset, short value) {
      data[offset] = (byte)((value >>> 8) & 0xFF);
      data[offset + 1] = (byte)((value >>> 0) & 0xFF);
   }

   public ByteArray writeShort(short value) {
      writeShort(data, pos, value);
      pos += 2;
      return this;
   }

   public static void writeChar(byte[] data, int offset, char value) {
      data[offset] = (byte)((value >>> 8) & 0xFF);
      data[offset + 1] = (byte)((value >>> 0) & 0xFF);
   }

   public ByteArray writeChar(char value) {
      writeChar(data, pos, value);
      pos += 2;
      return this;
   }

   public static void writeByte(byte[] data, int offset, byte value) {
      data[offset] = value;
   }

   public ByteArray writeByte(byte value) {
      data[pos++] = value;
      return this;
   }

   public static void writeBytes(byte[] data, int offset, byte[] buffer, int pos, int length) {
      System.arraycopy(buffer, pos, data, offset, length);
   }

   public ByteArray writeBytes(byte[] buffer, int offset, int length) {
      System.arraycopy(buffer, pos, data, offset, length);
      pos += length;
      return this;
   }

   public static void writeBytes(byte[] data, int offset, byte[] buffer) {
      writeBytes(data, offset, buffer, 0, buffer.length);
   }

   public ByteArray writeBytes(byte[] buffer) {
      return writeBytes(buffer, 0, buffer.length);
   }
}
