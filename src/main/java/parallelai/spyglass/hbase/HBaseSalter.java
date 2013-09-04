package parallelai.spyglass.hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.SortedSet;
import java.util.TreeSet;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;

public class HBaseSalter {
  private static final Log LOG = LogFactory.getLog(HBaseSalter.class);

  
  public static byte [] addSaltPrefix(byte [] key) throws IOException {
    if( key == null || key.length < 1 ) throw new IOException("Input Key is EMPTY or Less than 1 Character Long");
    
    String keyStr = Bytes.toString(key);
    
    return Bytes.toBytes(keyStr.substring(keyStr.length() - 1) + "_" + keyStr); 
  }

  public static String addSaltPrefix(String key) throws IOException {
    if( key == null || key.length() < 1 ) throw new IOException("Input Key is EMPTY or Less than 1 Character Long");
    
    return (key.substring(key.length() - 1) + "_" + key); 
  }
  
  public static ImmutableBytesWritable addSaltPrefix( ImmutableBytesWritable key ) throws IOException {
	  return new ImmutableBytesWritable( addSaltPrefix(key.get()));
  }

  public static byte [] delSaltPrefix(byte [] key) throws IOException {
    if( key == null || key.length < 3 ) throw new IOException("Input Key is EMPTY or Less than 3 Characters Long");
    
    String keyStr = Bytes.toString(key);
    
    return Bytes.toBytes(keyStr.substring(2)); 
  }

  public static String delSaltPrefix(String key) throws IOException {
    if( key == null || key.length() < 3 ) throw new IOException("Input Key is EMPTY or Less than 3 Characters Long");
    
    return key.substring(2); 
  }
  
  public static ImmutableBytesWritable delSaltPrefix( ImmutableBytesWritable key ) throws IOException {
	  return new ImmutableBytesWritable( delSaltPrefix(key.get()));
  }
  
  public static final String DEFAULT_PREFIX_LIST = " !\"#$%&'()*+,-./0123456789:;<=>?@ABCDEFGHIJKLMNOPQRSTUVWXYZ[\\]^_`abcdefghijklmnopqrstuvwxyz{|}~";
  
  public static Pair<byte[], byte[]>[] getDistributedIntervals(byte[] originalStartKey, byte[] originalStopKey) throws IOException {
	  return getDistributedIntervals(originalStartKey, originalStopKey, DEFAULT_PREFIX_LIST);
  }
  
  public static Pair<byte[], byte[]>[] getDistributedIntervals(byte[] originalStartKey, byte[] originalStopKey, String prefixList) throws IOException {
	  return getDistributedIntervals(originalStartKey, originalStopKey, HConstants.EMPTY_START_ROW, HConstants.EMPTY_END_ROW, prefixList);
  }
  

  public static Pair<byte[], byte[]>[] getDistributedIntervals(
		  byte[] originalStartKey, byte[] originalStopKey, 
		  byte[] regionStartKey, byte[] regionStopKey, 
		  String prefixList) throws IOException {
	LOG.debug("".format("OSRT: (%s) OSTP: (%s) RSRT: (%s) RSTP: (%s) PRFX: (%s)",
			Bytes.toString(originalStartKey),
			Bytes.toString(originalStopKey),
			Bytes.toString(regionStartKey),
			Bytes.toString(regionStopKey),
			prefixList
			)); 
	 
    byte[][] startKeys;
    byte[][] stopKeys;
    
    if(Arrays.equals(regionStartKey, HConstants.EMPTY_START_ROW)
    		&& Arrays.equals(regionStopKey, HConstants.EMPTY_END_ROW) ) {
    	startKeys = getAllKeys(originalStartKey, prefixList);
    	stopKeys = getAllKeys(originalStopKey, prefixList);
    } else if(Arrays.equals(regionStartKey, HConstants.EMPTY_START_ROW)) {
    	startKeys = getAllKeysWithStop(originalStartKey, prefixList, regionStopKey[0]);
    	stopKeys = getAllKeysWithStop(originalStopKey, prefixList, regionStopKey[0]);
    } else if(Arrays.equals(regionStopKey, HConstants.EMPTY_END_ROW)) {
    	startKeys = getAllKeysWithStart(originalStartKey, prefixList, regionStartKey[0]);
    	stopKeys = getAllKeysWithStart(originalStopKey, prefixList, regionStartKey[0]);
    } else {
    	startKeys = getAllKeysInRange(originalStartKey, prefixList, regionStartKey[0], regionStopKey[0]);
    	stopKeys = getAllKeysInRange(originalStopKey, prefixList, regionStartKey[0], regionStopKey[0]);
    }
    
    if( startKeys.length != stopKeys.length) {
    	throw new IOException("LENGTH of START Keys and STOP Keys DO NOT match");
    }
    
    if( Arrays.equals(originalStartKey, HConstants.EMPTY_START_ROW) 
    		&& Arrays.equals(originalStopKey, HConstants.EMPTY_END_ROW) ) {
        Arrays.sort(stopKeys, Bytes.BYTES_RAWCOMPARATOR);
        // stop keys are the start key of the next interval
        for (int i = startKeys.length - 1; i >= 1; i--) {
        	startKeys[i] = startKeys[i - 1];
        }
        startKeys[0] = HConstants.EMPTY_START_ROW;
        stopKeys[stopKeys.length - 1] = HConstants.EMPTY_END_ROW;
    } else if (Arrays.equals(originalStartKey, HConstants.EMPTY_START_ROW)) {
        Arrays.sort(stopKeys, Bytes.BYTES_RAWCOMPARATOR);
        // stop keys are the start key of the next interval
        for (int i = startKeys.length - 1; i >= 1; i--) {
        	startKeys[i] = startKeys[i - 1];
        }
        startKeys[0] = HConstants.EMPTY_START_ROW;
    } else if (Arrays.equals(originalStopKey, HConstants.EMPTY_END_ROW)) {
        Arrays.sort(startKeys, Bytes.BYTES_RAWCOMPARATOR);
        // stop keys are the start key of the next interval
        for (int i = 0; i < stopKeys.length - 1; i++) {
          stopKeys[i] = stopKeys[i + 1];
        }
        stopKeys[stopKeys.length - 1] = HConstants.EMPTY_END_ROW;
    } 
      
    Pair<byte[], byte[]>[] intervals = new Pair[startKeys.length];
    for (int i = 0; i < startKeys.length; i++) {
      intervals[i] = new Pair<byte[], byte[]>(startKeys[i], stopKeys[i]);
    }

    return intervals;
  }  
  
  public static byte[][] getAllKeys(byte[] originalKey) throws IOException {
    return getAllKeys(originalKey, DEFAULT_PREFIX_LIST);
  }

  public static byte[][] getAllKeys(byte[] originalKey, String prefixList) throws IOException {
	  char[] prefixArray = prefixList.toCharArray();
	  
	  return getAllKeysWithStartStop(originalKey, prefixList, (byte)prefixArray[0], (byte)prefixArray[prefixArray.length - 1]);
  }

  public static byte[][] getAllKeysWithStart(byte[] originalKey, String prefixList, byte startKey) throws IOException {
	  char[] prefixArray = prefixList.toCharArray();
	  
	  return getAllKeysWithStartStop(originalKey, prefixList, startKey, (byte)prefixArray[prefixArray.length - 1]);
  }

  public static byte[][] getAllKeysWithStop(byte[] originalKey, String prefixList, byte stopKey) throws IOException {
	  char[] prefixArray = prefixList.toCharArray();

      return getAllKeysWithStartStop(originalKey, prefixList, (byte)prefixArray[0], stopKey);
//	  return getAllKeysWithStartStop(originalKey, prefixList, (byte)prefixArray[0], (byte)(stopKey - 1));
  }

  public static byte[][] getAllKeysInRange(byte[] originalKey, String prefixList, byte startPrefix, byte stopPrefix) {
      return getAllKeysWithStartStop(originalKey, prefixList, startPrefix, stopPrefix);
//	  return getAllKeysWithStartStop(originalKey, prefixList, startPrefix, (byte)(stopPrefix - 1));
  }
  
  private static byte[][] getAllKeysWithStartStop(byte[] originalKey, String prefixList, byte startPrefix, byte stopPrefix) {
      LOG.debug("".format("getAllKeysWithStartStop: OKEY (%s) PLIST (%s) PSRT (%s) PSTP (%s)",
              Bytes.toString(originalKey), prefixList, startPrefix, stopPrefix));

      char[] prefixArray = prefixList.toCharArray();
	  TreeSet<Byte> prefixSet = new TreeSet<Byte>();
	  
	  for( char c : prefixArray ) {
		  prefixSet.add((byte)c);
	  }
	  
	  SortedSet<Byte> subSet = prefixSet.subSet(startPrefix, true, stopPrefix, true);

      LOG.debug("".format("Prefix subset (%s)", subSet));
	  
	  return getAllKeys(originalKey, subSet.toArray(new Byte[]{}));
  }
  
  public static byte[][] getAllKeys(byte[] originalKey, Byte [] prefixArray) {
    LOG.debug("".format("getAllKeys: OKEY (%s) PARRAY (%s)",
              Bytes.toString(originalKey), prefixArray ));

	byte[][] keys = new byte[prefixArray.length][];
	
    for (byte i = 0; i < prefixArray.length; i++) {
      keys[i] = Bytes.add(new byte[] {prefixArray[i].byteValue()}, Bytes.add( Bytes.toBytes("_"), originalKey));
    }

    return keys;
  }
  
  public static void main(String [] args) throws IOException {
	  
   String s = "";
   
   for (byte i = 32; i < Byte.MAX_VALUE; i++) {
	 s += (char)i;
   }
   
   System.out.println("".format("[%s]", s));
   System.out.println("".format("[%s]", DEFAULT_PREFIX_LIST));

	  
    String start = Bytes.toString(HConstants.EMPTY_START_ROW);
    String stop = Bytes.toString(HConstants.EMPTY_END_ROW);
    
    System.out.println("".format("START: (%s) STOP: (%s)", start, stop));
    
    Pair<byte[], byte[]>[] intervals = getDistributedIntervals(Bytes.toBytes(start), Bytes.toBytes(stop), "1423");
    
    for( Pair<byte[], byte[]> p : intervals ) {
      System.out.println("".format("iSTART: (%s) iSTOP: (%s)", Bytes.toString(p.getFirst()), Bytes.toString(p.getSecond())));
    }
  }
}
