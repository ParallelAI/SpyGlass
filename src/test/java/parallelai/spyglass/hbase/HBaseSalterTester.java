package parallelai.spyglass.hbase;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.junit.Test;

import parallelai.spyglass.hbase.HBaseSalter;


public class HBaseSalterTester {

	@Test
	public void addSaltPrefix() throws IOException {
		String keyStr = "1021";
		byte [] keyBytes = Bytes.toBytes(keyStr);
		byte [] expected = Bytes.toBytes("1_1021");
		byte [] actual = HBaseSalter.addSaltPrefix(keyBytes);
		
		assertArrayEquals(actual, expected);
		
		String actualStr = HBaseSalter.addSaltPrefix(keyStr);
		
		System.out.println(Bytes.toString(expected) + " -> " + actualStr );
		
		assertEquals(Bytes.toString(expected), actualStr);

	}

	@Test
	public void delSaltPrefix() throws IOException {
		String keyStr = "1_1021";
		byte [] keyBytes = Bytes.toBytes(keyStr);
		byte [] expected = Bytes.toBytes("1021");
		byte [] actual = HBaseSalter.delSaltPrefix(keyBytes);
		
		assertArrayEquals(actual, expected);
		
		String actualStr = HBaseSalter.delSaltPrefix(keyStr);
		
		assertEquals(Bytes.toString(expected), actualStr);

	}

	@Test
	public void getAllKeys() throws IOException {
		String keyStr = "1021";
		byte [] keyBytes = Bytes.toBytes(keyStr);
		
		char [] prefixArr = HBaseSalter.DEFAULT_PREFIX_LIST.toCharArray();
		
		byte [][] expected = new byte[prefixArr.length][];
		
		for(int i = 0; i < prefixArr.length; i++ ) {
			expected[i] = Bytes.toBytes(prefixArr[i] + "_" + keyStr);
		}
		
		byte [][] actual = HBaseSalter.getAllKeys(keyBytes);
		
		assertEquals(expected.length, actual.length);
		
		for( int i = 0; i < expected.length; i++) {
			assertArrayEquals(expected[i], actual[i]);
		}
	}

	@Test
	public void getAllKeysWithPrefix() throws IOException {
		String keyStr = "1021";
		byte [] keyBytes = Bytes.toBytes(keyStr);
		String prefix = "0123456789";
		
		char [] prefixArr = prefix.toCharArray();
		
		byte [][] expected = new byte[prefixArr.length][];
		
		for(int i = 0; i < prefixArr.length; i++ ) {
			expected[i] = Bytes.toBytes(prefixArr[i] + "_" + keyStr);
		}
		
		byte [][] actual = HBaseSalter.getAllKeys(keyBytes, prefix);
		
		assertEquals(expected.length, actual.length);
		
		for( int i = 0; i < expected.length; i++) {
			assertArrayEquals(expected[i], actual[i]);
		}
	}

	@Test
	public void getAllKeysWithPrefixAndRange() throws IOException {
		String keyStr = "1021";
		byte [] keyBytes = Bytes.toBytes(keyStr);
		String prefix = "12345";
		String fullPrefix = "0123456789";
		
		char [] prefixArr = prefix.toCharArray();
		Byte [] prefixBytes = new Byte[prefixArr.length]; 
		
		byte [][] expected = new byte[prefixArr.length][];
		
		for(int i = 0; i < prefixArr.length; i++ ) {
			expected[i] = Bytes.toBytes(prefixArr[i] + "_" + keyStr);
			prefixBytes[i] = (byte)prefixArr[i];
		}
		
		byte [][] actual = HBaseSalter.getAllKeysInRange(keyBytes, fullPrefix, (byte)'1', (byte)'5');
		
		assertEquals(expected.length, actual.length);
		
		for( int i = 0; i < expected.length; i++) {
			assertArrayEquals(expected[i], actual[i]);
		}
		
		actual = HBaseSalter.getAllKeys(keyBytes, prefixBytes);
		
		for( int i = 0; i < expected.length; i++) {
			assertArrayEquals(expected[i], actual[i]);
		}
		
	}


	@Test
	public void getAllKeysWithPrefixWithStart() throws IOException {
		String keyStr = "1021";
		byte [] keyBytes = Bytes.toBytes(keyStr);
		String prefix = "3456789";
		String fullPrefix = "0123456789";
		
		char [] prefixArr = prefix.toCharArray();
		Byte [] prefixBytes = new Byte[prefixArr.length]; 
		
		byte [][] expected = new byte[prefixArr.length][];
		
		for(int i = 0; i < prefixArr.length; i++ ) {
			expected[i] = Bytes.toBytes(prefixArr[i] + "_" + keyStr);
			prefixBytes[i] = (byte)prefixArr[i];
		}
		
		byte [][] actual = HBaseSalter.getAllKeysWithStart(keyBytes, fullPrefix, (byte)'3');
		
		assertEquals(expected.length , actual.length);
		
		for( int i = 0; i < expected.length; i++) {
			assertArrayEquals(expected[i], actual[i]);
		}
		
		actual = HBaseSalter.getAllKeys(keyBytes, prefixBytes);
		
		for( int i = 0; i < expected.length; i++) {
			assertArrayEquals(expected[i], actual[i]);
		}
		
	}

	@Test
	public void getAllKeysWithPrefixWithStop() throws IOException {
		String keyStr = "1021";
		byte [] keyBytes = Bytes.toBytes(keyStr);
		String prefix = "012345";
		String fullPrefix = "0123456789";
		
		char [] prefixArr = prefix.toCharArray();
		Byte [] prefixBytes = new Byte[prefixArr.length]; 
		
		byte [][] expected = new byte[prefixArr.length][];
		
		for(int i = 0; i < prefixArr.length; i++ ) {
			expected[i] = Bytes.toBytes(prefixArr[i] + "_" + keyStr);
			prefixBytes[i] = (byte)prefixArr[i];
		}
		
		byte [][] actual = HBaseSalter.getAllKeysWithStop(keyBytes, fullPrefix, (byte)'5');
		
		assertEquals(expected.length , actual.length);
		
		for( int i = 0; i < expected.length; i++) {
			assertArrayEquals(expected[i], actual[i]);
		}
		
		actual = HBaseSalter.getAllKeys(keyBytes, prefixBytes);
		
		for( int i = 0; i < expected.length; i++) {
			assertArrayEquals(expected[i], actual[i]);
		}
	}
	
	@Test
	public void getDistributedIntervals() throws IOException {
		String keyStrStart = "1021";
		byte [] keyBytesStart = Bytes.toBytes(keyStrStart);

		String keyStrStop = "1022";
		byte [] keyBytesStop = Bytes.toBytes(keyStrStop);

		char [] prefixArr = HBaseSalter.DEFAULT_PREFIX_LIST.toCharArray();
		
		byte [][] expectedStart = new byte[prefixArr.length][];
		byte [][] expectedStop = new byte[prefixArr.length][];
		Pair<byte[], byte[]> expectedPairs [] = new Pair[prefixArr.length];
		
		for(int i = 0; i < prefixArr.length; i++ ) {
			expectedStart[i] = Bytes.toBytes(prefixArr[i] + "_" + keyStrStart);
			expectedStop[i] = Bytes.toBytes(prefixArr[i] + "_" + keyStrStop);
			expectedPairs[i] = new Pair<byte[], byte[]>(expectedStart[i], expectedStop[i]);
		}
		
		Pair<byte[], byte[]> actualPairs [] = HBaseSalter.getDistributedIntervals(keyBytesStart, keyBytesStop);
		
		assertEquals(expectedPairs.length, actualPairs.length);
		
		for( int i = 0; i < expectedPairs.length; i++ ) {
//			System.out.println("".format("FIRST: EXPECTED: (%s) ACTUAL: (%s)", 
//					Bytes.toString(expectedPairs[i].getFirst()), Bytes.toString(actualPairs[i].getFirst()) ));
//
//			System.out.println("".format("SECOND: EXPECTED: (%s) ACTUAL: (%s)", 
//					Bytes.toString(expectedPairs[i].getSecond()), Bytes.toString(actualPairs[i].getSecond()) ));

			assertArrayEquals(expectedPairs[i].getFirst(), actualPairs[i].getFirst());
			assertArrayEquals(expectedPairs[i].getSecond(), actualPairs[i].getSecond());
		}
	}
	
	
	@Test
	public void getDistributedIntervalsWithPrefix() throws IOException {
		String keyStrStart = "1021";
		byte [] keyBytesStart = Bytes.toBytes(keyStrStart);

		String keyStrStop = "1022";
		byte [] keyBytesStop = Bytes.toBytes(keyStrStop);

		String prefix = "0123";
		char [] prefixArr = prefix.toCharArray();
		
		byte [][] expectedStart = new byte[prefixArr.length][];
		byte [][] expectedStop = new byte[prefixArr.length][];
		Pair<byte[], byte[]> expectedPairs [] = new Pair[prefixArr.length];
		
		for(int i = 0; i < prefixArr.length; i++ ) {
			expectedStart[i] = Bytes.toBytes(prefixArr[i] + "_" + keyStrStart);
			expectedStop[i] = Bytes.toBytes(prefixArr[i] + "_" + keyStrStop);
			expectedPairs[i] = new Pair<byte[], byte[]>(expectedStart[i], expectedStop[i]);
		}
		
		Pair<byte[], byte[]> actualPairs [] = HBaseSalter.getDistributedIntervals(keyBytesStart, keyBytesStop, prefix);
		
		assertEquals(expectedPairs.length, actualPairs.length);
		
		for( int i = 0; i < expectedPairs.length; i++ ) {
			System.out.println("".format("FIRST: EXPECTED: (%s) ACTUAL: (%s)", 
					Bytes.toString(expectedPairs[i].getFirst()), Bytes.toString(actualPairs[i].getFirst()) ));

			System.out.println("".format("SECOND: EXPECTED: (%s) ACTUAL: (%s)", 
					Bytes.toString(expectedPairs[i].getSecond()), Bytes.toString(actualPairs[i].getSecond()) ));

			assertArrayEquals(expectedPairs[i].getFirst(), actualPairs[i].getFirst());
			assertArrayEquals(expectedPairs[i].getSecond(), actualPairs[i].getSecond());
		}
	}
	
	@Test
	public void getDistributedIntervalsWithRegionsStartStop() throws IOException {
		String keyStrStart = "1021";
		byte [] keyBytesStart = Bytes.toBytes(keyStrStart);

		String keyStrStop = "1022";
		byte [] keyBytesStop = Bytes.toBytes(keyStrStop);
		
		byte [] regionStart = Bytes.toBytes("1");
		byte [] regionsStop = Bytes.toBytes("4");

		String expectedPrefix = "1234";
		char [] prefixArr = expectedPrefix.toCharArray();
		
		byte [][] expectedStart = new byte[prefixArr.length][];
		byte [][] expectedStop = new byte[prefixArr.length][];
		Pair<byte[], byte[]> expectedPairs [] = new Pair[prefixArr.length];
		
		for(int i = 0; i < prefixArr.length; i++ ) {
			expectedStart[i] = Bytes.toBytes(prefixArr[i] + "_" + keyStrStart);
			expectedStop[i] = Bytes.toBytes(prefixArr[i] + "_" + keyStrStop);
			expectedPairs[i] = new Pair<byte[], byte[]>(expectedStart[i], expectedStop[i]);
		}
		
		Pair<byte[], byte[]> actualPairs [] = HBaseSalter.getDistributedIntervals(keyBytesStart, keyBytesStop, regionStart, regionsStop, HBaseSalter.DEFAULT_PREFIX_LIST);
		
		assertEquals(expectedPairs.length, actualPairs.length);
		
		for( int i = 0; i < expectedPairs.length; i++ ) {
			System.out.println("".format("FIRST: EXPECTED: (%s) ACTUAL: (%s)", 
					Bytes.toString(expectedPairs[i].getFirst()), Bytes.toString(actualPairs[i].getFirst()) ));

			System.out.println("".format("SECOND: EXPECTED: (%s) ACTUAL: (%s)", 
					Bytes.toString(expectedPairs[i].getSecond()), Bytes.toString(actualPairs[i].getSecond()) ));

			assertArrayEquals(expectedPairs[i].getFirst(), actualPairs[i].getFirst());
			assertArrayEquals(expectedPairs[i].getSecond(), actualPairs[i].getSecond());
		}
	}
	
	
	@Test
	public void getDistributedIntervalsWithRegionsStartStopWithPrefixAll() throws IOException {
		System.out.println("------------ TEST 20 --------------");
		getDistributedIntervalsWithRegionsStartStopWithPrefix(
		    "1020", "1021",
		    "1_1021", "3_1023",
		    "123", "012345"
				);

		System.out.println("------------ TEST 21 --------------");
		getDistributedIntervalsWithRegionsStartStopWithPrefix(
			    "1020", "1021",
			    "2_1021", Bytes.toString(HConstants.EMPTY_END_ROW),
			    "2345", "012345"
					);

		System.out.println("------------ TEST 22 --------------");
		getDistributedIntervalsWithRegionsStartStopWithPrefix(
			    "1020", "1021",
			    Bytes.toString(HConstants.EMPTY_START_ROW), "3_1023",
			    "0123", "012345"
					);

		System.out.println("------------ TEST 23 --------------");
		getDistributedIntervalsWithRegionsStartStopWithPrefix(
			    "1020", "1021",
			    Bytes.toString(HConstants.EMPTY_START_ROW), Bytes.toString(HConstants.EMPTY_END_ROW),
			    "012345", "012345"
					);

		System.out.println("------------ TEST 24 --------------");
		getDistributedIntervalsWithRegionsStartStopWithPrefix(
			Bytes.toString(HConstants.EMPTY_START_ROW), "1021",
		    "1_1021", "3_1023",
		    "123", "012345"
				);

		System.out.println("------------ TEST 25 --------------");
		getDistributedIntervalsWithRegionsStartStopWithPrefix(
			"1020", Bytes.toString(HConstants.EMPTY_END_ROW),
		    "1_1021", "3_1023",
		    "123", "012345"
				);

		System.out.println("------------ TEST 26 --------------");
		getDistributedIntervalsWithRegionsStartStopWithPrefix(
				Bytes.toString(HConstants.EMPTY_START_ROW), Bytes.toString(HConstants.EMPTY_END_ROW),
		    "1_1021", "3_1023",
		    "123", "012345"
				);

		System.out.println("------------ TEST 27 --------------");
		getDistributedIntervalsWithRegionsStartStopWithPrefix(
			Bytes.toString(HConstants.EMPTY_START_ROW), "1021",
			Bytes.toString(HConstants.EMPTY_START_ROW), "3_1023",
		    "0123", "012345"
				);

		System.out.println("------------ TEST 28 --------------");
		getDistributedIntervalsWithRegionsStartStopWithPrefix(
			"1020", Bytes.toString(HConstants.EMPTY_END_ROW),
			Bytes.toString(HConstants.EMPTY_START_ROW), "3_1023",
		    "0123", "012345"
				);

		System.out.println("------------ TEST 29 --------------");
		getDistributedIntervalsWithRegionsStartStopWithPrefix(
			Bytes.toString(HConstants.EMPTY_START_ROW), Bytes.toString(HConstants.EMPTY_END_ROW),
			Bytes.toString(HConstants.EMPTY_START_ROW), "3_1023",
		    "0123", "012345"
				);

		System.out.println("------------ TEST 30 --------------");
		getDistributedIntervalsWithRegionsStartStopWithPrefix(
			Bytes.toString(HConstants.EMPTY_START_ROW), "1021",
		    "1_1021", Bytes.toString(HConstants.EMPTY_END_ROW),
		    "12345", "012345"
				);

		System.out.println("------------ TEST 31 --------------");
		getDistributedIntervalsWithRegionsStartStopWithPrefix(
			"1020", Bytes.toString(HConstants.EMPTY_END_ROW),
		    "1_1021", Bytes.toString(HConstants.EMPTY_END_ROW),
		    "12345", "012345"
				);

		System.out.println("------------ TEST 32 --------------");
		getDistributedIntervalsWithRegionsStartStopWithPrefix(
				Bytes.toString(HConstants.EMPTY_START_ROW), Bytes.toString(HConstants.EMPTY_END_ROW),
		    "1_1021", Bytes.toString(HConstants.EMPTY_END_ROW),
		    "12345", "012345"
				);

		System.out.println("------------ TEST 33 --------------");
		getDistributedIntervalsWithRegionsStartStopWithPrefix(
			Bytes.toString(HConstants.EMPTY_START_ROW), "1021",
			Bytes.toString(HConstants.EMPTY_START_ROW), Bytes.toString(HConstants.EMPTY_END_ROW),
		    "012345", "012345"
				);

		System.out.println("------------ TEST 34 --------------");
		getDistributedIntervalsWithRegionsStartStopWithPrefix(
			"1020", Bytes.toString(HConstants.EMPTY_END_ROW),
			Bytes.toString(HConstants.EMPTY_START_ROW), Bytes.toString(HConstants.EMPTY_END_ROW),
		    "012345", "012345"
				);

		System.out.println("------------ TEST 35 --------------");
		getDistributedIntervalsWithRegionsStartStopWithPrefix(
				Bytes.toString(HConstants.EMPTY_START_ROW), Bytes.toString(HConstants.EMPTY_END_ROW),
				Bytes.toString(HConstants.EMPTY_START_ROW), Bytes.toString(HConstants.EMPTY_END_ROW),
		    "012345", "012345"
				);

	}
	
	private void getDistributedIntervalsWithRegionsStartStopWithPrefix(
			String keyStrStart, String keyStrStop,
			String regionStrStart, String regionStrStop,
			String expectedPrefix, String sendPrefix) throws IOException {

		byte [] keyBytesStart = Bytes.toBytes(keyStrStart);
		byte [] keyBytesStop = Bytes.toBytes(keyStrStop);
		
		byte [] regionStart = Bytes.toBytes(regionStrStart);
		byte [] regionsStop = Bytes.toBytes(regionStrStop);

		char [] prefixArr = expectedPrefix.toCharArray();
		
		byte [][] expectedStart = new byte[prefixArr.length][];
		byte [][] expectedStop = new byte[prefixArr.length][];
		Pair<byte[], byte[]> expectedPairs [] = new Pair[prefixArr.length];
		
		for(int i = 0; i < prefixArr.length; i++ ) {
			expectedStart[i] = Bytes.toBytes(prefixArr[i] + "_" + keyStrStart);
			expectedStop[i] = Bytes.toBytes(prefixArr[i] + "_" + keyStrStop);
		}
		
		if( Arrays.equals(keyBytesStart, HConstants.EMPTY_START_ROW)
				&& Arrays.equals(keyBytesStop, HConstants.EMPTY_END_ROW) ) {
			for( int i = expectedStart.length - 1; i >=1; i--) {
				expectedStart[i] = expectedStart[i - 1];
			}
			
			expectedStart[0] = HConstants.EMPTY_START_ROW;
			expectedStop[expectedStop.length - 1] = HConstants.EMPTY_END_ROW;
		} else if(Arrays.equals(keyBytesStart, HConstants.EMPTY_START_ROW)) {
			for( int i = expectedStart.length - 1; i >=1; i--) {
				expectedStart[i] = expectedStart[i - 1];
			}
			
			expectedStart[0] = HConstants.EMPTY_START_ROW;
		} else if (Arrays.equals(keyBytesStop, HConstants.EMPTY_END_ROW)) {
			for(int i = 0; i < expectedStop.length - 1; i++ ) {
				expectedStop[i] = expectedStop[i + 1];
			}
			expectedStop[expectedStop.length - 1] = HConstants.EMPTY_END_ROW;
		}
		
		for(int i = 0; i < prefixArr.length; i++ ) {
			expectedPairs[i] = new Pair<byte[], byte[]>(expectedStart[i], expectedStop[i]);
		}
		
		Pair<byte[], byte[]> actualPairs [] = HBaseSalter.getDistributedIntervals(keyBytesStart, keyBytesStop, regionStart, regionsStop, sendPrefix);
		
		for(Pair<byte[], byte[]> p : expectedPairs ) {
			System.out.println("- EXPECTED " + Bytes.toString(p.getFirst())
					+ " -> " + Bytes.toString(p.getSecond()));
		}
		
		for(Pair<byte[], byte[]> p : actualPairs ) {
			System.out.println("- ACTUAL " + Bytes.toString(p.getFirst())
					+ " -> " + Bytes.toString(p.getSecond()));
		}
		
		assertEquals(expectedPairs.length, actualPairs.length);
		
		for( int i = 0; i < expectedPairs.length; i++ ) {
//			System.out.println("".format("FIRST: EXPECTED: (%s) ACTUAL: (%s)", 
//					Bytes.toString(expectedPairs[i].getFirst()), Bytes.toString(actualPairs[i].getFirst()) ));
//
//			System.out.println("".format("SECOND: EXPECTED: (%s) ACTUAL: (%s)", 
//					Bytes.toString(expectedPairs[i].getSecond()), Bytes.toString(actualPairs[i].getSecond()) ));

			assertArrayEquals(expectedPairs[i].getFirst(), actualPairs[i].getFirst());
			assertArrayEquals(expectedPairs[i].getSecond(), actualPairs[i].getSecond());
		}
	}
	
	
}
