package parallelai.spyglass.hbase;

import static org.apache.hadoop.hbase.mapreduce.TableRecordReaderImpl.LOG_PER_ROW_COUNT;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.Vector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.ScannerCallable;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableInputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Writables;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.util.StringUtils;

import parallelai.spyglass.hbase.HBaseConstants.SourceMode;

public class HBaseRecordReader implements
		RecordReader<ImmutableBytesWritable, Result> {

	static final Log LOG = LogFactory.getLog(HBaseRecordReader.class);

	private byte[] startRow;
	private byte[] endRow;
	private byte[] lastSuccessfulRow;
	private TreeSet<String> keyList;
	private SourceMode sourceMode;
	private Filter trrRowFilter;
	private ResultScanner scanner;
	private HTable htable;
	private byte[][] trrInputColumns;
	private long timestamp;
	private int rowcount;
	private boolean logScannerActivity = false;
	private int logPerRowCount = 100;
	private boolean endRowInclusive = true;
	private int versions = 1;
	private boolean useSalt = false;

	private HBaseMultiInputSplit multiSplit = null;
	private List<HBaseTableSplit> allSplits = null;

	private HBaseRecordReader() {
	}

	public HBaseRecordReader(HBaseMultiInputSplit mSplit) throws IOException {
		multiSplit = mSplit;

		LOG.info("Creatin Multi Split for region location : "
				+ multiSplit.getRegionLocation());

		allSplits = multiSplit.getSplits();
	}

	public boolean setNextSplit() throws IOException {
		if (allSplits.size() > 0) {
			setSplitValue(allSplits.remove(0));
			return true;
		} else {
			return false;
		}
	}

	private void setSplitValue(HBaseTableSplit tSplit) throws IOException {
		switch (tSplit.getSourceMode()) {
			case SCAN_ALL:
			case SCAN_RANGE: {
				LOG.debug(String.format(
						"For split [%s] we have start key (%s) and stop key (%s)",
						tSplit, tSplit.getStartRow(), tSplit.getEndRow()));

				setStartRow(tSplit.getStartRow());
				setEndRow(tSplit.getEndRow());
				setEndRowInclusive(tSplit.getEndRowInclusive());
			}

			break;

			case GET_LIST: {
				LOG.debug(String.format("For split [%s] we have key list (%s)",
						tSplit, tSplit.getKeyList()));

				setKeyList(tSplit.getKeyList());
				setVersions(tSplit.getVersions());
			}

			break;

			case EMPTY:
				LOG.info("EMPTY split. Doing nothing.");
			break;

			default:
				throw new IOException("Unknown source mode : "
						+ tSplit.getSourceMode());
		}

		setSourceMode(tSplit.getSourceMode());

		init();
	}

	/**
	 * Restart from survivable exceptions by creating a new scanner.
	 * 
	 * @param firstRow
	 * @throws IOException
	 */
	private void restartRangeScan(byte[] firstRow) throws IOException {
		Scan currentScan;
		if ((endRow != null) && (endRow.length > 0)) {
			if (trrRowFilter != null) {
				Scan scan = new Scan(firstRow, (endRowInclusive ? Bytes.add(endRow,
						new byte[] { 0 }) : endRow));

				TableInputFormat.addColumns(scan, trrInputColumns);
				scan.setFilter(trrRowFilter);
				scan.setCacheBlocks(false);
				this.scanner = this.htable.getScanner(scan);
				currentScan = scan;
			} else {
				LOG.debug("TIFB.restart, firstRow: " + Bytes.toString(firstRow)
						+ ", endRow: " + Bytes.toString(endRow));
				Scan scan = new Scan(firstRow, (endRowInclusive ? Bytes.add(endRow,
						new byte[] { 0 }) : endRow));
				TableInputFormat.addColumns(scan, trrInputColumns);
				this.scanner = this.htable.getScanner(scan);
				currentScan = scan;
			}
		} else {
			LOG.debug("TIFB.restart, firstRow: " + Bytes.toStringBinary(firstRow)
					+ ", no endRow");

			Scan scan = new Scan(firstRow);
			TableInputFormat.addColumns(scan, trrInputColumns);
			scan.setFilter(trrRowFilter);
			this.scanner = this.htable.getScanner(scan);
			currentScan = scan;
		}
		if (logScannerActivity) {
			LOG.debug("Current scan=" + currentScan.toString());
			timestamp = System.currentTimeMillis();
			rowcount = 0;
		}
	}

	public TreeSet<String> getKeyList() {
		return keyList;
	}

	private void setKeyList(TreeSet<String> keyList) {
		this.keyList = keyList;
	}

	private void setVersions(int versions) {
		this.versions = versions;
	}

	public void setUseSalt(boolean useSalt) {
		this.useSalt = useSalt;
	}

	public SourceMode getSourceMode() {
		return sourceMode;
	}

	private void setSourceMode(SourceMode sourceMode) {
		this.sourceMode = sourceMode;
	}

	public byte[] getEndRow() {
		return endRow;
	}

	private void setEndRowInclusive(boolean isInclusive) {
		endRowInclusive = isInclusive;
	}

	public boolean getEndRowInclusive() {
		return endRowInclusive;
	}

	private byte[] nextKey = null;
	private Vector<List<KeyValue>> resultVector = null;
	Map<Long, List<KeyValue>> keyValueMap = null;

	/**
	 * Build the scanner. Not done in constructor to allow for extension.
	 * 
	 * @throws IOException
	 */
	private void init() throws IOException {
		switch (sourceMode) {
			case SCAN_ALL:
			case SCAN_RANGE:
				restartRangeScan(startRow);
			break;

			case GET_LIST:
				nextKey = Bytes.toBytes(keyList.pollFirst());
			break;

			case EMPTY:
				LOG.info("EMPTY mode. Do nothing");
			break;

			default:
				throw new IOException(" Unknown source mode : " + sourceMode);
		}
	}

	byte[] getStartRow() {
		return this.startRow;
	}

	/**
	 * @param htable
	 *           the {@link HTable} to scan.
	 */
	public void setHTable(HTable htable) {
		Configuration conf = htable.getConfiguration();
		logScannerActivity = conf.getBoolean(
				ScannerCallable.LOG_SCANNER_ACTIVITY, false);
		logPerRowCount = conf.getInt(LOG_PER_ROW_COUNT, 100);
		this.htable = htable;
	}

	/**
	 * @param inputColumns
	 *           the columns to be placed in {@link Result}.
	 */
	public void setInputColumns(final byte[][] inputColumns) {
		this.trrInputColumns = inputColumns;
	}

	/**
	 * @param startRow
	 *           the first row in the split
	 */
	private void setStartRow(final byte[] startRow) {
		this.startRow = startRow;
	}

	/**
	 * 
	 * @param endRow
	 *           the last row in the split
	 */
	private void setEndRow(final byte[] endRow) {
		this.endRow = endRow;
	}

	/**
	 * @param rowFilter
	 *           the {@link Filter} to be used.
	 */
	public void setRowFilter(Filter rowFilter) {
		this.trrRowFilter = rowFilter;
	}

	@Override
	public void close() {
		if (this.scanner != null)
			this.scanner.close();
	}

	/**
	 * @return ImmutableBytesWritable
	 * 
	 * @see org.apache.hadoop.mapred.RecordReader#createKey()
	 */
	@Override
	public ImmutableBytesWritable createKey() {
		return new ImmutableBytesWritable();
	}

	/**
	 * @return RowResult
	 * 
	 * @see org.apache.hadoop.mapred.RecordReader#createValue()
	 */
	@Override
	public Result createValue() {
		return new Result();
	}

	@Override
	public long getPos() {
		// This should be the ordinal tuple in the range;
		// not clear how to calculate...
		return 0;
	}

	@Override
	public float getProgress() {
		// Depends on the total number of tuples and getPos
		return 0;
	}

	/**
	 * @param key
	 *           HStoreKey as input key.
	 * @param value
	 *           MapWritable as input value
	 * @return true if there was more data
	 * @throws IOException
	 */
	@Override
	public boolean next(ImmutableBytesWritable key, Result value)
			throws IOException {

		switch (sourceMode) {
			case SCAN_ALL:
			case SCAN_RANGE: {

				Result result;
				try {
					try {
						result = this.scanner.next();
						if (logScannerActivity) {
							rowcount++;
							if (rowcount >= logPerRowCount) {
								long now = System.currentTimeMillis();
								LOG.debug("Mapper took " + (now - timestamp)
										+ "ms to process " + rowcount + " rows");
								timestamp = now;
								rowcount = 0;
							}
						}
					} catch (IOException e) {
						// try to handle all IOExceptions by restarting
						// the scanner, if the second call fails, it will be rethrown
						LOG.debug("recovered from "
								+ StringUtils.stringifyException(e));
						if (lastSuccessfulRow == null) {
							LOG.warn("We are restarting the first next() invocation,"
									+ " if your mapper has restarted a few other times like this"
									+ " then you should consider killing this job and investigate"
									+ " why it's taking so long.");
						}
						if (lastSuccessfulRow == null) {
							restartRangeScan(startRow);
						} else {
							restartRangeScan(lastSuccessfulRow);
							this.scanner.next(); // skip presumed already mapped row
						}
						result = this.scanner.next();
					}

					if (result != null && result.size() > 0) {
						if (useSalt) {
							key.set(HBaseSalter.delSaltPrefix(result.getRow()));
						} else {
							key.set(result.getRow());
						}

						lastSuccessfulRow = key.get();
						Writables.copyWritable(result, value);
						return true;
					}
					return setNextSplit();
				} catch (IOException ioe) {
					if (logScannerActivity) {
						long now = System.currentTimeMillis();
						LOG.debug("Mapper took " + (now - timestamp)
								+ "ms to process " + rowcount + " rows");
						LOG.debug(ioe);
						String lastRow = lastSuccessfulRow == null ? "null" : Bytes
								.toStringBinary(lastSuccessfulRow);
						LOG.debug("lastSuccessfulRow=" + lastRow);
					}
					throw ioe;
				}
			}

			case GET_LIST: {
				LOG.debug(String.format("INTO next with GET LIST and Key (%s)",
						Bytes.toString(nextKey)));

				if (versions == 1) {
					if (nextKey != null) {
						LOG.debug(String.format("Processing Key (%s)",
								Bytes.toString(nextKey)));

						Get theGet = new Get(nextKey);
						theGet.setMaxVersions(versions);

						Result result = this.htable.get(theGet);

						if (result != null && (!result.isEmpty())) {
							LOG.debug(String.format(
									"Key (%s), Version (%s), Got Result (%s)",
									Bytes.toString(nextKey), versions, result));

							if (keyList != null || !keyList.isEmpty()) {
								String newKey = keyList.pollFirst();
								LOG.debug("New Key => " + newKey);
								nextKey = (newKey == null || newKey.length() == 0) ? null
										: Bytes.toBytes(newKey);
							} else {
								nextKey = null;
							}

							LOG.debug(String.format("=> Picked a new Key (%s)",
									Bytes.toString(nextKey)));

							// Write the result
							if (useSalt) {
								key.set(HBaseSalter.delSaltPrefix(result.getRow()));
							} else {
								key.set(result.getRow());
							}
							lastSuccessfulRow = key.get();
							Writables.copyWritable(result, value);

							return true;
						} else {
							LOG.debug(" Key (" + Bytes.toString(nextKey)
									+ ") return an EMPTY result. Get (" + theGet.getId()
									+ ")"); // alg0

							String newKey;
							while ((newKey = keyList.pollFirst()) != null) {
								LOG.debug("WHILE NEXT Key => " + newKey);

								nextKey = (newKey == null || newKey.length() == 0) ? null
										: Bytes.toBytes(newKey);

								if (nextKey == null) {
									LOG.error("BOMB! BOMB! BOMB!");
									continue;
								}

								if (!this.htable.exists(new Get(nextKey))) {
									LOG.debug(String.format(
											"Key (%s) Does not exist in Table (%s)",
											Bytes.toString(nextKey),
											Bytes.toString(this.htable.getTableName())));
									continue;
								} else {
									break;
								}
							}

							nextKey = (newKey == null || newKey.length() == 0) ? null
									: Bytes.toBytes(newKey);

							LOG.debug("Final New Key => " + Bytes.toString(nextKey));

							return next(key, value);
						}
					} else {
						// Nothig left. return false
						return setNextSplit();
					}
				} else {
					if (resultVector != null && resultVector.size() != 0) {
						LOG.debug(String.format("+ Version (%s), Result VECTOR <%s>",
								versions, resultVector));

						List<KeyValue> resultKeyValue = resultVector
								.remove(resultVector.size() - 1);
						Result result = new Result(resultKeyValue);

						LOG.debug(String.format("+ Version (%s), Got Result <%s>",
								versions, result));

						if (useSalt) {
							key.set(HBaseSalter.delSaltPrefix(result.getRow()));
						} else {
							key.set(result.getRow());
						}
						lastSuccessfulRow = key.get();
						Writables.copyWritable(result, value);

						return true;
					} else {
						if (nextKey != null) {
							LOG.debug(String.format("+ Processing Key (%s)",
									Bytes.toString(nextKey)));

							Get theGet = new Get(nextKey);
							theGet.setMaxVersions(versions);

							Result resultAll = this.htable.get(theGet);

							if (resultAll != null && (!resultAll.isEmpty())) {
								List<KeyValue> keyValeList = resultAll.list();

								keyValueMap = new HashMap<Long, List<KeyValue>>();

								LOG.debug(String.format(
										"+ Key (%s) Versions (%s) Val;ute map <%s>",
										Bytes.toString(nextKey), versions, keyValueMap));

								for (KeyValue keyValue : keyValeList) {
									long version = keyValue.getTimestamp();

									if (keyValueMap.containsKey(new Long(version))) {
										List<KeyValue> keyValueTempList = keyValueMap
												.get(new Long(version));
										if (keyValueTempList == null) {
											keyValueTempList = new ArrayList<KeyValue>();
										}
										keyValueTempList.add(keyValue);
									} else {
										List<KeyValue> keyValueTempList = new ArrayList<KeyValue>();
										keyValueMap.put(new Long(version),
												keyValueTempList);
										keyValueTempList.add(keyValue);
									}
								}

								resultVector = new Vector<List<KeyValue>>();
								resultVector.addAll(keyValueMap.values());

								List<KeyValue> resultKeyValue = resultVector
										.remove(resultVector.size() - 1);

								Result result = new Result(resultKeyValue);

								LOG.debug(String.format(
										"+ Version (%s), Got Result (%s)", versions,
										result));

								String newKey = keyList.pollFirst(); // Bytes.toString(resultKeyValue.getKey());//

								System.out.println("+ New Key => " + newKey);
								nextKey = (newKey == null || newKey.length() == 0) ? null
										: Bytes.toBytes(newKey);

								if (useSalt) {
									key.set(HBaseSalter.delSaltPrefix(result.getRow()));
								} else {
									key.set(result.getRow());
								}
								lastSuccessfulRow = key.get();
								Writables.copyWritable(result, value);
								return true;
							} else {
								LOG.debug(String.format(
										"+ Key (%s) return an EMPTY result. Get (%s)",
										Bytes.toString(nextKey), theGet.getId())); // alg0

								String newKey;

								while ((newKey = keyList.pollFirst()) != null) {
									LOG.debug("+ WHILE NEXT Key => " + newKey);

									nextKey = (newKey == null || newKey.length() == 0) ? null
											: Bytes.toBytes(newKey);

									if (nextKey == null) {
										LOG.error("+ BOMB! BOMB! BOMB!");
										continue;
									}

									if (!this.htable.exists(new Get(nextKey))) {
										LOG.debug(String.format(
												"+ Key (%s) Does not exist in Table (%s)",
												Bytes.toString(nextKey),
												Bytes.toString(this.htable.getTableName())));
										continue;
									} else {
										break;
									}
								}

								nextKey = (newKey == null || newKey.length() == 0) ? null
										: Bytes.toBytes(newKey);

								LOG.debug("+ Final New Key => "
										+ Bytes.toString(nextKey));

								return next(key, value);
							}

						} else {
							return setNextSplit();
						}
					}
				}
			}

			case EMPTY: {
				LOG.info("GOT an empty Split");
				return setNextSplit();
			}

			default:
				throw new IOException("Unknown source mode : " + sourceMode);
		}
	}
}
