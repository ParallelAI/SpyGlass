package parallelai.spyglass.hbase;

import java.io.IOException;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.UUID;

import javax.naming.NamingException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionLocation;
import org.apache.hadoop.hbase.HServerAddress;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.util.Addressing;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.hadoop.hbase.util.Strings;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.JobConfigurable;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.net.DNS;
import org.apache.hadoop.util.StringUtils;

import parallelai.spyglass.hbase.HBaseConstants.SourceMode;

public class HBaseInputFormatGranular extends HBaseInputFormatBase {

	private final Log LOG = LogFactory.getLog(HBaseInputFormatGranular.class);

	// private String tableName = "";

	private HashMap<InetAddress, String> reverseDNSCacheMap = new HashMap<InetAddress, String>();

	private String nameServer = null;

	// private Scan scan = null;

	@SuppressWarnings("deprecation")
	@Override
	public HBaseTableSplitGranular[] getSplits(JobConf job, int numSplits) throws IOException {
		if (this.table == null) {
			throw new IOException("No table was provided");
		}

		if (this.inputColumns == null || this.inputColumns.length == 0) {
			throw new IOException("Expecting at least one column");
		}

		Pair<byte[][], byte[][]> keys = table.getStartEndKeys();

		if (keys == null || keys.getFirst() == null
				|| keys.getFirst().length == 0) {
			HRegionLocation regLoc = table.getRegionLocation(
					HConstants.EMPTY_BYTE_ARRAY, false);

			if (null == regLoc) {
				throw new IOException("Expecting at least one region.");
			}

			List<HBaseTableSplitGranular> splits = new ArrayList<HBaseTableSplitGranular>(1);
			HBaseTableSplitGranular split = new HBaseTableSplitGranular(table.getTableName(),
					HConstants.EMPTY_BYTE_ARRAY, HConstants.EMPTY_BYTE_ARRAY, regLoc
							.getHostnamePort().split(
									Addressing.HOSTNAME_PORT_SEPARATOR)[0], regLoc.getRegionInfo().getRegionNameAsString(),
					SourceMode.EMPTY, false);

			splits.add(split);

			return splits.toArray(new HBaseTableSplitGranular[splits.size()]);
		}

		if (keys.getSecond() == null || keys.getSecond().length == 0) {
			throw new IOException("Expecting at least one region.");
		}

		if (keys.getFirst().length != keys.getSecond().length) {
			throw new IOException("Regions for start and end key do not match");
		}

		byte[] minKey = keys.getFirst()[keys.getFirst().length - 1];
		byte[] maxKey = keys.getSecond()[0];

		LOG.debug(String.format("SETTING min key (%s) and max key (%s)",
				Bytes.toString(minKey), Bytes.toString(maxKey)));

		byte[][] regStartKeys = keys.getFirst();
		byte[][] regStopKeys = keys.getSecond();
		String[] regions = new String[regStartKeys.length];
        String[] regionNames = new String[regStartKeys.length];

		for (int i = 0; i < regStartKeys.length; i++) {
			minKey = (regStartKeys[i] != null && regStartKeys[i].length != 0)
					&& (Bytes.compareTo(regStartKeys[i], minKey) < 0) ? regStartKeys[i]
					: minKey;
			maxKey = (regStopKeys[i] != null && regStopKeys[i].length != 0)
					&& (Bytes.compareTo(regStopKeys[i], maxKey) > 0) ? regStopKeys[i]
					: maxKey;

			HRegionLocation regionLoc = table.getRegionLocation(keys.getFirst()[i]);
            HServerAddress regionServerAddress = regionLoc.getServerAddress();
			InetAddress regionAddress = regionServerAddress.getInetSocketAddress().getAddress();
			String regionLocation;
			try {
				regionLocation = reverseDNS(regionAddress);
			} catch (NamingException e) {
				LOG.error("Cannot resolve the host name for " + regionAddress
						+ " because of " + e);
				regionLocation = regionServerAddress.getHostname();
			}

            regionNames[i] = regionLoc.getRegionInfo().getRegionNameAsString();

			LOG.debug("***** " + regionLocation);

			if (regionLocation == null || regionLocation.length() == 0)
				throw new IOException("The region info for regiosn " + i
						+ " is null or empty");

			regions[i] = regionLocation;

			LOG.debug(String.format(
					"Region (%s) has start key (%s) and stop key (%s)", regions[i],
					Bytes.toString(regStartKeys[i]), Bytes.toString(regStopKeys[i])));
		}

		byte[] startRow = HConstants.EMPTY_START_ROW;
		byte[] stopRow = HConstants.EMPTY_END_ROW;

		LOG.debug(String.format("Found min key (%s) and max key (%s)",
				Bytes.toString(minKey), Bytes.toString(maxKey)));

		LOG.debug("SOURCE MODE is : " + sourceMode);

		switch (sourceMode) {
			case SCAN_ALL:
				startRow = HConstants.EMPTY_START_ROW;
				stopRow = HConstants.EMPTY_END_ROW;

				LOG.debug(String.format(
						"SCAN ALL: Found start key (%s) and stop key (%s)",
						Bytes.toString(startRow), Bytes.toString(stopRow)));
			break;

			case SCAN_RANGE:
				startRow = (startKey != null && startKey.length() != 0) ? Bytes
						.toBytes(startKey) : HConstants.EMPTY_START_ROW;
				stopRow = (stopKey != null && stopKey.length() != 0) ? Bytes
						.toBytes(stopKey) : HConstants.EMPTY_END_ROW;

				LOG.debug(String.format(
						"SCAN RANGE: Found start key (%s) and stop key (%s)",
						Bytes.toString(startRow), Bytes.toString(stopRow)));
			break;
		}

		switch (sourceMode) {
			case EMPTY:
			case SCAN_ALL:
			case SCAN_RANGE: {
				// startRow = (Bytes.compareTo(startRow, minKey) < 0) ? minKey :
				// startRow;
				// stopRow = (Bytes.compareTo(stopRow, maxKey) > 0) ? maxKey :
				// stopRow;

				List<HBaseTableSplitGranular> splits = new ArrayList<HBaseTableSplitGranular>();

				if (!useSalt) {

					List<HRegionLocation> validRegions = table.getRegionsInRange(
							startRow, stopRow);

					int maxRegions = validRegions.size();
					int currentRegion = 1;

					for (HRegionLocation cRegion : validRegions) {
						byte[] rStart = cRegion.getRegionInfo().getStartKey();
						byte[] rStop = cRegion.getRegionInfo().getEndKey();

						HServerAddress regionServerAddress = cRegion
								.getServerAddress();
						InetAddress regionAddress = regionServerAddress
								.getInetSocketAddress().getAddress();
						String regionLocation;
						try {
							regionLocation = reverseDNS(regionAddress);
						} catch (NamingException e) {
							LOG.error("Cannot resolve the host name for "
									+ regionAddress + " because of " + e);
							regionLocation = regionServerAddress.getHostname();
						}

                        String regionName = cRegion.getRegionInfo().getRegionNameAsString();

                        byte[] sStart = (startRow == HConstants.EMPTY_START_ROW
								|| (Bytes.compareTo(startRow, rStart) <= 0) ? rStart
								: startRow);
						byte[] sStop = (stopRow == HConstants.EMPTY_END_ROW
								|| (Bytes.compareTo(stopRow, rStop) >= 0 && rStop.length != 0) ? rStop
								: stopRow);

						LOG.debug(String.format(
								"BOOL start (%s) stop (%s) length (%d)",
								(startRow == HConstants.EMPTY_START_ROW || (Bytes
										.compareTo(startRow, rStart) <= 0)),
								(stopRow == HConstants.EMPTY_END_ROW || (Bytes
										.compareTo(stopRow, rStop) >= 0)), rStop.length));

						HBaseTableSplitGranular split = new HBaseTableSplitGranular(
								table.getTableName(), sStart, sStop, regionLocation, regionName,
								SourceMode.SCAN_RANGE, useSalt);

						split.setEndRowInclusive(currentRegion == maxRegions);

						currentRegion++;

						LOG.debug(String
								.format(
										"START KEY (%s) STOP KEY (%s) rSTART (%s) rSTOP (%s) sSTART (%s) sSTOP (%s) REGION [%s] SPLIT [%s]",
										Bytes.toString(startRow),
										Bytes.toString(stopRow), Bytes.toString(rStart),
										Bytes.toString(rStop), Bytes.toString(sStart),
										Bytes.toString(sStop), cRegion.getHostnamePort(),
										split));

						splits.add(split);
					}
				} else {
					LOG.debug("Using SALT : " + useSalt);

					// Will return the start and the stop key with all possible
					// prefixes.
					for (int i = 0; i < regions.length; i++) {
						Pair<byte[], byte[]>[] intervals = HBaseSalter
								.getDistributedIntervals(startRow, stopRow,
										regStartKeys[i], regStopKeys[i], prefixList);

						for (Pair<byte[], byte[]> pair : intervals) {
							LOG.debug("".format(
									"Using SALT, Region (%s) Start (%s) Stop (%s)",
									regions[i], Bytes.toString(pair.getFirst()),
									Bytes.toString(pair.getSecond())));

							HBaseTableSplitGranular split = new HBaseTableSplitGranular(
									table.getTableName(), pair.getFirst(),
									pair.getSecond(), regions[i], regionNames[i], SourceMode.SCAN_RANGE,
									useSalt);

							split.setEndRowInclusive(true);
							splits.add(split);
						}
					}
				}

				LOG.debug("RETURNED NO OF SPLITS: split -> " + splits.size());
				for (HBaseTableSplitGranular s : splits) {
					LOG.debug("RETURNED SPLITS: split -> " + s);
				}

				return splits.toArray(new HBaseTableSplitGranular[splits.size()]);
			}

			case GET_LIST: {
				// if( keyList == null || keyList.size() == 0 ) {
				if (keyList == null) {
					throw new IOException(
							"Source Mode is GET_LIST but key list is EMPTY");
				}

				if (useSalt) {
					TreeSet<String> tempKeyList = new TreeSet<String>();

					for (String key : keyList) {
						tempKeyList.add(HBaseSalter.addSaltPrefix(key));
					}

					keyList = tempKeyList;
				}

				LOG.debug("".format("Splitting Key List (%s)", keyList));

				List<HBaseTableSplitGranular> splits = new ArrayList<HBaseTableSplitGranular>();

				for (int i = 0; i < keys.getFirst().length; i++) {

					if (!includeRegionInSplit(keys.getFirst()[i],
							keys.getSecond()[i])) {
						continue;
					}

					LOG.debug(String.format(
							"Getting region (%s) subset (%s) to (%s)", regions[i],
							Bytes.toString(regStartKeys[i]),
							Bytes.toString(regStopKeys[i])));

					Set<String> regionsSubSet = null;

					if ((regStartKeys[i] == null || regStartKeys[i].length == 0)
							&& (regStopKeys[i] == null || regStopKeys[i].length == 0)) {
						LOG.debug("REGION start is empty");
						LOG.debug("REGION stop is empty");
						regionsSubSet = keyList;
					} else if (regStartKeys[i] == null
							|| regStartKeys[i].length == 0) {
						LOG.debug("REGION start is empty");
						regionsSubSet = keyList.headSet(
								Bytes.toString(regStopKeys[i]), true);
					} else if (regStopKeys[i] == null || regStopKeys[i].length == 0) {
						LOG.debug("REGION stop is empty");
						regionsSubSet = keyList.tailSet(
								Bytes.toString(regStartKeys[i]), true);
					} else if (Bytes.compareTo(regStartKeys[i], regStopKeys[i]) <= 0) {
						regionsSubSet = keyList.subSet(
								Bytes.toString(regStartKeys[i]), true,
								Bytes.toString(regStopKeys[i]), true);
					} else {
						throw new IOException(String.format(
								"For REGION (%s) Start Key (%s) > Stop Key(%s)",
								regions[i], Bytes.toString(regStartKeys[i]),
								Bytes.toString(regStopKeys[i])));
					}

					if (regionsSubSet == null || regionsSubSet.size() == 0) {
						LOG.debug("EMPTY: Key is for region " + regions[i]
								+ " is null");

						continue;
					}

					TreeSet<String> regionKeyList = new TreeSet<String>(
							regionsSubSet);

					LOG.debug(String.format("Regions [%s] has key list <%s>",
							regions[i], regionKeyList));

					HBaseTableSplitGranular split = new HBaseTableSplitGranular(
							table.getTableName(), regionKeyList, versions, regions[i], regionNames[i],
							SourceMode.GET_LIST, useSalt);
					splits.add(split);
				}

				LOG.debug("RETURNED SPLITS: split -> " + splits);

				return splits.toArray(new HBaseTableSplitGranular[splits.size()]);
			}

			default:
				throw new IOException("Unknown source Mode : " + sourceMode);
		}
	}

	private String reverseDNS(InetAddress ipAddress) throws NamingException {
		String hostName = this.reverseDNSCacheMap.get(ipAddress);
		if (hostName == null) {
			hostName = Strings.domainNamePointerToHostName(DNS.reverseDns(
					ipAddress, this.nameServer));
			this.reverseDNSCacheMap.put(ipAddress, hostName);
		}
		return hostName;
	}

	@Override
	public RecordReader<ImmutableBytesWritable, Result> getRecordReader(
			InputSplit split, JobConf job, Reporter reporter) throws IOException {

        LOG.info("GRANULAR SPLIT -> " + split);

        if (!(split instanceof HBaseTableSplitGranular))
			throw new IOException("Table Split is not type HBaseTableSplitGranular");

		HBaseTableSplitGranular tSplit = (HBaseTableSplitGranular) split;

		HBaseRecordReaderGranular trr = new HBaseRecordReaderGranular();

        HBaseConfigUtils.setRecordReaderParms(trr, tSplit);

		trr.setHTable(this.table);
		trr.setInputColumns(this.inputColumns);
		trr.setRowFilter(this.rowFilter);

		trr.init();

		return trr;
	}

	/* Configuration Section */

	/**
	 * space delimited list of columns
	 */

	public void validateInput(JobConf job) throws IOException {
		// expecting exactly one path
		String tableName = getTableName(job);

		if (tableName == null) {
			throw new IOException("expecting one table name");
		}
		LOG.debug(String.format("Found Table name [%s]", tableName));

		// connected to table?
		if (getHTable() == null) {
			throw new IOException("could not connect to table '" + tableName + "'");
		}
		LOG.debug(String.format("Found Table [%s]", getHTable().getTableName()));

		// expecting at least one column
		String colArg = job.get(COLUMN_LIST);
		if (colArg == null || colArg.length() == 0) {
			throw new IOException("expecting at least one column");
		}
		LOG.debug(String.format("Found Columns [%s]", colArg));

		LOG.debug(String.format("Found Start & STop Key [%s][%s]", startKey,
				stopKey));

		if (sourceMode == SourceMode.EMPTY) {
			throw new IOException("SourceMode should not be EMPTY");
		}

		if (sourceMode == SourceMode.GET_LIST
				&& (keyList == null || keyList.size() == 0)) {
			throw new IOException("Source mode is GET_LIST bu key list is empty");
		}
	}


	protected boolean includeRegionInSplit(final byte[] startKey,
			final byte[] endKey) {
		return true;
	}
}
