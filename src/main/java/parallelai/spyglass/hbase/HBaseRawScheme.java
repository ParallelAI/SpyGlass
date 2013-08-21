/*
* Copyright (c) 2009 Concurrent, Inc.
*
* This work has been released into the public domain
* by the copyright holder. This applies worldwide.
*
* In case this is not legally possible:
* The copyright holder grants any entity the right
* to use this work for any purpose, without any
* conditions, unless such conditions are required by law.
*/

package parallelai.spyglass.hbase;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashSet;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapred.TableOutputFormat;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.RecordReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twitter.elephantbird.mapred.input.DeprecatedInputFormatValueCopier;
import com.twitter.elephantbird.mapred.input.DeprecatedInputFormatWrapper;

import cascading.flow.FlowProcess;
import cascading.scheme.Scheme;
import cascading.scheme.SinkCall;
import cascading.scheme.SourceCall;
import cascading.tap.Tap;
import cascading.tuple.Fields;
import cascading.tuple.Tuple;
import cascading.tuple.TupleEntry;
import cascading.util.Util;

/**
* The HBaseRawScheme class is a {@link Scheme} subclass. It is used in conjunction
* with the {@HBaseRawTap} to allow for the reading and writing of data
* to and from a HBase cluster.
*
* @see HBaseRawTap
*/
@SuppressWarnings({ "rawtypes", "deprecation" })
public class HBaseRawScheme extends Scheme<JobConf, RecordReader, OutputCollector, Object[], Object[]> {
	/**
	 *
	 */
	private static final long serialVersionUID = 6248976486883281356L;

	/** Field LOG */
	private static final Logger LOG = LoggerFactory.getLogger(HBaseRawScheme.class);

	public final Fields RowKeyField = new Fields("rowkey");
	public final Fields RowField = new Fields("row");

	/** String familyNames */
	private String[] familyNames;

	private boolean writeNulls = true;

	/**
	 * Constructor HBaseScheme creates a new HBaseScheme instance.
	 *
	 * @param keyFields
	 *            of type Fields
	 * @param familyName
	 *            of type String
	 * @param valueFields
	 *            of type Fields
	 */
	public HBaseRawScheme(String familyName) {
		this(new String[] { familyName });
	}

	public HBaseRawScheme(String[] familyNames) {
		this.familyNames = familyNames;
		setSourceFields();
	}

	public HBaseRawScheme(String familyName, boolean writeNulls) {
		this(new String[] { familyName }, writeNulls);
	}

	public HBaseRawScheme(String[] familyNames, boolean writeNulls) {
		this.familyNames = familyNames;
		this.writeNulls = writeNulls;
		setSourceFields();
	}

	private void setSourceFields() {
		Fields sourceFields = Fields.join(RowKeyField, RowField);
		setSourceFields(sourceFields);
	}

	/**
	 * Method getFamilyNames returns the set of familyNames of this HBaseScheme
	 * object.
	 *
	 * @return the familyNames (type String[]) of this HBaseScheme object.
	 */
	public String[] getFamilyNames() {
		HashSet<String> familyNameSet = new HashSet<String>();
		if (familyNames != null) {
			for (String familyName : familyNames) {
				familyNameSet.add(familyName);
			}
		}
		return familyNameSet.toArray(new String[0]);
	}

	@Override
	public void sourcePrepare(FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall) {
		Object[] pair = new Object[] { sourceCall.getInput().createKey(), sourceCall.getInput().createValue() };

		sourceCall.setContext(pair);
	}

	@Override
	public void sourceCleanup(FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall) {
		sourceCall.setContext(null);
	}

	@SuppressWarnings("unchecked")
	@Override
	public boolean source(FlowProcess<JobConf> flowProcess, SourceCall<Object[], RecordReader> sourceCall)
			throws IOException {
		Tuple result = new Tuple();

		Object key = sourceCall.getContext()[0];
		Object value = sourceCall.getContext()[1];
		boolean hasNext = sourceCall.getInput().next(key, value);
		if (!hasNext) {
			return false;
		}

		// Skip nulls
		if (key == null || value == null) {
			return true;
		}

		ImmutableBytesWritable keyWritable = (ImmutableBytesWritable) key;
		Result row = (Result) value;
		result.add(keyWritable);
		result.add(row);
		sourceCall.getIncomingEntry().setTuple(result);
		return true;
	}

	@SuppressWarnings("unchecked")
	@Override
	public void sink(FlowProcess<JobConf> flowProcess, SinkCall<Object[], OutputCollector> sinkCall) throws IOException {
		TupleEntry tupleEntry = sinkCall.getOutgoingEntry();
		OutputCollector outputCollector = sinkCall.getOutput();
		Tuple key = tupleEntry.selectTuple(RowKeyField);
		Object okey = key.getObject(0);
		ImmutableBytesWritable keyBytes = getBytes(okey);
		Put put = new Put(keyBytes.get());
		Fields outFields = tupleEntry.getFields().subtract(RowKeyField);
		if (null != outFields) {
			TupleEntry values = tupleEntry.selectEntry(outFields);
			for (int n = 0; n < values.getFields().size(); n++) {
				Object o = values.get(n);
				ImmutableBytesWritable valueBytes = getBytes(o);
				Comparable field = outFields.get(n);
				ColumnName cn = parseColumn((String) field);
				if (null == cn.family) {
					if (n >= familyNames.length)
						cn.family = familyNames[familyNames.length - 1];
					else
						cn.family = familyNames[n];
				}
				if (null != o || writeNulls)
					put.add(Bytes.toBytes(cn.family), Bytes.toBytes(cn.name), valueBytes.get());
			}
		}
		outputCollector.collect(null, put);
	}

	private ImmutableBytesWritable getBytes(Object obj) {
		if (null == obj)
			return new ImmutableBytesWritable(new byte[0]);
		if (obj instanceof ImmutableBytesWritable)
			return (ImmutableBytesWritable) obj;
		else if (obj instanceof String)
			return new ImmutableBytesWritable(Bytes.toBytes((String) obj));
		else if (obj instanceof Long)
			return new ImmutableBytesWritable(Bytes.toBytes((Long) obj));
		else if (obj instanceof Integer)
			return new ImmutableBytesWritable(Bytes.toBytes((Integer) obj));
		else if (obj instanceof Short)
			return new ImmutableBytesWritable(Bytes.toBytes((Short) obj));
		else if (obj instanceof Boolean)
			return new ImmutableBytesWritable(Bytes.toBytes((Boolean) obj));
		else if (obj instanceof Double)
			return new ImmutableBytesWritable(Bytes.toBytes((Double) obj));
		else
			throw new IllegalArgumentException("cannot convert object to ImmutableBytesWritable, class="
					+ obj.getClass().getName());
	}

	private ColumnName parseColumn(String column) {
		ColumnName ret = new ColumnName();
		int pos = column.indexOf(":");
		if (pos > 0) {
			ret.name = column.substring(pos + 1);
			ret.family = column.substring(0, pos);
		} else {
			ret.name = column;
		}
		return ret;
	}

	private class ColumnName {
		String family;
		String name;

		ColumnName() {
		}
	}

	@Override
	public void sinkConfInit(FlowProcess<JobConf> process, Tap<JobConf, RecordReader, OutputCollector> tap, JobConf conf) {
		conf.setOutputFormat(TableOutputFormat.class);
		conf.setOutputKeyClass(ImmutableBytesWritable.class);
		conf.setOutputValueClass(Put.class);
	}

	@Override
	public void sourceConfInit(FlowProcess<JobConf> process, Tap<JobConf, RecordReader, OutputCollector> tap,
			JobConf conf) {
		DeprecatedInputFormatWrapper.setInputFormat(org.apache.hadoop.hbase.mapreduce.TableInputFormat.class, conf,
				ValueCopier.class);
		if (null != familyNames) {
			String columns = Util.join(this.familyNames, " ");
			LOG.debug("sourcing from column families: {}", columns);
			conf.set(org.apache.hadoop.hbase.mapreduce.TableInputFormat.SCAN_COLUMNS, columns);
		}
	}

	@Override
	public boolean equals(Object object) {
		if (this == object) {
			return true;
		}
		if (object == null || getClass() != object.getClass()) {
			return false;
		}
		if (!super.equals(object)) {
			return false;
		}

		HBaseRawScheme that = (HBaseRawScheme) object;

		if (!Arrays.equals(familyNames, that.familyNames)) {
			return false;
		}
		return true;
	}

	@Override
	public int hashCode() {
		int result = super.hashCode();
		result = 31 * result + (familyNames != null ? Arrays.hashCode(familyNames) : 0);
		return result;
	}

	public static class ValueCopier implements DeprecatedInputFormatValueCopier<Result> {

		public ValueCopier() {
		}

		public void copyValue(Result oldValue, Result newValue) {
			if (null != oldValue && null != newValue) {
				oldValue.copyFrom(newValue);
			}
		}

	}
}
