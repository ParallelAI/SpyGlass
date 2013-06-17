SpyGlass
========

Cascading and Scalding wrapper for HBase/JDBC with advanced read and write features.

Prevent Hot Spotting by the use of transparent key prefixes.

Building
========

	$ mvn clean install -U
	
	Requires Maven 3.x.x
	
To use SpyGlass as a dependency use the following repository

	<repositories>
	    <repository>
	        <id>parallelai-releases</id>
	        <url>https://github.com/ParallelAI/mvn-repo/raw/master/releases</url>
	    </repository>
	</repositories>
	
	<dependencies>
		<dependency>
			<groupId>parallelai</groupId>
			<artifactId>parallelai.spyglass</artifactId>
			<version>2.0.3</version>
		</dependency>
	</dependencies>

	

1. Read Mode Features
=====================

HBaseSource supports modes namely **GET_LIST**, **SCAN_RANGE** and **SCAN_ALL**. Use the **_sourceMode_** parameter to select the source mode.

  - **GET_LIST** -> Provide a list of keys to retrieve from the HBase table
  - **SCAN_RANGE** -> Provide a start and stop key (inclusive) to get out of the HBase table.
  - **SCAN_ALL** -> Get all rows form the HBase Table
  
GET_LIST
--------
Requires the **_keyList_** parameter to be specified as well.

(e.g.)

	val hbs2 = new HBaseSource(
	    "table_name",
	    "quorum_name:2181",
	    'key,
	    Array("column_family"),
	    Array('column_name),
	    sourceMode = SourceMode.GET_LIST, keyList = List("5003914", "5000687", "5004897"))
	    
	    
Additionally, the **_versions_** parameter can be used to retrieve more than one version of the row. 

(e.g.)

	val hbs2 = new HBaseSource(
	    "table_name",
	    "quorum_name:2181",
	    'key,
	    Array("column_family"),
	    Array('column_name),
	    sourceMode = SourceMode.GET_LIST,
	    versions = 5, 
	    keyList = List("5003914", "5000687", "5004897"))
	
	    
SCAN_RANGE
----------
Scan range uses the optional **_startKey_** and **_stopKey_** parameters to specify the range of keys to extract; both keys are inclusive. 

if:
 - Only **_startKey_** provided -> All rows from **_startKey_** till **END OF TABLE** are returned
 - Only **_stopKey_** provided -> All rows from **START OF TABLE** till **_stopKey_** are returned
 - Neither provided -> All rows in table are returned.
 
(e.g.)

	  val hbs4 = new HBaseSource(
	    "table_name",
	    "quorum_name:2181",
	    'key,
	    Array("column_family"),
	    Array('column_name),
	    sourceMode = SourceMode.SCAN_RANGE, stopKey = "5003914")
	    .read
	    .write(Tsv(output.format("scan_range_to_end")))
	
	  val hbs5 = new HBaseSource(
	    "table_name",
	    "quorum_name:2181",
	    'key,
	    Array("column_family"),
	    Array('column_name),
	    sourceMode = SourceMode.SCAN_RANGE, startKey = "5003914")
	    .read
	    .write(Tsv(output.format("scan_range_from_start")))
	
	  val hbs6 = new HBaseSource(
	    "table_name",
	    "quorum_name:2181",
	    'key,
	    Array("column_family"),
	    Array('column_name),
	    sourceMode = SourceMode.SCAN_RANGE, startKey = "5003914", stopKey = "5004897")
	    .read
	    .write(Tsv(output.format("scan_range_between")))
 
 
SCAN_ALL
--------
Returns all rows in the table

(e.g.)

	val hbs2 = new HBaseSource(
	    "table_name",
	    "quorum_name:2181",
	    'key,
	    Array("column_family"),
	    Array('column_name),
	    sourceMode = SourceMode.SCAN_ALL)


2. Write Mode Features
======================

HBaseSource supports writing at a particular time stamp i.e. a version. 

The time dimension can be added to the row by using the **_timestamp_** parameter. If the parameter is not present the current time is used.

(e.g.)
   
	pipe.write(new HBaseSource( "table_name",
		"quorum_name:2181",
		'key,  
	   TABLE_SCHEMA.tail.map((x: Symbol) => "data").toArray, 
	   TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)).toArray,
	   timestamp = Platform.currentTime ))
	

3. Region Hot Spot Prevention
=============================

Region hot spotting is a common problem with HBase. Spy Glass uses key prefix salting to avoid this. 
The row key is prefixed with the last byte followed by a '_' (underscore) character.

(e.g.)

	Original Row Key   ->  Becomes
	SPYGLASS           ->  S_SPYGLASS
	12345678           ->  8_12345678

Conversion to and from salted keys is done automatically.

Setting the **_useSalt_** parameter to **true** enables this functionality


(e.g.)

	  val TABLE_SCHEMA = List('key, 'salted, 'unsalted)
	
	  val hbase07 = 
	  new HBaseSource( "table_name",
		"quorum_name:2181", 'key,  
	      TABLE_SCHEMA.tail.map((x: Symbol) => "data").toArray, 
	      TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)).toArray,
	      sourceMode = SourceMode.SCAN_RANGE, startKey = "11445", stopKey = "11455", 
	      useSalt = true, prefixList = "0123456789" )
	  .read
	  
	  // Convert from ImmutableBytesWritable to String 
	  .fromBytesWritable( TABLE_SCHEMA )
	
	  .write(TextLine("saltTesting/ScanRangePlusSalt10"))
	
	  // Convert from String to ImmutableBytesWritable 
	  .toBytesWritable( TABLE_SCHEMA )
	
	  .write(new HBaseSource( "table_name",
	    	"quorum_name:2181", 'key,  
	          TABLE_SCHEMA.tail.map((x: Symbol) => "data").toArray, 
	          TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)).toArray,
	          useSalt = true ))
	          
	
Setting the **_prefixList_** parameter to the available prefixes can increase the read performance quite a bit. 

4. Pipe Conversion Implicits
============================

HBaseSource will always read or write fields of type **_ImmutableBytesWritable_**. The supplied **_HBasePipeConversions_** trait is used to convert to and from **_String_** to **_ImmutableBytesWritable_**

Add the trait to the job class and start using the conversions in the pipe directly

(e.g.)

	class HBaseSaltTester (args: Args) extends JobBase(args) with HBasePipeConversions {
	  val TABLE_SCHEMA = List('key, 'salted, 'unsalted)

	  val hbase07 = 
      new HBaseSource( "table_name",
    	"quorum_name:2181", 'key,  
          TABLE_SCHEMA.tail.map((x: Symbol) => "data").toArray, 
          TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)).toArray,
          sourceMode = SourceMode.SCAN_RANGE, startKey = "11445", stopKey = "11455", 
          useSalt = true, prefixList = "0123456789" )
	  .read
	  
	  // Convert from ImmutableBytesWritable to String 
	  .fromBytesWritable( TABLE_SCHEMA )
	
	  .write(TextLine("saltTesting/ScanRangePlusSalt10"))
	
	  // Convert from String to ImmutableBytesWritable 
	  .toBytesWritable( TABLE_SCHEMA )
	
	  .write(new HBaseSource( "table_name",
	    	"quorum_name:2181", 'key,  
	          TABLE_SCHEMA.tail.map((x: Symbol) => "data").toArray, 
	          TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)).toArray,
	          useSalt = true ))
	} 

5. Raw HBase Tap and Source
===========================
HBaseRawSource is an alternative HBase source implementation that provides two main features:
1. Ability to provide a custom scan object.
2. Passing the row object to the mapper to allow full customized processing (without the need to declare in advance the read columns).

**Passing a scan object**
HBaseRawSource object provides a helper function to encode a scan object as a base64 string, which you can pass to the source.
e.g.
	
	val scan = new Scan
	val key = "my_key_prefix"
	scan.setStartRow(Bytes.toBytes(key))
	scan.setFilter(new PrefixFilter(Bytes.toBytes(key)))
	val scanner = HBaseRawSource.convertScanToString(scan)
	val hbaseSource = new HBaseRawSource("MY-TABLE", "hbase-local", Array("col-family"), base64Scan = scanner)

**Processing the rows**
The mapper function gets from HBaseRawSource a tuple containing two fields: (rowkey, row). 
The first field is the row key, the second is the row Result object. You can then process the row as needed. 
The sink will write the output fields as columns under the provided family and field name as the column name. 
You can also provide the field name as a full qualifier (family:column) to specify a different family than was declared in the source.
e.g.
	
	val hbaseOut = new HBaseRawSource("MY_RESULTS", "hbase-local", Array("out-family"), writeNulls=false, sinkMode = SinkMode.REPLACE)
	hbaseSource.read
		.mapTo(('rowkey, 'row) -> ('rowkey, "different_family:col1", 'col2)) {
			x: (ImmutableBytesWritable, Result) =>
				{
					val (rowkey, row) = x
					val col1Times2 = Bytes.toInt(row.getValue(Bytes.toBytes("col-family"), Bytes.toBytes("col1"))) * 2;
					val col2 = row.getValue(Bytes.toBytes("col-family"), Bytes.toBytes("col2"));
					(rowkey, col1Times2, col2)
				}
		}
		.write(hbaseOut)

