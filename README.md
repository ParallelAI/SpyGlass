SpyGlass
========

Cascading and Scalding wrapper for HBase with advanced read and write features. JDBC wrapper is recently added to support SQL in taps. Insert/Update/Upserts (Update on Duplicates) are supported with MultiRowInserts & Batch Modes.

Prevent Hot Spotting by the use of transparent key prefixes.

Changes
=======
- Compatible with Scalding 0.12.0 and later
- Added JDBC Tap Functionality
- Added Delete Functionality
- Added Region grouping of splits
- Migrated maven repo to local subdirectory. See below.

Building
========

	$ mvn clean install -U
	$ mvn deploy

	Requires Maven 3.x.x

To use SpyGlass as a dependency use the following repository

```xml
<repositories>
  <repository>
    <id>conjars.org</id>
    <url>http://conjars.org/repo</url>
  </repository>
</repositories>
```

For Scalding 0.12.0 on CDH 5.3 use :

```xml
<dependencies>
  <dependency>
    <groupId>parallelai</groupId>
    <artifactId>parallelai.spyglass</artifactId>
    <version>2.10_0.12_0_5.3</version>
  </dependency>
</dependencies>
```

or use any earlier versions :

```xml
<dependencies>
  <dependency>
    <groupId>parallelai</groupId>
    <artifactId>parallelai.spyglass</artifactId>
    <version>2.9.3_4.1.0</version>
  </dependency>
</dependencies>
```



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

```scala
val hbs2 = new HBaseSource(
  "table_name",
  "quorum_name:2181",
  'key,
  Array("column_family"),
  Array('column_name),
  sourceMode = SourceMode.GET_LIST, keyList = List("5003914", "5000687", "5004897"))
```

Additionally, the **_versions_** parameter can be used to retrieve more than one version of the row.

(e.g.)

```scala
val hbs2 = new HBaseSource(
  "table_name",
  "quorum_name:2181",
  'key,
  Array("column_family"),
  Array('column_name),
  sourceMode = SourceMode.GET_LIST,
  versions = 5,
  keyList = List("5003914", "5000687", "5004897"))
```

SCAN_RANGE
----------
Scan range uses the optional **_startKey_** and **_stopKey_** parameters to specify the range of keys to extract; both keys are inclusive.

if:
 - Only **_startKey_** provided -> All rows from **_startKey_** till **END OF TABLE** are returned
 - Only **_stopKey_** provided -> All rows from **START OF TABLE** till **_stopKey_** are returned
 - Neither provided -> All rows in table are returned.

(e.g.)

```scala
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
```

SCAN_ALL
--------
Returns all rows in the table

(e.g.)

```scala
val hbs2 = new HBaseSource(
  "table_name",
  "quorum_name:2181",
  'key,
  Array("column_family"),
  Array('column_name),
  sourceMode = SourceMode.SCAN_ALL)
```

2. Write Mode Features
======================

HBaseSource supports writing at a particular time stamp i.e. a version.

The time dimension can be added to the row by using the **_timestamp_** parameter. If the parameter is not present the current time is used.

(e.g.)

```scala
pipe.write(new HBaseSource( "table_name",
  "quorum_name:2181",
  'key,
  TABLE_SCHEMA.tail.map((x: Symbol) => "data").toArray,
  TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)).toArray,
  timestamp = Platform.currentTime ))
```


3. Region Hot Spot Prevention
=============================

Region hot spotting is a common problem with HBase. Spy Glass uses key prefix salting to avoid this.
The row key is prefixed with the last byte followed by a '_' (underscore) character.

(e.g.)

```
Original Row Key   ->  Becomes
SPYGLASS           ->  S_SPYGLASS
12345678           ->  8_12345678
```

Conversion to and from salted keys is done automatically.

Setting the **_useSalt_** parameter to **true** enables this functionality


(e.g.)

```scala
val TABLE_SCHEMA = List('key, 'salted, 'unsalted)

val hbase07 = new HBaseSource( "table_name",
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
```


Setting the **_prefixList_** parameter to the available prefixes can increase the read performance quite a bit.

4. Pipe Conversion Implicits
============================

HBaseSource will always read or write fields of type **_ImmutableBytesWritable_**. The supplied **_HBasePipeConversions_** trait is used to convert to and from **_String_** to **_ImmutableBytesWritable_**

Add the trait to the job class and start using the conversions in the pipe directly

(e.g.)

```scala
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
```


5. Raw HBase Tap and Source
===========================
HBaseRawSource is an alternative HBase source implementation that provides two main features:
* Ability to provide a custom scan object.
* Passing the row object to the mapper to allow full customized processing (without the need to declare in advance the read columns).

**Passing a scan object**

`HBaseRawSource` object provides a helper function to encode a scan object as a base64 string, which you can pass to the source.
e.g.

```scala
val scan = new Scan
val key = "my_key_prefix"
scan.setStartRow(Bytes.toBytes(key))
scan.setFilter(new PrefixFilter(Bytes.toBytes(key)))
val scanner = HBaseRawSource.convertScanToString(scan)
val hbaseSource = new HBaseRawSource("MY-TABLE", "hbase-local", Array("col-family"), base64Scan = scanner)
```

**Processing the rows**

The mapper function gets from HBaseRawSource a tuple containing two fields: (rowkey, row).
The first field is the row key, the second is the row Result object. You can then process the row as needed.
The sink expects a rowkey field in the tuple it gets to use as a row key (it doesn't have to be the same as the one emitted by the source).
It will then write the output fields (except the rowkey) as columns under the provided family, using the field name as the column name.
You can also provide the field name as a full qualifier (family:column) to specify a different family than was declared in the source.
e.g.

```scala
val hbaseOut = new HBaseRawSource("MY_RESULTS", "hbase-local", Array("out-family"), writeNulls=false, sinkMode = SinkMode.REPLACE)
hbaseSource.read
.mapTo(('rowkey, 'row) -> ('rowkey, "different_family:col1", 'col2)) {
  x: (ImmutableBytesWritable, Result) => {
    val (rowkey, row) = x
    val col1Times2 = Bytes.toInt(row.getValue(Bytes.toBytes("col-family"),
                                 Bytes.toBytes("col1"))) * 2;
    val col2 = row.getValue(Bytes.toBytes("col-family"), Bytes.toBytes("col2"));
    (rowkey, col1Times2, col2)
  }
}
.write(hbaseOut)
```

6. Jdbc Tap and Source
===========================

To be added soon.

e.g.

```scala
val jdbcSourceRead = new JDBCSource(
  "TABLE_01",
  "com.mysql.jdbc.Driver",
  "jdbc:mysql://localhost:3306/db?zeroDateTimeBehavior=convertToNull",
  "root",
  "password",
  List("ID", "TEST_COLUMN1", "TEST_COLUMN2", "TEST_COLUMN3"),
  List("bigint(20)", "varchar(45)", "varchar(45)", "bigint(20)"),
  List("id"),
  new Fields("key", "column1", "column2", "column3"),
  null, null, null)

val jdbcSourceWrite = new JDBCSource(
  "TABLE_01",
  "com.mysql.jdbc.Driver",
  "jdbc:mysql://localhost:3306/db?zeroDateTimeBehavior=convertToNull",
  "root",
  "password",
  List("ID", "TEST_COLUMN1", "TEST_COLUMN2", "TEST_COLUMN3"),
  List("bigint(20)", "varchar(45)", "varchar(45)", "bigint(20)"),
  List("id"),
  new Fields("key", "column1", "column2", "column3"),
  null, null, null)
```

7. HBase Delete Functionality
=============================

Delete functionality has been added to SpyGlass version 4.1.0 onwards. Below is an example of how to use it.

The feature can be enable by using the parameter sinkMode = SinkMode.REPLACE in the HBaseSource
You can use sinkMode = SinkMode.UPDATE to add to the HBaseTable as usual.

e.g.

```scala
val eraser = toIBW(input, TABLE_SCHEMA)
  .write(new HBaseSource( "_TEST.SALT.01", quorum, 'key,
  TABLE_SCHEMA.tail.map((x: Symbol) => "data"),
  TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)), sinkMode = SinkMode.REPLACE ))
```

All rows with the key will be deleted. This includes all versions too.

8. Regional Split Group in Scan and Get
=======================================

This functionality reduces the number of mappers significantly when using hot spot prevention with prefixes.

This feature can be activated by using inputSplitType = SplitType.REGIONAL
You can use inputSplitType = SplitType.GRANULAR to use the previous functionality as is.

e.g.

```scala
val hbase04 = new HBaseSource( "_TEST.SALT.01", quorum, 'key,
  TABLE_SCHEMA.tail.map((x: Symbol) => "data"),
  TABLE_SCHEMA.tail.map((x: Symbol) => new Fields(x.name)),
  sourceMode = SourceMode.SCAN_RANGE, startKey = sttKeyP, stopKey = stpKeyP,
  inputSplitType = splitType).read
  .fromBytesWritable(TABLE_SCHEMA )
  .map(('key, 'salted, 'unsalted) -> 'testData) {x: (String, String, String) => List(x._1, x._2, x._3)}
  .project('testData)
  .write(TextLine("saltTesting/ScanRangeNoSalt01"))
  .groupAll(group => group.toList[List[List[String]]]('testData -> 'testData))
```

9. Complete example
===================

If using Maven - create a pom.xml

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
  <modelVersion>4.0.0</modelVersion>

  <groupId>mygroup</groupId>
  <artifactId>myartifact</artifactId>
  <version>1.0-SNAPSHOT</version>
  <name>myname</name>

  <repositories>
    <repository>
      <id>conjars.org</id>
      <url>http://conjars.org/repo</url>
    </repository>
  </repositories>

  <dependencies>
    <dependency>
      <groupId>parallelai</groupId>
      <artifactId>parallelai.spyglass</artifactId>
      <!-- Scala: 2.10 | Scalding: 0.10.0 | SpyGlass: 4.3 -->
      <version>2.10_0.10_4.3</version>
    </dependency>
  </dependencies>

  <build>
    <plugins>
      <plugin>
        <groupId>org.scala-tools</groupId>
        <artifactId>maven-scala-plugin</artifactId>
        <version>2.15.2</version>
        <executions>
          <execution>
            <id>scala-compile</id>
            <goals>
              <goal>compile</goal>
              <goal>testCompile</goal>
            </goals>
            <configuration>
              <args>
                <arg>-make:transitive</arg>
                <arg>-dependencyfile</arg>
                <arg>${project.build.directory}/.scala_dependencies</arg>
              </args>
            </configuration>
          </execution>
        </executions>
      </plugin>
    </plugins>
  </build>

</project>
```

A single dependency brings in Scalding 0.10.0 (in this example)

```scala
import com.twitter.scalding.{TextLine, Job, Args}
import parallelai.spyglass.hbase.{HBaseSource, HBasePipeConversions}
import cascading.tuple.Fields
import parallelai.spyglass.hbase.HBaseConstants.SourceMode

/**
 * Before executing this example, have a working HBase
 * (see https://github.com/kijiproject/kiji-bento for a standalone zero-configuration HBase micro-cluster)
 * , and
 *
 * $ hbase shell
 *
 * hbase(main):003:0> create 'spyglass.hbase.test1' , 'data'
 * hbase(main):006:0> put 'spyglass.hbase.test1' , 'row1' , 'data:column1' , 'value1'
 * hbase(main):007:0> put 'spyglass.hbase.test1' , 'row2' , 'data:column1' , 'value2'
 * hbase(main):008:0> put 'spyglass.hbase.test1' , 'row3' , 'data:column1' , 'value3'
 * hbase(main):009:0> scan 'spyglass.hbase.test1'
 *
 */
class HBaseTest(args: Args) extends Job(args) with HBasePipeConversions {

  val SCHEMA = List('key, 'column1)
  val tableName = "spyglass.hbase.test1"
  val hbaseHost = "localhost:2181"

  val data = new HBaseSource(
    tableName,
    hbaseHost,
    SCHEMA.head,
    SCHEMA.tail.map((x: Symbol) => "data"),
    SCHEMA.tail.map((x: Symbol) => new Fields(x.name)),
    sourceMode = SourceMode.SCAN_ALL)
    .read
    .fromBytesWritable(SCHEMA)
    .debug
    .write(TextLine("test_hbase"))
}
```
