# HadoopDxramFS

This DXRAM connector lets you run [Apache Hadoop](http://hadoop.apache.org)
or [HBASE](https://hbase.apache.org/) jobs directly on data in [DXRAM](https://dxram.io/)
instead of HDFS.

**It is still in pre Alpha state!**

## Links

-   [Hadoop DXRAM Code on github](https://github.com/no-go/HadoopDxramFS)
-   [Hadoop DXRAM Website](https://no-go.github.io/HadoopDxramFS/)
-   [DX:RAM](https://dxram.io/)

## Notes (for me!)

use hadoop fs CLI to access `dxram://namenode:9000` from `core-site.xml`

alpha works on /tmp/ folder and not in dxram!!!

ok:

    bin/hadoop fs -mkdir /user
    bin/hadoop fs -mkdir /user/tux
    bin/hadoop fs -ls /user
    cp example /tmp/user/tux/
    bin/hadoop fs -rm -f /user/tux
    bin/hadoop fs -rm -f /user/tux
    -> /user/tux not exists!

not working:

    bin/hadoop fs -ls /
    bin/hadoop fs -mkdir /user/tux/a/b/c/d
    bin/hadoop fs -put example.txt /user/ex.txt
    bin/hadoop fs -cat /user/example.txt
    bin/hadoop fs -cp /user/example.txt /user/tux/ex.txt

reading data or writing into files is still a big problem.

File `hadoop-2.8.2-src/hadoop-dist/target/hadoop-2.8.2/etc/hadoop/core-site.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<?xml-stylesheet type="text/xsl" href="configuration.xsl"?>
<configuration>
    <property>
        <name>fs.dxram.impl</name>
        <value>de.hhu.bsinfo.hadoop.fs.dxram.DxramFileSystem</value>
        <description>The FileSystem for dxram.</description>
    </property>
    <property>
        <name>fs.AbstractFileSystem.dxram.impl</name>
        <value>de.hhu.bsinfo.hadoop.fs.dxram.DxramFs</value>
        <description>
            The AbstractFileSystem for dxram
        </description>
    </property>
    <property>
        <name>fs.defaultFS</name>
        <value>dxram://namenode:9000</value>
    </property>
</configuration>
```

It is still a testing scenario.

### hadoop example

    cd /EXAMPLE/hadoop-2.8.2-src/hadoop-dist/target/hadoop-2.8.2/
    mkdir input
    cp etc/hadoop/*.xml input
    bin/hadoop jar share/hadoop/mapreduce/hadoop-mapreduce-examples-2.8.2.jar grep input output 'dfs[a-z.]+'

### Hbase example

You need

-   kerberos (does `kinit` work?)
-   hadoop "binaries" (maybe part of hbase)
-   zookeeper (part of hbase)

[Code example](https://stackoverflow.com/questions/13728857/how-to-run-hbase-java-example)

### build hadoop

You need an old protobuf version

    git clone https://github.com/google/protobuf.git
    cd protobuf
    git checkout tags/v2.5.0
    unsure: ./autogen.sh
    ./configure
    make
    sudo make install
    sudo ldconfig
    reboot ?!

Get Hadoop:

    gunzip hadoop-*
    tar -xvf hadoop-*
    cd hadoop-2.8.2-src/
    mvn package -Pdist -Pdoc -Psrc -Dtar -DskipTests
    or use " mvn package -Pdist -Pdoc -Psrc -Dtar -DskipTests -o " for offline

### .bashrc

```bash
    export JAVA_HOME=/usr
    export HADOOP_CONF_DIR="/EXAMPLE/hadoop-2.8.2-src/hadoop-dist/target/hadoop-2.8.2/etc/hadoop/"
    export HADOOP_HOME="/EXAMPLE/hadoop-2.8.2-src/hadoop-dist/target/hadoop-2.8.2/"
    export HBASE_CONF_DIR="/etc/hbase/"
    export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin/:$HADOOP_HOME/sbin/:$PATH
```

### Intellij project specials

I add jar direcories `/EXAMPLE/hadoop-2.8.2-src/hadoop-dist/target/hadoop-2.8.2/share/hadoop/common/`
and `/EXAMPLE/hadoop-2.8.2-src/hadoop-common-project/hadoop-annotations/target/` to the project.

Build a jar artefact. Copy it to:
`/EXAMPLE/hadoop-2.8.2-src/hadoop-dist/target/hadoop-2.8.2/share/hadoop/common/lib/hadoopDxramfs.jar`


### hdfs and hbase

start

    hdfs namenode -format
    start-dfs.sh
    hdfs dfs -mkdir /user
    hdfs dfs -mkdir /user/tux
    start-hbase.sh
    kinit
    klist

stop

    stop-hbase.sh
    stop-dfs.sh
