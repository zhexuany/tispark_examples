package com.pingcap.tispark.examples.sql;

import java.util.List;
import java.util.ArrayList;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.AnalysisException;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.TiContext;

public class JavaTiSparkSQLExample {
    public static void main(String[] args) throws AnalysisException {
        SparkSession spark = SparkSession
            .builder()
            .appName("Java TiSpark SQL basic example")
            .getOrCreate();

        // close spark session and recycle the resources.
        List<String> addrs = new ArrayList<>();
        // Suppose TiDB cluster pd's ip is 172.16.10.7 and port is 2379
        addrs.add("172.16.10.7:2379");
        TiContext ti = new TiContext(spark, addrs);
        // Suppose TiDB cluster has a database and its name is tpch
        ti.tidbMapDatabase("tpch100");
        Dataset<Row> sqlDF = spark.sql("SELECT * FROM lineitem limit 5");
        sqlDF.show();
        spark.stop();
    }
}

