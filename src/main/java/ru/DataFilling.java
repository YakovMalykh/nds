package ru;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;

public class DataFilling {
    /**
     * метод создает таблицы и заполняет их исходными данными
     */
    public static void fillingDbInitialData(SparkSession spark) {
        spark.sql("CREATE DATABASE IF NOT EXISTS tmp_tos");

        spark.sql("CREATE TABLE IF NOT EXISTS tmp_tos.vsa_nd_nds_r1(" +
                "code_period string, code_present_place string, date_receipt date, " +
                "fid decimal(38,0), s40 decimal(38,0), year decimal(38,0), quarter decimal(38,0)) STORED AS ORC");

        spark.sql("CREATE TABLE IF NOT EXISTS tmp_tos.vsa_nd_nds_r3(" +
                "date_creation date, fid decimal(38,0), s109_5 decimal(38,0), " +
                "s120_3 decimal(38,0), s3_5 decimal(38,0), year decimal(38,0), quarter decimal(38,0)) STORED AS ORC");

        Dataset<Row> firstFile = spark.read()
                .option("header", "true")
                .option("sep", "\t")
                .csv("src/main/resources/tmp_tos_vsa_nd_nds_r1_2.tsv");

        Dataset<Row> secondFile = spark.read()
                .option("header", "true")
                .option("sep", "\t")
                .csv("src/main/resources/tmp_tos_vsa_nd_nds_r3_2.tsv");

        firstFile.write().mode(SaveMode.Overwrite).orc("spark-warehouse/tmp_tos.db/vsa_nd_nds_r1");

        secondFile.write().mode(SaveMode.Overwrite).orc("spark-warehouse/tmp_tos.db/vsa_nd_nds_r3");
    }
}
