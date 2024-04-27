package com.bbva.lrba.mx.jsprk.examen.v00;

import com.bbva.lrba.spark.transformers.Transform;
import org.apache.hadoop.shaded.org.eclipse.jetty.websocket.common.frames.DataFrame;
import org.apache.spark.sql.*;
import org.apache.spark.sql.functions.*;
import org.apache.spark.sql.Dataset;

import java.util.HashMap;
import java.util.Map;
import org.apache.spark.sql.types.*;
import scala.collection.immutable.Nil;

import static org.apache.spark.sql.functions.*;

public class Transformer implements Transform {

    @Override
    public Map<String, Dataset<Row>> transform(Map<String, Dataset<Row>> datasetsFromRead) {
        Map<String, Dataset<Row>> datasetsToWrite = new HashMap<>();

        SparkSession spark = SparkSession.builder().appName("examen").master("local[*]").getOrCreate();

        //inputs

        Dataset<Row> charac = spark.read().option("header","True").option("delimiter",",").csv("local-execution/files/characteristics.csv");
        Dataset<Row> movies = spark.read().option("header","True").option("delimiter",",").csv("local-execution/files/movies.csv");
        Dataset<Row> streaming = spark.read().option("header","True").option("delimiter",",").csv("local-execution/files/streaming.csv");

        //ejericio 1
        Dataset<Row> char_mv = charac.alias("I").join(movies.alias("J"), charac.col("title").equalTo(movies.col("title")), "outer")
                .selectExpr("I.*","J.director","J.cast","J.description","J.code","J.type");

        Dataset<Row> df_final = char_mv.alias("K").join(streaming.alias("L"), char_mv.col("code").equalTo(streaming.col("code")), "outer")
                .selectExpr("K.*","L.service");

        //df_final.write().csv("C:/Users/XMF5539/Documents/LRBA/examen_lrba/union.csv");

        //ejericio 2
        Dataset<Row> conteo_streaming = df_final.groupBy("service").count().orderBy(col("count").desc());
        conteo_streaming.show();

        //conteo_streaming.write().csv("C:/Users/XMF5539/Documents/LRBA/examen_lrba/streaming.csv");

        //ejercicio 3
        Dataset<Row> conteo_director = df_final
                .groupBy("director", "service").count().orderBy(functions.col("director").asc())
                .select("director", "service", "count");
        conteo_director.show();

        //conteo_director.write().csv("C:/Users/XMF5539/Documents/LRBA/examen_lrba/director.csv");

        //ejercicio 4
        Dataset<Row> peliculas = df_final.filter(functions.col("type").equalTo("Movie"));
        Dataset<Row> series = df_final.filter(functions.col("type").equalTo("TV Show"));

        Dataset<Row> top_peliculas = peliculas
                .withColumn("duration", functions.regexp_extract(functions.col("duration"), "\\d+", 0).cast("integer"))
                .filter(functions.col("duration").isNotNull())
                .orderBy(functions.col("duration").desc())
                .select("title", "duration").distinct()
                .limit(10);

        top_peliculas.show();

        //top_peliculas.write().csv("C:/Users/XMF5539/Documents/LRBA/examen_lrba/top_peliculas.csv");

        Dataset<Row> top_series = series
                .withColumn("duration", functions.regexp_extract(functions.col("duration"), "\\d+", 0).cast("integer"))
                .filter(functions.col("duration").isNotNull())
                .orderBy(functions.col("duration").desc())
                .select("title", "duration").distinct()
                .limit(10);

        top_series.show();

        //top_series.write().csv("C:/Users/XMF5539/Documents/LRBA/examen_lrba/top_series.csv");

        //ejercicio 5
        Dataset<Row> dfModificado = df_final.withColumn("director",functions.when(functions.col("type").equalTo("Movie")
                                .and(functions.col("director").isNull()), "George Lucas").when(functions.col("type").equalTo("TV Show")
                                .and(functions.col("director").isNull()), "Matt Groening")
                                .otherwise(functions.col("director")));

        double porcentajeDisney = 0.4; // 40% para Disney+
        double porcentajeNetflix = 0.6; // 60% para Netflix

        Dataset<Row> dfAsignado = df_final.withColumn("service",
                functions.when(functions.rand().lt(porcentajeDisney), "Disney+")
                        .otherwise("Netflix"));

        dfAsignado.groupBy("service").count().show();

        //dfAsignado.write().csv("C:/Users/XMF5539/Documents/LRBA/examen_lrba/df_modificado.csv");





        return datasetsToWrite;

    }

}