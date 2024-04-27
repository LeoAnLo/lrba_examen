package com.bbva.lrba.mx.jsprk.examen.v00;

import com.bbva.lrba.builder.annotation.Builder;
import com.bbva.lrba.builder.spark.RegisterSparkBuilder;
import com.bbva.lrba.builder.spark.domain.SourcesList;
import com.bbva.lrba.builder.spark.domain.TargetsList;
import com.bbva.lrba.spark.domain.datasource.Source;
import com.bbva.lrba.spark.domain.datatarget.Target;
import com.bbva.lrba.spark.domain.transform.TransformConfig;

@Builder
public class JobExamenBuilder extends RegisterSparkBuilder {

    @Override
    public SourcesList registerSources() {
        //EXAMPLE WITH A LOCAL SOURCE FILE
        return SourcesList.builder()
                .add(Source.File.Csv.builder()
                        .alias("credits")
                        .physicalName("credits.csv")
                        .serviceName("local.logicalDataStore.batch")
                        .sql("SELECT * FROM credits")
                        .header(true)
                        .delimiter(",")
                        .build())
                .add(Source.File.Csv.builder()
                        .alias("keywords")
                        .physicalName("keywords.csv")
                        .serviceName("local.logicalDataStore.batch")
                        .sql("SELECT * FROM keywords")
                        .header(true)
                        .delimiter(",")
                        .build())
                .add(Source.File.Csv.builder()
                        .alias("movies_metadata")
                        .physicalName("movies_metadata.csv")
                        .serviceName("local.logicalDataStore.batch")
                        .sql("SELECT * FROM movies_metadata")
                        .header(true)
                        .delimiter(",")
                        .build())
                .add(Source.File.Csv.builder()
                        .alias("ratings")
                        .physicalName("ratings.csv")
                        .serviceName("local.logicalDataStore.batch")
                        .sql("SELECT * FROM ratings")
                        .header(true)
                        .delimiter(",")
                        .build())
                .add(Source.File.Csv.builder()
                        .alias("char")
                        .physicalName("characteristics.csv")
                        .serviceName("local.logicalDataStore.batch")
                        .sql("SELECT * FROM ratings")
                        .header(true)
                        .delimiter(",")
                        .build())
                .add(Source.File.Csv.builder()
                        .alias("movies")
                        .physicalName("movies.csv")
                        .serviceName("local.logicalDataStore.batch")
                        .sql("SELECT * FROM ratings")
                        .header(true)
                        .delimiter(",")
                        .build())
                .add(Source.File.Csv.builder()
                        .alias("streaming")
                        .physicalName("streaming.csv")
                        .serviceName("local.logicalDataStore.batch")
                        .sql("SELECT * FROM ratings")
                        .header(true)
                        .delimiter(",")
                        .build())
                .build();
    }

    @Override
    public TransformConfig registerTransform() {
        //IF YOU WANT TRANSFORM CLASS
        return TransformConfig.TransformClass.builder().transform(new Transformer()).build();
        //IF YOU WANT SQL TRANSFORM
        //return TransformConfig.SqlStatements.builder().addSql("targetAlias1", "SELECT CAMPO1 FROM sourceAlias1").build();
        //IF YOU DO NOT WANT TRANSFORM
        //return null;
    }

    @Override
    public TargetsList registerTargets() {
        //EXAMPLE WITH A LOCAL TARGET FILE
        return TargetsList.builder()
                .add(Target.File.Csv.builder()
                        .alias("salida1")
                        .physicalName("output/output.csv")
                        .serviceName("local.logicalDataStore.batch")
                        .header(true)
                        .delimiter(",")
                        .build())
                .build();
    }

}