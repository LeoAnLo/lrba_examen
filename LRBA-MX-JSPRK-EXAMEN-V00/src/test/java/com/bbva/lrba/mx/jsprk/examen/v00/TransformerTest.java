package com.bbva.lrba.mx.jsprk.examen.v00;

import com.bbva.lrba.spark.test.LRBASparkTest;
import com.bbva.lrba.spark.wrapper.DatasetUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class TransformerTest extends LRBASparkTest {

    private Transformer transformer;

    @BeforeEach
    void setUp() {
        this.transformer = new Transformer();
    }

    @Test
    void transform_Output() {
        StructType schema = DataTypes.createStructType(
               new StructField[]{
                         DataTypes.createStructField("cast", DataTypes.IntegerType, false),
                         DataTypes.createStructField("crew", DataTypes.StringType, false),
                         DataTypes.createStructField("credit_id", DataTypes.StringType, false),
                         DataTypes.createStructField("gender", DataTypes.IntegerType, false),
                         DataTypes.createStructField("id", DataTypes.IntegerType, false),
                         DataTypes.createStructField("name", DataTypes.StringType, false),
                         DataTypes.createStructField("order", DataTypes.IntegerType, false),
                         DataTypes.createStructField("profile_path", DataTypes.StringType, false),

               });
        Row firstRow = RowFactory.create(14, "Woody (voice)", "52fe4284c3a36847f8024f95",2,31,"Tom Hanks",0,"/pQFoyx7rp09CJTAb932F2g8Nlho.jpg");
        Row secondRow = RowFactory.create(15, "Buzz Lightyear (voice)", "52fe4284c3a36847f8024f99",2,12898,"Tim Allen",1,"/uX2xVf6pMmPepxnvFWyBtjexzgY.jpg");

        final List<Row> listRows = Arrays.asList(firstRow, secondRow);

        DatasetUtils<Row> datasetUtils = new DatasetUtils<>();
        Dataset<Row> credits = datasetUtils.createDataFrame(listRows, schema);

        final Map<String, Dataset<Row>> datasetMap = this.transformer.transform(new HashMap<>(Map.of("credits", credits)));

        assertNotNull(datasetMap);
        assertEquals(0, datasetMap.size());
}
    /*@Test
    void transform_Output_1() {
        StructType schema = DataTypes.createStructType(
                new StructField[]{
                        DataTypes.createStructField("id", DataTypes.IntegerType, false),
                        DataTypes.createStructField("name", DataTypes.StringType, false),
                });
        Row firstRow = RowFactory.create(931, "jealousy");
        Row secondRow = RowFactory.create(4290, "toy");

        final List<Row> listRows = Arrays.asList(firstRow, secondRow);

        DatasetUtils<Row> datasetUtils = new DatasetUtils<>();
        Dataset<Row> keywords = datasetUtils.createDataFrame(listRows, schema);

        final Map<String, Dataset<Row>> datasetMap = this.transformer.transform(new HashMap<>(Map.of("keywords", keywords)));

        assertNotNull(datasetMap);
        assertEquals(1, datasetMap.size());
    }*/
}