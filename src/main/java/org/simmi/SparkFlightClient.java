package org.simmi;

import org.apache.arrow.flight.*;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.VarCharVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.ArrowType;
import org.apache.arrow.vector.types.pojo.Field;
import org.apache.arrow.vector.types.pojo.FieldType;
import org.apache.arrow.vector.types.pojo.Schema;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;

public class SparkFlightClient {
    final static Location location = Location.forGrpcInsecure("0.0.0.0", 33333);

    final static String pythonScript = """
            import pyspark
            from pyspark.sql import SparkSession
            print("hello")
            spark = SparkSession.builder \\
                                .appName('demo_pca') \\
                                .getOrCreate()
            print("bello")
                                
            letters = [{'letter': 'a'}, {'letter': 'b'}, {'letter': 'c'}]
            df = spark.createDataFrame(letters)
            df.createOrReplaceTempView('letters')
            print("cello")
            
            df.count()
            print("done")
            """;

    final static String RScript = """
            library(SparkR, lib.loc = c(file.path(Sys.getenv("SPARK_HOME"), "R", "lib")))
            sparkR.session()
            df <- sql("select * from letters")
            c <- nrow(df)
            print(c)
            """;
    final static String sqlScript = """
            select * from letters
            """;

    public static void main(String[] args) {
        try (BufferAllocator allocator = new RootAllocator()) {
            try (FlightClient flightClient = FlightClient.builder(allocator, location).build()) {
                System.out.println("C1: Client (Location): Connected to " + location.getUri());
                Schema schema = new Schema(Arrays.asList(
                        new Field("type", FieldType.nullable(new ArrowType.Utf8()), null),
                        new Field("query", FieldType.nullable(new ArrowType.Utf8()), null),
                        new Field("config", FieldType.nullable(new ArrowType.Utf8()), null)));
                try(VectorSchemaRoot vectorSchemaRoot = VectorSchemaRoot.create(schema, allocator);
                    VectorSchemaRoot vectorSchemaRoot2 = VectorSchemaRoot.create(schema, allocator);
                    VarCharVector varCharVectorType = (VarCharVector) vectorSchemaRoot.getVector("type");
                    VarCharVector varCharVectorQuery = (VarCharVector) vectorSchemaRoot.getVector("query");
                    VarCharVector varCharVectorConfig = (VarCharVector) vectorSchemaRoot.getVector("config");
                    VarCharVector varCharVectorType2 = (VarCharVector) vectorSchemaRoot2.getVector("type");
                    VarCharVector varCharVectorQuery2 = (VarCharVector) vectorSchemaRoot2.getVector("query");
                    VarCharVector varCharVectorConfig2 = (VarCharVector) vectorSchemaRoot2.getVector("config")) {
                    FlightClient.ClientStreamListener listener = flightClient.startPut(
                            FlightDescriptor.path("profiles"),
                            vectorSchemaRoot, new AsyncPutListener());

                    FlightClient.ClientStreamListener listener2 = flightClient.startPut(
                            FlightDescriptor.path("testspark"),
                            vectorSchemaRoot2, new AsyncPutListener());

                    varCharVectorType.allocateNew(1);
                    varCharVectorQuery.allocateNew(1);
                    varCharVectorConfig.allocateNew(1);

                    varCharVectorType2.allocateNew(1);
                    varCharVectorQuery2.allocateNew(1);
                    varCharVectorConfig2.allocateNew(1);

                    varCharVectorType2.set(0, "python".getBytes());
                    varCharVectorQuery2.set(0, pythonScript.getBytes());
                    varCharVectorConfig2.set(0, "config".getBytes());
                    vectorSchemaRoot2.setRowCount(1);
                    //listener2.putNext();
                    //listener2.completed();
                    //listener2.getResult();

                    varCharVectorType.set(0, "python".getBytes());
                    varCharVectorQuery.set(0, pythonScript.getBytes());
                    varCharVectorConfig.set(0, "config".getBytes());
                    vectorSchemaRoot.setRowCount(1);
                    listener.putNext();
                    varCharVectorType.set(0, "R".getBytes());
                    varCharVectorQuery.set(0, RScript.getBytes());
                    varCharVectorConfig.set(0, "config".getBytes());
                    vectorSchemaRoot.setRowCount(1);
                    //listener.putNext();

                    varCharVectorType.set(0, "sql".getBytes());
                    varCharVectorQuery.set(0, sqlScript.getBytes());
                    varCharVectorConfig.set(0, "config".getBytes());

                    vectorSchemaRoot.setRowCount(1);
                    //listener.putNext();

                    /*try(FlightStream flightStream = flightClient.getStream(new Ticket(
                            FlightDescriptor.path("letters").getPath().get(0).getBytes(StandardCharsets.UTF_8)))) {
                        int batch = 0;
                        try (VectorSchemaRoot vectorSchemaRootReceived = flightStream.getRoot()) {
                            System.out.println("C4: Client (Get Stream):");
                            while (flightStream.next()) {
                                batch++;
                                System.out.println("Client Received batch #" + batch + ", Data:");
                                System.out.print(vectorSchemaRootReceived.contentToTSVString());
                            }
                        }
                    } catch (Exception e) {
                        e.printStackTrace();
                    }*/

                    listener.completed();
                    listener.getResult();
                    System.out.println("C2: Client (Populate Data): Wrote 2 batches with 3 rows each");
                }

                // Get metadata information
                /*FlightInfo flightInfo = flightClient.getInfo(FlightDescriptor.path("profiles"));
                System.out.println("C3: Client (Get Metadata): " + flightInfo);

                //flightClient.
                // Get data information
                try(FlightStream flightStream = flightClient.getStream(new Ticket(
                        FlightDescriptor.path("profiles").getPath().get(0).getBytes(StandardCharsets.UTF_8)))) {
                    int batch = 0;
                    try (VectorSchemaRoot vectorSchemaRootReceived = flightStream.getRoot()) {
                        System.out.println("C4: Client (Get Stream):");
                        while (flightStream.next()) {
                            batch++;
                            System.out.println("Client Received batch #" + batch + ", Data:");
                            System.out.print(vectorSchemaRootReceived.contentToTSVString());
                        }
                    }
                } catch (Exception e) {
                    e.printStackTrace();
                }

                // Get all metadata information
                Iterable<FlightInfo> flightInfosBefore = flightClient.listFlights(Criteria.ALL);
                System.out.print("C5: Client (List Flights Info): ");
                flightInfosBefore.forEach(System.out::println);

                // Do delete action
                Iterator<Result> deleteActionResult = flightClient.doAction(new Action("DELETE",
                                                                                       FlightDescriptor.path("profiles").getPath().get(0).getBytes(StandardCharsets.UTF_8)));
                while (deleteActionResult.hasNext()) {
                    Result result = deleteActionResult.next();
                    System.out.println("C6: Client (Do Delete Action): " +
                                       new String(result.getBody(), StandardCharsets.UTF_8));
                }

                // Get all metadata information (to validate detele action)
                Iterable<FlightInfo> flightInfos = flightClient.listFlights(Criteria.ALL);
                flightInfos.forEach(t -> System.out.println(t));
                System.out.println("C7: Client (List Flights Info): After delete - No records");*/
            }
            catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }
}