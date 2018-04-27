import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;


public class Kmeans {

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("Kmeans");
        JavaSparkContext sc = new JavaSparkContext(conf);
        String filepath = System.getProperty("user.dir") + "/kmeandata";
        JavaRDD<String> data = sc.textFile(filepath);

        //load data
        JavaRDD<Vector> parsedData = data.map(
                (Function<String, Vector>) s -> {
                    String[] sarray = s.split(" ");
                    double[] values = new double[sarray.length];
                    for (int i = 0; i < sarray.length; i++) {
                        values[i] = Double.parseDouble(sarray[i]);
                    }
                    return Vectors.dense(values);
                }
        );

        //number of cluster passed through argument variables
        int numberOfCluster = Integer.parseInt(args[0]);
        int iterations = Integer.parseInt(args[1]);


        Map<String, Double> means = setUpInitialMeans(parsedData, numberOfCluster);

        while (iterations != 0) {
            //CREATING value to differnce Rdd
            JavaPairRDD<String, List<Double>> valueTodiffRdd = parsedData.mapToPair(vector -> {
                double first1 = vector.toArray()[0];
                List<Double> diffs = new ArrayList<>();
                for (Map.Entry<String, Double> entry : means.entrySet()) {
                    double diff = Math.abs(entry.getValue() - first1);
                    diffs.add(diff);
                }
                String value = evaluateCluster(diffs);
                return new Tuple2<>(value, Arrays.asList(first1));
            });

            //reducing the cluster based on the difference
            JavaPairRDD<String, List<Double>> reduceOnCluster = valueTodiffRdd.reduceByKey((doubles, doubles2) -> {
                List<Double> list = new ArrayList<>();
                list.addAll(doubles);
                list.addAll(doubles2);
                return list;
            });

            //calculating mean against each cluster
            JavaRDD<Tuple2<String, Double>> clusterToMean = reduceOnCluster.map(tuple -> {
                double mean = tuple._2.stream().mapToDouble(x -> x).average().getAsDouble();
                return new Tuple2<>(tuple._1, mean);
            });

            List<Tuple2<String, Double>> stagedMeans = clusterToMean.collect();
            assignMeans(means, stagedMeans);
            iterations--;
        }

        printOutput(means);

    }

    private static Map<String, Double> setUpInitialMeans(JavaRDD<Vector> parsedData, int numberOfCluster) {
        JavaRDD<Vector> dataTemp = parsedData;
        Map<String, Double> means = new LinkedHashMap<>();
        double first = dataTemp.first().toArray()[0];
        means.put("M1", first);
        for (int i = 2; i <= numberOfCluster; i++) {
            means.put("M" + i, first + 5.0);
        }
        return means;
    }

    private static void assignMeans(Map<String, Double> means, List<Tuple2<String, Double>> collect) {
        for (Tuple2<String, Double> tuple : collect) {
            int index = Integer.parseInt(tuple._1.substring(1));
            means.put("M" + index, tuple._2);
        }
    }

    private static String evaluateCluster(List<Double> diffs) {
        int minIndex = diffs.indexOf(Collections.min(diffs));
        minIndex = minIndex + 1;
        return "K" + minIndex;
    }

    private static void printOutput(Map<String,Double> result) throws IOException {
        String filename = "output/kmeansOutput";

        System.out.println("**********************************************************" +
                "********************For Output Check file :" + filename);
        File file = new File(filename);
        if (file.exists() && file.isFile()) {
            file.delete();
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        writer.write(result.toString());

        writer.close();
    }


}
