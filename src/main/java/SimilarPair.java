import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SimilarPair {

    public static void main(String[] args) throws IOException {
        SparkConf conf = new SparkConf().setMaster("local").setAppName("SimilarPair");
        JavaSparkContext sc = new JavaSparkContext(conf);

        // Load our input data.
        String filepath = System.getProperty("user.dir") + "/similarPairs";

        JavaRDD<String> input = sc.textFile(filepath);

        //creating pairs with key as Document Name and Values as List of words
        JavaPairRDD<String, List<String>> docToWord = input.mapToPair(s -> new Tuple2<>(s.split(" ")[0], Arrays.asList(s.substring(2).split("\\|"))));

        // creating reverse index: word to document
        JavaPairRDD<String, Iterable<String>> invertedWordToDoc = docToWord.flatMap(stringListTuple2 -> {
            List<Tuple2<String, String>> list = new ArrayList<>();
            String key = stringListTuple2._1;
            for (String s : stringListTuple2._2) {
                String[] split = s.split("\\-");
                Tuple2<String, String> stringStringTuple2 = new Tuple2<>(split[0], key + "-" + split[1]);
                list.add(stringStringTuple2);
            }
            return list.iterator();
        }).mapToPair(x -> new Tuple2<>(x._1, x._2)).groupByKey();

        //finding product against each word
        JavaPairRDD<String, Integer> productRdd = invertedWordToDoc.flatMap(tuple -> {
            List<Tuple2<String, Integer>> list = new ArrayList<>();
            List<String> iterable = new ArrayList<>();
            Iterable<String> tuple2s = tuple._2;
            tuple2s.forEach(x -> iterable.add(x));
            for (int i = 0; i < iterable.size() - 1; i++) {
                for (int j = i + 1; j < iterable.size(); j++) {
                    String[] first = iterable.get(i).split("\\-");
                    String[] second = iterable.get(j).split("\\-");
                    list.add(new Tuple2<>(first[0] + "." + second[0], Math.multiplyExact(Integer.parseInt(first[1]), Integer.parseInt(second[1]))));
                }
            }
            return list.iterator();
        }).mapToPair(y -> new Tuple2<>(y._1, y._2));

        //reduce the document to document product
        JavaPairRDD<String, Integer> result = productRdd.reduceByKey((x, y) -> x + y);

        printOutput(result);

    }

    private static void printOutput(JavaPairRDD<String, Integer> result) throws IOException {
        String filename = "output/similarOutput";

        System.out.println("**********************************************************" +
                "********************For Output Check file :" + filename);
        File file = new File(filename);
        if (file.exists() && file.isFile()) {
            file.delete();
        }
        BufferedWriter writer = new BufferedWriter(new FileWriter(file));
        writer.write(result.collect().toString());

        writer.close();
    }

}