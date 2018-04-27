import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.*;

public class MaximalCliques {

    public static void main(String[] args) throws IOException {


        SparkConf conf = new SparkConf().setMaster("local").setAppName("MaximalCliques");
        JavaSparkContext sc = new JavaSparkContext(conf);

        //filePath
        String filepath = System.getProperty("user.dir") + "/cliques";

        // Load our input data.
        JavaRDD<String> input = sc.textFile(filepath);

        //creating pairs of set against adjacency list
        JavaPairRDD<Set<String>, Set<String>> pairRDD = input.mapToPair(s -> {
            String[] split = s.split(" ");
            Set<String> initial = new LinkedHashSet<>();
            initial.add(split[0]);
            String s1 = split[1];
            String[] split1 = s1.substring(1, s1.length() - 1).split("\\,");
            return new Tuple2<>(initial, new LinkedHashSet<>(Arrays.asList(split1)));
        });

        JavaPairRDD<Set<String>, Set<String>> temp= pairRDD;
        boolean terminate =false;

        while (!terminate){

            //creating every possible combination from adjacency list
            JavaPairRDD<Set<String>, Set<String>> tuple2JavaRDD = temp.
                    flatMap( tuple -> createCombinations(tuple).iterator()).
                    mapToPair(setSetTuple2 -> new Tuple2<>(setSetTuple2._1,setSetTuple2._2));

            //taking intersection between similar key cliques
            JavaPairRDD<Set<String>, Set<String>> setSetJavaPairRDD = tuple2JavaRDD.reduceByKey((x,y) -> {
                x.retainAll(y);
                x.add("$");
                return x;
            }).filter(x -> x._2.contains("$")).mapToPair(tuple -> {
                Set<String> edges = tuple._2;
                edges.remove("$");
                return new Tuple2<>(tuple._1, edges);
            });

            long count = setSetJavaPairRDD.filter(x -> !x._2.isEmpty()).count();
            if(count==0){
                terminate=true;
            }
            temp= setSetJavaPairRDD;
        }

        printOutput(temp.collect());
    }

    private static List<Tuple2<Set<String>, Set<String>>> createCombinations(Tuple2<Set<String>, Set<String>> stringListTuple2) {
        List<Tuple2<Set<String>, Set<String>>> list = new ArrayList<>();
        Set<String> first = stringListTuple2._1;
        List<String> strings = new ArrayList<>(stringListTuple2._2);
        for (int i = 0; i < strings.size(); i++) {
            Set<String> subset = new LinkedHashSet<>();
            subset.addAll(first);
            subset.add(strings.get(i));
            Set<String> remaining = getNewList(i, strings);

            list.add(new Tuple2<>(subset, remaining));
        }
        return list;
    }

    private static Set<String> getNewList(int i, List<String> strings) {
        Set<String> list = new LinkedHashSet<>();
        for (int j = 0; j < strings.size(); j++) {
            if(j!=i){
                list.add(strings.get(j));
            }
        }
        return list;
    }

    private static void printOutput(List<Tuple2<Set<String>, Set<String>>> result) throws IOException {
        String filename = "output/cliquesOutput";

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