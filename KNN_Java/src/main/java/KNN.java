import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;

import java.util.*;

public class KNN {

    public static void main(String[] args) throws Exception {

        // checking input parameters
        final ParameterTool params = ParameterTool.fromArgs(args);

        // set up execution environment
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        env.getConfig().setGlobalJobParameters(params); // make parameters available in the web interface

        // get input data
        // read training data and test data from the provided paths of fall back to default data
        DataSet<LabeledPoint> trainingDataSet = getTrainingDataSet(params, env);
        DataSet<Point> testingDataSet = getTestingDataSet(params, env);

        // set k for KNN algorithm
        int k = params.getInt("k", 10);

        DataSet<LabeledPoint> result = testingDataSet.map(new RichMapFunction<Point, LabeledPoint>() {
            private Collection<LabeledPoint> trainingData;

            @Override
            public void open(Configuration parameters) {
                this.trainingData = getRuntimeContext().getBroadcastVariable("trainingDataSet");
            }

            @Override
            public LabeledPoint map(Point point) {
                Queue<Tuple2<Double, String>> queue = new PriorityQueue<>(k, (o1, o2) -> {
                    if (o1.f0.equals(o2.f0)) {
                        return 0;
                    } else if (o1.f0 > o2.f0) {
                        return -1;
                    } else {
                        return 1;
                    }
                });
                for (LabeledPoint labeledPoint : this.trainingData) {
                    Double dist = point.euclideanDistance(new Point(labeledPoint.getX(), labeledPoint.getY()));
                    if (queue.size() == k) {
                        assert queue.peek() != null;
                        if (dist < queue.peek().f0) {
                            queue.poll();
                        }
                    }
                    queue.add(new Tuple2<>(dist, labeledPoint.getLabel()));
                }
                // 统计标签数量
                Map<String, Integer> map = new HashMap<>();
                while (!queue.isEmpty()) {
                    Tuple2<Double, String> tuple = queue.poll();
                    map.put(tuple.f1, map.getOrDefault(tuple.f1, 0) + 1);
                }
                // 找出数量最多的标签
                String label = null;
                int maxCount = -1;
                for (String s : map.keySet()) {
                    if (map.get(s) > maxCount) {
                        maxCount = map.get(s);
                        label = s;
                    }
                }
                return new LabeledPoint(point, label);
            }
        }).withBroadcastSet(trainingDataSet, "trainingDataSet");

        // emit result
        if (params.has("output")) {
            result.map((MapFunction<LabeledPoint, Tuple3<Double, Double, String>>) labeledPoint -> new Tuple3<>(labeledPoint.getX(), labeledPoint.getY(), labeledPoint.getLabel())).writeAsCsv(params.get("output"), "\n", " ", FileSystem.WriteMode.OVERWRITE);
            // since file sinks are lazy, we trigger the execution explicitly
            env.execute("KNN Example");
        } else {
            System.out.println("Printing result to stdout. Use --output to specify output path.");
            result.print();
        }
    }

    private static DataSet<LabeledPoint> getTrainingDataSet(ParameterTool params, ExecutionEnvironment env) {
        DataSet<LabeledPoint> trainingDataSet;
        if (params.has("train")) {
            trainingDataSet = env.readCsvFile(params.get("train"))
                    .fieldDelimiter(" ")
                    .pojoType(LabeledPoint.class, "x", "y", "label");
        } else {
            System.out.println("Executing KNN example with default training data set.");
            System.out.println("Use --train to specify file input.");
            trainingDataSet = KNNData.getDefaultTrainingDataSet(env);
        }
        return trainingDataSet;
    }

    private static DataSet<Point> getTestingDataSet(ParameterTool params, ExecutionEnvironment env) {
        DataSet<Point> testingDataSet;
        if (params.has("test")) {
            // read testing data from CSV file
            testingDataSet = env.readCsvFile(params.get("test"))
                    .fieldDelimiter(" ")
                    .pojoType(Point.class, "x", "y");
        } else {
            System.out.println("Executing KNN example with default testing data set.");
            System.out.println("Use --test to specify file input.");
            testingDataSet = KNNData.getDefaultTestingDataSet(env);
        }
        return testingDataSet;
    }
}
