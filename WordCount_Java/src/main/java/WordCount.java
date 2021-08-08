import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.AggregateOperator;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

public class WordCount {

    public static void main(String[] args) throws Exception {

        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> source = env.fromElements(
                "Who's there?",
                "I think I hear them. Stand, ho! Who's there?"
        );

        AggregateOperator<Tuple2<String, Integer>> wordCounts = source
                .flatMap(new LineSplitter())
                .groupBy(0)
                .sum(1);

        wordCounts.print();
    }

    public static class LineSplitter implements FlatMapFunction<String, Tuple2<String, Integer>> {
        @Override
        public void flatMap(String line, Collector<Tuple2<String, Integer>> collector) {
            for (String word : line.split(" ")) {
                collector.collect(new Tuple2<>(word, 1));
            }
        }
    }
}
