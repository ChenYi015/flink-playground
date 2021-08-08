import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.LinkedList;
import java.util.List;


public class KNNData {

    public static final Object[][] TEST = new Object[][] {
            new Object[] {-31.85, -44.77},
            new Object[] {35.16, 17.46},
            new Object[] {-5.16, 21.93},
            new Object[] {-24.06, 6.81}
    };

    public static final Object[][] POINTS = new Object[][] {
            new Object[] {-14.22, -48.01, "0"},
            new Object[] {-22.78, 37.10, "1"},
            new Object[] {56.18, -42.99, "2"},
            new Object[] {35.04, 50.29, "3"},
            new Object[] {-9.53, -46.26, "4"},
            new Object[] {-34.35, 48.25, "0"},
            new Object[] {55.82, -57.49, "1"},
            new Object[] {21.03, 54.64, "2"},
            new Object[] {-13.63, -42.26, "3"},
            new Object[] {-36.57, 32.63, "4"},
            new Object[] {50.65, -52.40, "0"},
            new Object[] {24.48, 34.04, "1"},
            new Object[] {-2.69, -36.02, "2"},
            new Object[] {-38.80, 36.58, "3"},
            new Object[] {24.00, -53.74, "4"},
            new Object[] {32.41, 24.96, "0"},
            new Object[] {-4.32, -56.92, "1"},
            new Object[] {-22.68, 29.42, "2"},
            new Object[] {59.02, -39.56, "3"},
            new Object[] {24.47, 45.07, "4"},
            new Object[] {5.23, -41.20, "0"},
            new Object[] {-23.00, 38.15, "1"},
            new Object[] {44.55, -51.50, "2"},
            new Object[] {14.62, 59.06, "3"},
            new Object[] {7.41, -56.05, "4"},
            new Object[] {-26.63, 28.97, "0"},
            new Object[] {47.37, -44.72, "1"},
            new Object[] {29.07, 51.06, "2"},
            new Object[] {0.59, -31.89, "3"},
            new Object[] {-39.09, 20.78, "4"},
            new Object[] {42.97, -48.98, "0"},
            new Object[] {34.36, 49.08, "1"},
            new Object[] {-21.91, -49.01, "2"},
            new Object[] {-46.68, 46.04, "3"},
            new Object[] {48.52, -43.67, "4"},
            new Object[] {30.05, 49.25, "0"},
            new Object[] {4.03, -43.56, "1"},
            new Object[] {-37.85, 41.72, "2"},
            new Object[] {38.24, -48.32, "3"},
            new Object[] {20.83, 57.85, "4"}
    };

    public static DataSet<LabeledPoint> getDefaultTrainingDataSet(ExecutionEnvironment env) {
        List<LabeledPoint> trainingData = new LinkedList<>();
        for (Object[] object: POINTS) {
            trainingData.add(new LabeledPoint((Double) object[0], (Double) object[1], (String) object[2]));
        }
        return env.fromCollection(trainingData);
    }

    public static DataSet<Point> getDefaultTestingDataSet(ExecutionEnvironment env) {
        List<Point> testingData = new LinkedList<>();
        for (Object[] object: TEST) {
            testingData.add(new Point((Double) object[0], (Double) object[1]));
        }
        return env.fromCollection(testingData);
    }
}
