import java.io.Serializable;

/**
 * A simple two-dimensional point.
 */
public class LabeledPoint implements Serializable {

    public double x, y;

    public String label;

    public LabeledPoint() {}

    public LabeledPoint(double x, double y, String label) {
        this.x = x;
        this.y = y;
        this.label = label;
    }

    public LabeledPoint(Point point, String label) {
        this.x = point.getX();
        this.y = point.getY();
        this.label = label;
    }

    public double getX() {
        return x;
    }

    public double getY() {
        return y;
    }

    public String getLabel() {
        return label;
    }

    public double euclideanDistance(Point other) {
        return Math.sqrt((x - other.x) * (x - other.x) + (y - other.y) * (y - other.y));
    }

    @Override
    public String toString() {
        return "(" + x + ", " + y + "): " + label;
    }
}