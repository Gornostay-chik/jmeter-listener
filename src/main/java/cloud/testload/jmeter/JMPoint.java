package cloud.testload.jmeter;

/**
 * JM aggregation point class
 *
 * @author Alexander Babaev
 */

public class JMPoint {
    final long errorsCount;
    final long pointsCount;
    final Double averageTime;
    private String sampleLabel;
    private String threadName;

    JMPoint(String d, long b, long c, Double e) {
        //this.sampleLabel = a;
        this.errorsCount = b;
        this.pointsCount = c;
        this.threadName = d;
        this.averageTime = e;
    }

    JMPoint(long b, long c, Double e) {
        this.errorsCount = b;
        this.pointsCount = c;
        this.averageTime = e;
    }

    public String getSampleLabel() {
        return sampleLabel;
    }

    public void setSampleLabel(String newSampleLabel) {
        this.sampleLabel = newSampleLabel;
    }

    public long getErrorCount() {
        return errorsCount;
    }

    public long getPointsCount() {
        return pointsCount;
    }

    public Double getAverageTime() {
        return averageTime;
    }

    public long getAverageTimeInt() {
        return averageTime.intValue();
    }

    public String getThreadName() {
        return threadName;
    }

    @Override
    public String toString() {
        return "JMPoint{"
                + threadName +
                ";" +
                +pointsCount +
                ";" +
                +errorsCount +
                ";" +
                +averageTime +
                "}";
    }
}
