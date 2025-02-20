public class ClusterPoint {
    private double x, y;
    public ClusterPoint(double x, double y) {
        this.x = x;
        this.y = y;
    }
    public double getX() {
        return x;
    }
    public double getY() {
        return y;
    }
    public static ClusterPoint fromString(String s) {
        try {
            String[] stringArr = s.split(",");
            return new ClusterPoint(Double.parseDouble(stringArr[0]), Double.parseDouble(stringArr[1]));

        } catch (Exception e) {
            System.out.println("Error in parsing string: " + s);
            return null;
        }
    }
    public double getDistance(ClusterPoint otherClusterPoint) {
        return Math.sqrt(Math.pow(this.x - otherClusterPoint.x, 2) + Math.pow(this.y - otherClusterPoint.y, 2));
    }

    @Override
    public String toString() {
        return x + "," + y;
    }
}
