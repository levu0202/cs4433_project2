import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;


public class KMean {
    public static class computeKMeanMapper extends Mapper<Object, Text, Text, Text> {
        private List<ClusterPoint> centroidArr = new ArrayList<>();

        @Override
        protected void setup(Context context) throws IOException, InterruptedException {
            Path centroidsPath = new Path("centroids.txt"); // Change if stored elsewhere
            FileSystem fs = FileSystem.get(context.getConfiguration());
            BufferedReader br = new BufferedReader(new InputStreamReader(fs.open(centroidsPath)));

            String line;
            while ((line = br.readLine()) != null) {
                centroidArr.add(ClusterPoint.fromString(line));
            }
            br.close();
        }

        @Override
        protected void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            ClusterPoint currentPoint = ClusterPoint.fromString(value.toString());
            ClusterPoint closestCentroidPoint = null;
            double minDistance = Double.MAX_VALUE;

            for (ClusterPoint centroid: centroidArr) {
                double distanceValue = currentPoint.getDistance(centroid);
                if (distanceValue < minDistance) {
                    minDistance = distanceValue;
                    closestCentroidPoint = centroid;
                }
            }
            if (closestCentroidPoint != null) {
                context.write(new Text(closestCentroidPoint.toString()), new Text(currentPoint.toString()));
            }
        }
    }

    public static class computeKMeanReducer extends Reducer<Text, Text, Text, Text> {
        @Override
        protected void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
            List<ClusterPoint> pointArr = new ArrayList<>();
            double sumXValue = 0, sumYValue = 0;
            int pointCount = 0;
            for (Text value: values) {
                ClusterPoint point = ClusterPoint.fromString(value.toString());
                pointArr.add(point);
                sumXValue += point.getX();
                sumYValue += point.getY();
                pointCount++;
            }
            if (pointCount > 0) {
                ClusterPoint newPoint = new ClusterPoint(sumXValue / pointCount, sumYValue / pointCount);
                context.write(new Text(newPoint.toString()), null);
            }
        }
    }

    public static boolean computeKMean(String pathInput, String pathOutput, String pathCentroids) {
        try {

            Configuration conf = new Configuration();
            FileSystem fs = FileSystem.get(conf);
            if (!fs.exists(new Path(pathInput))) {
                System.err.println("Error: Input file does not exist: " + pathInput);
                return false;
            }
            if (!fs.exists(new Path(pathCentroids))) {
                System.err.println("Error: Centroids file does not exist: " + pathCentroids);
                return false;
            }
            conf.set("centroids", pathCentroids);

            Job firstJob = Job.getInstance(conf , "K mean Job");
            firstJob.setJarByClass(KMean.class);
            firstJob.setMapperClass(computeKMeanMapper.class);
            firstJob.setReducerClass(computeKMeanReducer.class);

            firstJob.setMapOutputKeyClass(Text.class);
            firstJob.setMapOutputValueClass(Text.class);

            FileInputFormat.addInputPath(firstJob, new Path(pathInput));
            FileOutputFormat.setOutputPath(firstJob, new Path(pathOutput));

            return firstJob.waitForCompletion(true);

        } catch (IOException | InterruptedException | ClassNotFoundException e) {
            System.err.println("Error while running the K-Means job:");
            e.printStackTrace();
            return false;
        }

    }




}
