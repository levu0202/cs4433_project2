import java.io.*;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;



public class Main {
    public static final int MAX_ITERATION_VALUE = 10;
    public static final double THRESHOLD_VALUE = 0.001;

    public static void main(String[] args) throws IOException {
        computeDataGenerate("points.txt", 3000, 5000);
        computeDataGenerate("centroids.txt", 5, 10000);
        System.out.println("Data generation complete.");
        boolean isConverged = false;
        int currentIteration = 0;
        String pathInput = "points.txt";
        String pathCentroids = "centroids.txt";
        while (!isConverged && currentIteration < MAX_ITERATION_VALUE) {
            String currentPathOutput = "output" + currentIteration;
            boolean isSuccess = KMean.computeKMean(pathInput, currentPathOutput, pathCentroids);
            if (!isSuccess) {
                System.out.println("Job Failed");
                System.exit(1);
            }
            isConverged = isConvergenceReached(pathCentroids, currentPathOutput + "/part-r-00000");
            pathCentroids = currentPathOutput + "/part-r-00000";
            currentIteration++;
        }
        System.out.println("Final Result:");
        printFinalCLusterComputation(pathCentroids);
    }

    public static boolean isConvergenceReached(String prevCentroids, String newCentroids) throws IOException {
        List<String> prevArr = Files.readAllLines(Paths.get(prevCentroids));
        List<String> newArr = Files.readAllLines(Paths.get(newCentroids));
        if (prevArr.size() != newArr.size()) return false;
        for (int i = 0; i < prevArr.size(); i++) {
            String[] prevCoords = prevArr.get(i).split(",");
            String[] newCoords = newArr.get(i).split(",");
            double prevX = Double.parseDouble(prevCoords[0]);
            double prevY = Double.parseDouble(prevCoords[1]);
            double newX = Double.parseDouble(newCoords[0]);
            double newY = Double.parseDouble(newCoords[1]);

            if (Math.sqrt(Math.pow(newX - prevX, 2) + Math.pow(newY - prevY, 2))>  THRESHOLD_VALUE) {
                return false;
            }
        }
        return true;
    }


    private static void printFinalCLusterComputation(String pathInput) throws IOException {
        List<String> finalClusterArray = Files.readAllLines(Paths.get(pathInput));
        for (String currentLine : finalClusterArray) {
            System.out.println(currentLine);
        }
    }

    public static void computeDataGenerate(String fileNameInput, int pointCountInput, int rangeInput) throws IOException {
        Random rand = new Random();
        FileWriter fw = new FileWriter(fileNameInput);

        for (int i = 0; i < pointCountInput; i++) {
            double currentXValue = rand.nextDouble() * rangeInput;
            double currentYValue = rand.nextDouble() * rangeInput;
            fw.write(currentXValue + "," + currentYValue + "\n");
        }
        fw.close();

    }

}
