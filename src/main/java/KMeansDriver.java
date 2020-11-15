import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class KMeansDriver {
    private int k;
    private int iterationNum;
    private String sourcePath;
    private String outputPath;

    private Configuration conf;

    public KMeansDriver(int k, int iterationNum, String sourcePath, String outputPath, Configuration conf) {
        this.k = k;
        this.iterationNum = iterationNum;
        this.sourcePath = sourcePath;
        this.outputPath = outputPath;
        this.conf = conf;
    }

    public void clusterCenterJob(int i) throws IOException, InterruptedException, ClassNotFoundException {
        Job clusterCenterJob = new Job();
        clusterCenterJob.setJobName("clusterCenterJob" + i);
        clusterCenterJob.setJarByClass(KMeans.class);

        clusterCenterJob.getConfiguration().set("clusterPath", outputPath + "/cluster-" + i + "/");

        clusterCenterJob.setMapperClass(KMeans.KMeansMapper.class);
        clusterCenterJob.setMapOutputKeyClass(IntWritable.class);
        clusterCenterJob.setMapOutputValueClass(Cluster.class);

        clusterCenterJob.setCombinerClass(KMeans.KMeansCombiner.class);
        clusterCenterJob.setReducerClass(KMeans.KMeansReducer.class);
        clusterCenterJob.setOutputKeyClass(NullWritable.class);
        clusterCenterJob.setOutputValueClass(Cluster.class);

        FileInputFormat.addInputPath(clusterCenterJob, new Path(sourcePath));
        FileOutputFormat.setOutputPath(clusterCenterJob, new Path(outputPath + "/cluster-" + (i + 1) + "/"));

        clusterCenterJob.waitForCompletion(true);
        System.out.println("finished!");
    }

    public void KMeansClusterJob(int i) throws IOException, InterruptedException, ClassNotFoundException {
        Job kMeansClusterJob = new Job();
        kMeansClusterJob.setJobName("KMeansClusterJob");
        kMeansClusterJob.setJarByClass(KMeansCluster.class);

        kMeansClusterJob.getConfiguration().set("clusterPath", outputPath + "/cluster-" + (i + 1) + "/");

        kMeansClusterJob.setMapperClass(KMeansCluster.KMeansClusterMapper.class);
        kMeansClusterJob.setMapOutputKeyClass(Text.class);
        kMeansClusterJob.setMapOutputValueClass(IntWritable.class);

        kMeansClusterJob.setNumReduceTasks(0);

        FileInputFormat.addInputPath(kMeansClusterJob, new Path(sourcePath));
        FileOutputFormat.setOutputPath(kMeansClusterJob, new Path(outputPath + "/clusteredInstances_" + (i + 1) + "/"));

        kMeansClusterJob.waitForCompletion(true);
    }

    public void generateInitialCluster() {
        RandomClusterGenerator generator = new RandomClusterGenerator(conf, sourcePath, k);
        generator.generateInitialCluster(outputPath + "/");
    }

    public static ArrayList<ArrayList<Double>> readPoints(Configuration conf, String inputPath) throws IOException {
        FileSystem fs = FileSystem.get(conf);
        FileStatus[] fileList = fs.listStatus(new Path(inputPath));
        ArrayList<ArrayList<Double>> points = new ArrayList<ArrayList<Double>>();
        for (FileStatus fileStatus : fileList) {
            BufferedReader fis = new BufferedReader(new InputStreamReader(fs.open(fileStatus.getPath())));
            String line = null;
            while ((line = fis.readLine()) != null) {
                ArrayList<Double> point = new ArrayList<Double>();
                StringTokenizer itr = new StringTokenizer(line, ",");
                if (!itr.hasMoreTokens()) break;
                int i = 0;
                while (itr.hasMoreTokens()) {
                    if (i != 0 && i != 1) point.add(Double.parseDouble(itr.nextToken()));
                    i++;
                }
                points.add(point);
            }
            fis.close();
        }
        return points;
    }

    public void run() throws IOException, ClassNotFoundException, InterruptedException {
        generateInitialCluster();
        ArrayList<ArrayList<Double>> clusters = readPoints(conf, outputPath + "/cluster-0/");
        int i = 0;
        for (; i < iterationNum; i++) {
            clusterCenterJob(i);
            KMeansClusterJob(i);
            ArrayList<ArrayList<Double>> newClusters = readPoints(conf,outputPath + "/cluster-" + (i + 1) + "/");
            if (newClusters.equals(clusters)) break;
            clusters = newClusters;
        }
    }

    public static void main(String[] args) throws IOException, InterruptedException, ClassNotFoundException {
        System.out.println("start");
        Configuration conf = new Configuration();
        int k = Integer.parseInt(args[0]);
        int iterationNum = Integer.parseInt(args[1]);
        String sourcePath = args[2];
        String outputPath = args[3];
        KMeansDriver driver = new KMeansDriver(k, iterationNum, sourcePath, outputPath, conf);
        driver.run();
    }
}
