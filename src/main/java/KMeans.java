import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.StringUtils;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.*;
import java.util.function.DoubleBinaryOperator;

public class KMeans {

    public static class Points extends ArrayList<ArrayList<Double>> implements Writable {

        public void initFromPoints(DataInput in) throws IOException{
            Points points = new Points();
        }

        @Override
        public void write(DataOutput out) throws IOException {
            out.writeInt(this.size());
            for (ArrayList<Double> i : this) {
                out.writeInt(i.size());
                for (Double j : i) {
                    out.writeDouble(j);
                }
            }
        }

        @Override
        public void readFields(DataInput in) throws IOException {
            this.clear();
            int listNum = in.readInt();
            for (int i = 0; i < listNum; i++) {
                ArrayList<Double> listTemp = new ArrayList<Double>();
                int doubleNum = in.readInt();
                for (int j = 0; j < doubleNum; j++) {
                    Double doubleTemp = in.readDouble();
                    listTemp.add(doubleTemp);
                }
                this.add(listTemp);
            }
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        GenericOptionsParser optionsParser = new GenericOptionsParser(conf, args);
        String[] remainingArgs = optionsParser.getRemainingArgs();

        Points clusterCenters = new Points();

        System.exit(0);
    }

}
