package org.datasyslab.geospark.showcase;

import com.vividsolutions.jts.geom.Coordinate;
import com.vividsolutions.jts.geom.Envelope;
import org.apache.spark.api.java.JavaSparkContext;
import org.datasyslab.geospark.spatialRDD.PointRDD;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by anusha on 11/29/16.
 */
public class GetisOrd {

    public static JavaSparkContext sc = new JavaSparkContext();

    public static List<Envelope> grid = new ArrayList<>();
    public static PointRDD pointRdd;

    public static double roundToTwo(double value) {
        return (double)Math.round(value * 100d) / 100d;
    }

    public static void createEnvelopes(){
        for(double i=40.5;i<40.9;){
            for(double j = -74.25; j<-73.7;){
                double minX = i;
                double minY = j;
                double maxX = i+0.01;
                double maxY = j+0.01;
                grid.add(new Envelope(roundToTwo(minX), roundToTwo(maxX), roundToTwo(minY), roundToTwo(maxY)));
                j=roundToTwo(j+0.01);
            }
            i=roundToTwo(i+0.01);
        }
    }

    public static void createPointRDD(){
        String inputLocation = "/Users/anusha/Documents/DDS/top10rows.csv";
        int offset = 5;
        String splitter = ",";
        pointRdd = new PointRDD(sc, inputLocation, offset, splitter);
    }


    public static void main(String[] args) {
        createEnvelopes();
        int i=0;
        for(Envelope e:grid) {
            System.out.println(e.toString());
        }

        createPointRDD();

        System.out.println(grid.size());
        System.out.println(pointRdd.getRawPointRDD().count());
    }
}
