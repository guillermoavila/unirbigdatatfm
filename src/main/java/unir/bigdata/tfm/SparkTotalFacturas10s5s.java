package unir.bigdata.tfm;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.Serializable;

public class SparkTotalFacturas10s5s extends SparkBase implements Serializable {

    String nombreDeArhivo;

    public SparkTotalFacturas10s5s(int duracionVentana) {
        super(duracionVentana);
        nombreDeArhivo = "/home/guillermo/Documents/salidas/SparkTotalFacturas10s5s_" + System.currentTimeMillis() +".txt";
    }

    @Override
    protected void configurarAccionesAnaliticas()
    {

        streamingConObjetosFactura
                .countByWindow(Durations.seconds(10), Durations.seconds(5))
                .foreachRDD(new VoidFunction<JavaRDD<Long>>() {

                    @Override
                    public void call(JavaRDD<Long> longJavaRDD) throws Exception
                    {
                        BufferedWriter writer = new BufferedWriter(new FileWriter(nombreDeArhivo,true));
                        if(!longJavaRDD.collect().isEmpty())
                        {
                            Long totalFacturas = longJavaRDD.collect().get(0).longValue();
                            writer.write(totalFacturas.toString());
                            writer.newLine();
                        }
                        writer.close();
                    }
        });
    }

    public static void main(String[] args) throws Exception
    {

        SparkTotalFacturas10s5s sparkTotalFacturas10s = new SparkTotalFacturas10s5s(5);
        sparkTotalFacturas10s.ejecutar();
    }
}
