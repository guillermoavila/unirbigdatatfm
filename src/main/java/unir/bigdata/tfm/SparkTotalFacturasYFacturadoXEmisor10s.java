package unir.bigdata.tfm;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.Time;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import scala.Tuple2;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.Serializable;
import java.util.Date;
import java.util.List;

public class SparkTotalFacturasYFacturadoXEmisor10s extends SparkBase implements Serializable {

    String nombreDeArchivo;

    public SparkTotalFacturasYFacturadoXEmisor10s(int duracionVentana) {
        super(duracionVentana);
        nombreDeArchivo = "/home/guillermo/Documents/salidas/SparkTotalFacturasYFacturadoXEmisor10s_" + System.currentTimeMillis() +".txt";
    }

    @Override
    protected void configurarAccionesAnaliticas() {

        streamingConObjetosFactura
                .window(Durations.seconds(10), Durations.seconds(10))
                .mapToPair(new PairFunction<Factura, String, Factura>()
                {
                    @Override
                    public Tuple2<String, Factura> call(Factura factura) throws Exception
                    {
                        return new Tuple2(factura.getNUMERO_RUC_EMISOR(), factura);
                    }
                })
                .groupByKey()
                .foreachRDD(new VoidFunction2<JavaPairRDD<String, Iterable<Factura>>, Time>()
                {

                    @Override
                    public void call(JavaPairRDD<String, Iterable<Factura>> stringIterableJavaPairRDD, Time window) throws Exception
                    {

                            long fechaInicio = System.currentTimeMillis();

                            List<Tuple2<String, Iterable<Factura>>> facturasXEmisores = stringIterableJavaPairRDD.collect();

                            for (Tuple2<String, Iterable<Factura>> facturasXEmisor : facturasXEmisores)
                            {

                                Long totalFacturas = 0L;
                                Float totalFacturado = 0F;
                                BufferedWriter writer = new BufferedWriter(new FileWriter(nombreDeArchivo,true));

                                String numeroRucEmisor = facturasXEmisor._1;
                                Iterable<Factura> facturas = facturasXEmisor._2;

                                for (Factura factura : facturas)
                                {
                                    totalFacturas += 1;
                                    totalFacturado += factura.getIMPORTE_TOTAL();
                                }

                                long fechaFin = System.currentTimeMillis();

                                if(totalFacturas>1)
                                {
                                    writer.write( window + "," + numeroRucEmisor + "," + totalFacturas + "," + totalFacturado + "," + (fechaFin - fechaInicio));
                                    writer.newLine();
                                }

                                writer.close();
                            }
                    }
                });
    }

    public static void main(String[] args) throws Exception {

        SparkTotalFacturasYFacturadoXEmisor10s sparkTotalFacturasYFacturadoXEmisor10s = new SparkTotalFacturasYFacturadoXEmisor10s(10);
        sparkTotalFacturasYFacturadoXEmisor10s.ejecutar();
    }
}