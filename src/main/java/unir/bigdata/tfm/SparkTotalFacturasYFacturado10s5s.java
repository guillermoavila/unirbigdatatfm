package unir.bigdata.tfm;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.streaming.Durations;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.Serializable;
import java.util.Date;
import java.util.List;

public class SparkTotalFacturasYFacturado10s5s extends SparkBase implements Serializable {

    String nombreDeArchivo;

    public SparkTotalFacturasYFacturado10s5s(int duracionVentana) {
        super(duracionVentana);
        nombreDeArchivo = "/home/guillermo/Documents/salidas/SparkTotalFacturasYFacturado10s5s_" + System.currentTimeMillis() +".txt";
    }

    @Override
    protected void configurarAccionesAnaliticas() {

        streamingConObjetosFactura.window(Durations.seconds(10), Durations.seconds(5))
        .foreachRDD(new VoidFunction<JavaRDD<Factura>>() {

            @Override
            public void call(JavaRDD<Factura> facturaJavaRDD) throws Exception {

                Long totalFacturas = 0L;
                Float totalFacturado = 0F;
                BufferedWriter writer = new BufferedWriter(new FileWriter(nombreDeArchivo,true));
                long fechaInicio = System.currentTimeMillis();

                List<Factura> collect = facturaJavaRDD.collect();

                for (Factura factura : collect) {
                    totalFacturas += 1;
                    totalFacturado += factura.getIMPORTE_TOTAL();
                }

                long fechaFin = new Date().getTime();

                if(totalFacturas>1) {
                    writer.write(totalFacturas + "," + totalFacturado + "," + (fechaFin - fechaInicio));
                    writer.newLine();
                }
                writer.close();
            }
        });
    }

    public static void main(String[] args) throws Exception {

        SparkTotalFacturasYFacturado10s5s sparkTotalFacturasYFacturado10s5s = new SparkTotalFacturasYFacturado10s5s(5);
        sparkTotalFacturasYFacturado10s5s.ejecutar();
    }
}
