package unir.bigdata.tfm;

import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.functions.windowing.WindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.Serializable;

public class FlinkTotalFacturasYFacturadoXEmisor10s5s extends FlinkBase implements Serializable {

    String nombreDeArchivo;

    public FlinkTotalFacturasYFacturadoXEmisor10s5s() {
        nombreDeArchivo =  "/home/guillermo/Documents/salidas/FlinkTotalFacturasYFacturadoXEmisor10s5s_" + System.currentTimeMillis() + ".txt";
    }

    @Override
    protected void configurarAccionesAnaliticas() {

        streamingConObjetosFactura
                .keyBy("NUMERO_RUC_EMISOR")
                .timeWindow(Time.seconds(10), Time.seconds(5))
                .apply(new WindowFunction<Factura, Tuple4<String, Long, Float, Long>, Tuple, TimeWindow>() {

                    @Override
                    public void apply(Tuple numeroRucEmisor, TimeWindow window, Iterable<Factura> facturas, Collector<Tuple4<String, Long, Float, Long>> collector) throws Exception {

                        BufferedWriter writer = new BufferedWriter(new FileWriter( nombreDeArchivo , true));
                        Tuple4<String, Long, Float, Long> totalFacturasYFacturado = new Tuple4<>(numeroRucEmisor.getField( 0), 0L, 0F, 0L);
                        long tiempoInicio = window.getEnd();

                        for (Factura factura : facturas) {
                            totalFacturasYFacturado.f1 += 1;
                            totalFacturasYFacturado.f2 += factura.getIMPORTE_TOTAL();
                        }

                        long tiempoFin = System.currentTimeMillis();
                        totalFacturasYFacturado.f3 = tiempoFin - tiempoInicio ;
                        collector.collect(totalFacturasYFacturado);

                        writer.write(window.getEnd() + "," + totalFacturasYFacturado.f0 + "," + totalFacturasYFacturado.f1 + "," + totalFacturasYFacturado.f2 + "," + totalFacturasYFacturado.f3);
                        writer.newLine();
                        writer.close();
                    }
                });
    }

    public static void main(String[] args) throws Exception {

        FlinkTotalFacturasYFacturadoXEmisor10s5s flinkTotalFacturasYFacturadoXEmisor10s5s = new FlinkTotalFacturasYFacturadoXEmisor10s5s();
        flinkTotalFacturasYFacturadoXEmisor10s5s.ejecutar();
    }
}
