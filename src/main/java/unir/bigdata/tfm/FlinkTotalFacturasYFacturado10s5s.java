package unir.bigdata.tfm;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.Serializable;

public class FlinkTotalFacturasYFacturado10s5s extends FlinkBase implements Serializable {

    String nombreDeArchivo;

    public FlinkTotalFacturasYFacturado10s5s()
    {
        nombreDeArchivo = "/home/guillermo/Documents/salidas/FlinkTotalFacturasYFacturado10s5s_" + System.currentTimeMillis() + ".txt";
    }

    @Override
    protected void configurarAccionesAnaliticas() {

        streamingConObjetosFactura
                .timeWindowAll(Time.seconds(10), Time.seconds(5))
                .apply(new AllWindowFunction<Factura, Tuple3<Long, Float, Long>, TimeWindow>() {

                    @Override
                    public void apply(TimeWindow timeWindow, Iterable<Factura> iterable, Collector<Tuple3<Long, Float, Long>> collector) throws Exception {

                    BufferedWriter writer = new BufferedWriter(new FileWriter( nombreDeArchivo ,true));
                    Tuple3<Long, Float, Long> totalFacturasYFacturado=new Tuple3<>(0L,0F, 0L);
                    long tiempoInicio = timeWindow.getEnd();

                    for (Factura factura : iterable) {
                        totalFacturasYFacturado.f0 += 1;
                        totalFacturasYFacturado.f1 += factura.getIMPORTE_TOTAL();
                    }

                    long tiempoFin = System.currentTimeMillis();
                    totalFacturasYFacturado.f2 = tiempoFin - tiempoInicio ;
                    collector.collect(totalFacturasYFacturado);

                    writer.write(totalFacturasYFacturado.f0 + "," + totalFacturasYFacturado.f1 + "," + totalFacturasYFacturado.f2);
                    writer.newLine();
                    writer.close();
            }
        });
  }

    public static void main(String[] args) throws Exception {

        FlinkTotalFacturasYFacturado10s5s flinkTotalFacturasYFacturado10S5S = new FlinkTotalFacturasYFacturado10s5s();
        flinkTotalFacturasYFacturado10S5S.ejecutar();
    }
}
