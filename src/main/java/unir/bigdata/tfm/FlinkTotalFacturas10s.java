package unir.bigdata.tfm;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple1;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.AllWindowedStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;

public class FlinkTotalFacturas10s extends FlinkBase implements Serializable {

    long currentTimeMillis;

    public FlinkTotalFacturas10s()
    {
        currentTimeMillis  = System.currentTimeMillis();
    }

    @Override
    protected void configurarAccionesAnaliticas() throws Exception {

        streamingConObjetosFactura
                .timeWindowAll(Time.seconds(10))
                .aggregate(new FlinkTotalFacturasYFacturado(System.currentTimeMillis()));
    }

    public static void main(String[] args) throws Exception {

        FlinkTotalFacturas10s flinkTotalFacturas10s = new FlinkTotalFacturas10s();
        flinkTotalFacturas10s.ejecutar();
    }

    private static class FlinkTotalFacturasYFacturado implements AggregateFunction<Factura, Factura, Long> {

        private long currentTimeMillis;
        private String nombreDeArchivo;

        FlinkTotalFacturasYFacturado(long currentTimeMillis)
        {
            this.currentTimeMillis = currentTimeMillis;
            this.nombreDeArchivo = "/home/guillermo/Documents/salidas/FlinkTotalFacturas10s_" + currentTimeMillis +".txt";
        }

        @Override
        public Factura createAccumulator() {
            Factura factura = new Factura();
            factura.setCONTADOR(0L);
            return factura;
        }

        @Override
        public Factura add(Factura factura, Factura factura2) {
            Factura facturaAcumulada = new Factura();
            facturaAcumulada.setCONTADOR(factura2.getCONTADOR() + 1L);
            return facturaAcumulada;
        }

        @Override
        public Long getResult(Factura factura) {
            try {
                BufferedWriter writer = new BufferedWriter(new FileWriter(nombreDeArchivo,true));
                writer.write(String.valueOf(factura.getCONTADOR()));
                writer.newLine();
                writer.close();
                return factura.getCONTADOR();
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }

        @Override
        public Factura merge(Factura factura, Factura acc1) {
            return null;
        }
    }
}
