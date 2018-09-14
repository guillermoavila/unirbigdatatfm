package unir.bigdata.tfm;

import com.google.gson.Gson;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Date;
import java.util.Properties;

public abstract class FlinkBase {

    private StreamExecutionEnvironment ambienteFlink;
    private FlinkKafkaConsumer011 consumidorKafka;
    protected DataStream<Factura> streamingConObjetosFactura;

    public void ejecutar() throws Exception {
        cargarAmbienteEjecucionFlink();
        conectarKafka();
        recibirStreamingDeFacturacion();
        configurarAccionesAnaliticas();
        ejecutarAccionesAnaliticas();
    }

    private void cargarAmbienteEjecucionFlink() throws Exception {
        //se obtiene el ambiente de ejecucion del servidor Flink
        ambienteFlink = StreamExecutionEnvironment.getExecutionEnvironment();
        //se configura el tipo de tiempo de proceso
        ambienteFlink.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);
    }

    private void conectarKafka() throws Exception {
        //se configura las propiedades del servidor Kafka que contiene la facturacion electronica
        Properties propiedadesServidorKafka = new Properties();
        propiedadesServidorKafka.setProperty("zookeeper.connect", "192.168.0.101:2181");
        propiedadesServidorKafka.setProperty("bootstrap.servers", "192.168.0.101:9092");
        propiedadesServidorKafka.setProperty("group.id", "grupoFacturacion");
        propiedadesServidorKafka.setProperty("auto.offset.reset", "earliest");
        //se conecta el consumidor de Kafka hacia el servidor de Kafka
        consumidorKafka = new FlinkKafkaConsumer011("topic_FACTURA", new SimpleStringSchema(), propiedadesServidorKafka);
    }

    private void recibirStreamingDeFacturacion() throws Exception {
        //se recibe de manera constante el streaming con datos de facturacion electronica
        DataStream<String> streamingDeFacturacion = ambienteFlink.addSource(consumidorKafka);

        //El streaming que contiene cadenas de facturas se convierte en streaming de objetos de factura
        streamingConObjetosFactura = streamingDeFacturacion.map(cadaFactura->{
            Factura factura = new Gson().fromJson(cadaFactura, Factura.class);
            factura.setFECHA_RECEPCION_EVENTO(System.currentTimeMillis());
            factura.setCONTADOR(1L);
            return factura;
        });
    }

    protected abstract void configurarAccionesAnaliticas() throws Exception;

    private void ejecutarAccionesAnaliticas() throws Exception {
        ambienteFlink.execute();
    }
}
