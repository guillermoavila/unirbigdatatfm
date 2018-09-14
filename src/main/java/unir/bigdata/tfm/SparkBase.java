package unir.bigdata.tfm;

import com.google.gson.Gson;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.ConsumerStrategies;
import org.apache.spark.streaming.kafka010.KafkaUtils;
import org.apache.spark.streaming.kafka010.LocationStrategies;

import java.util.*;

public abstract class SparkBase {

    private Map<String, Object> parametrosKafka;
    protected int duracionVentana;
    private JavaStreamingContext jssc;
    protected JavaDStream<Factura> streamingConObjetosFactura;

    public SparkBase() {

    }

    public SparkBase(int duracionVentana) {
        this.duracionVentana = duracionVentana;
    }

    public void ejecutar() throws Exception {
        cargarAmbienteEjecucionSpark();
        configurarConsumidorKafka();
        recibirStreamingDeFacturacion();
        configurarAccionesAnaliticas();
        ejecutarAccionesAnaliticas();
    }

    private void cargarAmbienteEjecucionSpark() {
        Logger.getRootLogger().setLevel(Level.WARN);
        SparkConf sparkConf = new SparkConf().setAppName("FromKafka");
        jssc = new JavaStreamingContext(sparkConf, Durations.seconds(duracionVentana));
        jssc.checkpoint("/home/guillermo/app/spark-2.3.1-bin-hadoop2.7/checkpoint");
    }

    private void configurarConsumidorKafka(){
        parametrosKafka = new HashMap<>();
        parametrosKafka.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"192.168.0.101:9092");
        parametrosKafka.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        parametrosKafka.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
                "org.apache.kafka.common.serialization.StringDeserializer");
        parametrosKafka.put(ConsumerConfig.GROUP_ID_CONFIG,"grupoFacturacion");
        parametrosKafka.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,"latest");
        parametrosKafka.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,true);
        parametrosKafka.put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG,1000*60*60);

    }

    private void recibirStreamingDeFacturacion() {

        Set<String> topicsSet = new HashSet<String>();
        topicsSet.add("topic_FACTURA");

        //se recibe de manera constante el streaming con datos de facturacion electronica
        JavaInputDStream<ConsumerRecord<String, String>> streamingDeFacturacion = KafkaUtils.createDirectStream(
                jssc,
                LocationStrategies.PreferConsistent(),
                ConsumerStrategies.Subscribe(topicsSet, parametrosKafka));

        streamingConObjetosFactura = streamingDeFacturacion.map(cadaFactura -> {
            Factura factura = new Gson().fromJson(cadaFactura.value(), Factura.class);
            factura.setFECHA_RECEPCION_EVENTO(new Date().getTime());
            factura.setCONTADOR(1L);
            return factura;
        });

    }

    protected abstract void configurarAccionesAnaliticas();

    private void ejecutarAccionesAnaliticas() throws Exception {
        jssc.start();
        jssc.awaitTermination();
    }

}
