package com.jdriven.kafkaworkshop;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.HashMap;

@Component
public class KafkaStreamListener {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaStreamListener.class);

    @Autowired
    public KafkaStreamListener(KStream<String, SensorData> stream) {
        stream.foreach((id, sensorData) -> LOGGER.info("received kafka stream data: " +
                "id: "+ id + " data: "+ sensorData));

        stream.filter((id, sensorData) -> sensorData.getTemperature() > 30)
                .foreach((id, sensorData)-> LOGGER.info("sensor with id "+id + " temperature too HIGH!!"));

        stream.filter((id, sensorData) -> sensorData.getTemperature() < 10)
                .foreach((id, sensorData)-> LOGGER.info("sensor with id "+id + " temperature too LOW!!"));


        stream.filter((id, sensorData) -> sensorData.getVoltage() < 3)
                .mapValues(v-> v.getVoltage() )
                .peek((k,v) -> LOGGER.info("low voltage for "+k))
                .to(TopicNames.LOW_VOLTAGE_ALERT, Produced.with(Serdes.String(), Serdes.Double()));
    }

}
