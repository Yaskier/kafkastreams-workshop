package com.jdriven.kafkaworkshop;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.PostMapping;

@Controller
public class SensorController {

    @Autowired private KafkaTemplate<String, SensorData> kafkaTemplate;
    private static final Logger LOGGER = LoggerFactory.getLogger(SensorController.class);
    private String topic = TopicNames.RECEIVED_SENSOR_DATA;

    @GetMapping("/sensor")
    public String greetingForm(Model model) {
        model.addAttribute("sensorData", new SensorData());
        return "sensor";
    }

    @PostMapping("/sensor")
    public String sensorSubmit(@ModelAttribute SensorData sensorData) {
        kafkaTemplate.send(topic, sensorData.getId(), sensorData);
        return "sensor";
    }

}
