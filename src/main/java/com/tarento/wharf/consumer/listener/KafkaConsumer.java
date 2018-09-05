package com.tarento.wharf.consumer.listener;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;
import com.tarento.wharf.models.Project;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.slf4j.LoggerFactory;

import java.util.Map;

@Service
public class KafkaConsumer {
    public static final org.slf4j.Logger logger = LoggerFactory.getLogger(KafkaConsumer.class);
    @Autowired
    private ObjectMapper objectMapper;

    /*  CONSUMING DATA FROM KAFKA SERVER   */
    @KafkaListener(topics = "Kafka_Publish_topic", group = "project_json",
            containerFactory = "kafkaListenerContainerFactory")
    public String consumeJson(Map<String, Object> consumerRecord) throws JsonProcessingException {
        try {
            consumerRecord.values();
            String json = objectMapper.writeValueAsString(consumerRecord);
            System.out.println(json);
            Gson gson = new Gson();
            Project project = gson.fromJson(json, Project.class);
            /// Kronos DB Called ////
            System.out.println(project);
            return null;

        } catch (Exception exception) {
            logger.debug("Consumer:processMessage:" + exception);
            throw exception;
        }

    }
}
