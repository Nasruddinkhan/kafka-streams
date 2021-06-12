package com.mypractice.kafka.streams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {
    private static final Logger logger = LoggerFactory.getLogger(WordCountApp.class);
    private static final String OUT_PUT_TOPIC = "word-count-output" ;
    private static final String COUNTS = "Counts";
    private static final String INPUT_TOPIC = "word-count-input";
    private static final String WORD_COUNT_APPLICATION = "wordcount-application";
    private static final String BOOTSTRAP_SERVERS =  "127.0.0.1:9092";
    private static final String EARLIEST ="earliest" ;

    public static void main(String[] args) {
        var config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, WORD_COUNT_APPLICATION);
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, EARLIEST);
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        var wordCountApp = new WordCountApp();
        try (var streams = new KafkaStreams(wordCountApp.createTopology(), config)) {
            streams.start();
            Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
            do {
                streams.localThreadsMetadata().forEach(data -> logger.info("data ->{}", data));
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    logger.warn("InterruptedException", e);
                    Thread.currentThread().interrupt();
                    break;
                }
            } while (true);
        }
    }
    private Topology createTopology() {
        var builder = new StreamsBuilder();
        KStream<String, String> textLines = builder.stream(INPUT_TOPIC);
        KTable<String, Long> wordCounts = textLines
                .mapValues((ValueMapper<String, String>) String::toLowerCase)
                .flatMapValues(textLine -> Arrays.asList(textLine.split("\\W+")))
                .selectKey((key, word) -> word)
                .groupByKey()
                .count(Materialized.as(COUNTS));
        wordCounts.toStream().to(OUT_PUT_TOPIC, Produced.with(Serdes.String(), Serdes.Long()));
        return builder.build();
    }

}

