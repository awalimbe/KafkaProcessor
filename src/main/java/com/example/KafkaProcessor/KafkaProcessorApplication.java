package com.example.KafkaProcessor;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.TimeUnit;


@SpringBootApplication
public class KafkaProcessorApplication {

	public static void main(String[] args) {
		SpringApplication.run(KafkaProcessorApplication.class, args);

		KafkaStreams streamsInnerJoin;


		final Serde<String> stringSerde = Serdes.String();
		final Serde<Long> longSerde = Serdes.Long();

		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "stream-stream-inner-join");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
		props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

		final StreamsBuilder builder = new StreamsBuilder();

		// Read the source stream.  In this example, we ignore whatever is stored in the record key and
		// assume the record value contains the username (and each record would represent a single
		// click by the corresponding user).
		KStream<String,String> transactions = builder.stream("transactions");
		KStream<String,String> descriptions = builder.stream("descriptions");


//		KStream<String, Long> left = builder.stream("transactions");
//		KStream<String, Double> right = builder.stream("descriptions");

// Java 8+ example, using lambda expressions
//		KStream<String, String> joined = left.join(right,
//				(leftValue, rightValue) -> "left=" + leftValue + ", right=" + rightValue, /* ValueJoiner */
//				JoinWindows.of(TimeUnit.MINUTES.toMillis(5)),
//				Joined.with(
//						Serdes.String(), /* key */
//						Serdes.Long(),   /* left value */
//						Serdes.Double())  /* right value */
//		);



		KStream<String, String> joined = transactions.join(descriptions,

				(String leftValue, String rightValue) -> {

					     JsonObject leftObject = new JsonParser().parse(leftValue).getAsJsonObject();
					     JsonObject rightObject = new JsonParser().parse(rightValue).getAsJsonObject();

					     JsonElement leftElement = leftObject.get("id");
					     JsonElement rightElement = rightObject.get("id");

					     String leftId = leftElement.getAsString();
					     String rightId = rightElement.getAsString();

					     if (leftId.equals(rightId))
						 {
						 	JsonElement rightDescription =  rightObject.get("description");
						 	leftObject.addProperty("description", rightDescription.getAsString());
						 	return leftObject.toString();
						 }
						 else
						 	return null;


			             },
				JoinWindows.of(Duration.ofMinutes(5)),
				Joined.with(
						Serdes.String(), /* key */
						Serdes.String(),   /* left value */
						Serdes.String())  /* right value */
		);

		KStream <String, String> filteredStream = joined.filter(new Predicate<String, String>(){
			public boolean test(String key, String value){
				return value != null;
			}
		});

		filteredStream.to("for_mongodb");

		final Topology topology = builder.build();
		streamsInnerJoin = new KafkaStreams(topology, props);

		streamsInnerJoin.cleanUp();
		streamsInnerJoin.start();




		// Add shutdown hook to respond to SIGTERM and gracefully close Kafka Streams
		Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {
			@Override
			public void run() {
				streamsInnerJoin.close();
			}
		}));


	}

}


