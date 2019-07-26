import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import java.util.*;


public class A4Application {

    static class RoomState {

        private Long occupancy;
        private Long capacity;
        private String output;

        RoomState() {
        }

        RoomState(Long occupancy, Long capacity) {
            this.occupancy = occupancy;
            this.capacity = capacity;
        }

        boolean isOk() {
            return occupancy == null || capacity == null || occupancy <= capacity;
        }

        boolean isOverloaded() {
            return !isOk();
        }

        void setOutput(String output) {
            this.output = output;
        }

        boolean hasOutput() {
            return output != null;
        }

        String getOutput() {
            return output;
        }

        @Override
        public String toString() {
            return String.format("RoomState{occupancy=%s, capacity=%s}", occupancy, capacity);
        }
    }

    static class RoomStateSerializer implements Serializer<RoomState> {
        @Override
        public byte[] serialize(String s, RoomState roomState) {
            if (roomState == null) return null;
            return String.format("%s,%s", roomState.occupancy, roomState.capacity).getBytes();
        }
    }

    static class RoomStateDeserializer implements Deserializer<RoomState> {
        @Override
        public RoomState deserialize(String s, byte[] bytes) {
            if (bytes == null) return null;
            String[] tokens = new String(bytes).split(",");
            Long occupancy = !tokens[0].equals("null") ? Long.valueOf(tokens[0]) : null;
            Long capacity = !tokens[1].equals("null") ? Long.valueOf(tokens[1]) : null;
            return new RoomState(occupancy, capacity);
        }
    }

    public static void main(String[] args) throws Exception {
        // do not modify the structure of the command line
        String bootstrapServers = args[0];
        String appName = args[1];
        String studentTopic = args[2];
        String classroomTopic = args[3];
        String outputTopic = args[4];
        String stateStoreDir = args[5];

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, appName);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.STATE_DIR_CONFIG, stateStoreDir);

        Serde<RoomState> stateSerde = Serdes.serdeFrom(new RoomStateSerializer(), new RoomStateDeserializer());

        StreamsBuilder builder = new StreamsBuilder();

        // student -> latest room
        KTable<String, String> students = builder.table(studentTopic);

        // room -> current capacity
        KTable<String, Long> roomCapacity = builder.<String, String>table(classroomTopic)
                .mapValues((ValueMapper<String, Long>) Long::valueOf);

        // room -> current occupancy
        KTable<String, Long> roomOccupancy = students
                .groupBy((student, room) -> KeyValue.pair(room, student))
                .count();

        /*
         * 1. Join current occupancy with current capacity,
         * 2. Group the joined stream by room,
         * 3. Accumulate a state for each room using aggregate(),
         * 4. Filter only the notable state changes (overloaded or OK) from the aggregation stream to the output topic.
         */
        roomOccupancy
                .outerJoin(roomCapacity, (occupancy, capacity) -> {
                    System.out.printf("joining on room: occupancy=%s, capacity=%s\n", occupancy, capacity);
                    return new RoomState(occupancy, capacity);
                }, Materialized.with(Serdes.String(), stateSerde))
                .toStream()
                .groupByKey()
                .aggregate(RoomState::new, (room, newState, prevState) -> {
                    System.out.printf("aggregate: room=%s, newState=%s prevState=%s\n", room, newState, prevState);
                    if (newState.isOverloaded()) {
                        newState.setOutput(String.valueOf(newState.occupancy));
                    } else if (newState.isOk() && prevState.isOverloaded()) {
                        newState.setOutput("OK");
                    }
                    return newState;
                }, Materialized.with(Serdes.String(), stateSerde))
                .toStream()
                .filter((room, state) -> state.hasOutput())
                .mapValues(RoomState::getOutput)
                .to(outputTopic);


        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // this line initiates processing
        streams.start();

        // shutdown hook for Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
