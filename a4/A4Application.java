import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import javassist.bytecode.ByteArray;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;

import java.util.*;


public class A4Application {

   public static class RoomState {

        long maxCapacity;

        final Set<String> students;

        RoomState() {
            this.students = new HashSet<>();
            this.maxCapacity = 0;
        }

        RoomState(Set<String> students, long maxCapacity) {
            this.students = students;
            this.maxCapacity = maxCapacity;
        }

        int currentCapacity() {
            return students.size();
        }

        RoomState enter(String student) {
            students.add(student);
            return new RoomState(students, maxCapacity);
        }

        RoomState exit(String student) {
            students.remove(student);
            return new RoomState(students, maxCapacity);
        }

        RoomState setMaxCapacity(long capacity) {
            return new RoomState(students, capacity);
        }

        @Override
        public String toString() {
            return String.format("maxCapacity=%s, students=%s", maxCapacity, students.toString());
        }
    }

    static class RoomStateSerializer implements Serializer<RoomState> {

        private final ObjectMapper mapper = new ObjectMapper();
        RoomStateSerializer() {
            mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        }

        @Override
        public byte[] serialize(String s, RoomState roomState) {
            if (Objects.isNull(roomState)) return null;
            try {
                return mapper.writeValueAsBytes(roomState);
            } catch (Exception e) {
                throw new SerializationException(e);
            }
        }
    }

    static class RoomStateDeserializer implements Deserializer<RoomState> {

        private final ObjectMapper mapper = new ObjectMapper();

        RoomStateDeserializer() {
            mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);

        }
        @Override
        public RoomState deserialize(String s, byte[] bytes) {
            if (Objects.isNull(bytes)) return null;
            try {
                return mapper.treeToValue(mapper.readTree(bytes), RoomState.class);
            } catch (Exception e) {
                throw new SerializationException(e);
            }
        }
    }

    static class StudentCapacity {
        final String student;
        final long capacity;

        StudentCapacity(String student, long capacity) {
            this.student = student;
            this.capacity = capacity;
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

        // add code here if you need any additional configuration options

        StreamsBuilder builder = new StreamsBuilder();
//
//        builder.<String, String>stream(classroomTopic)
//                .groupByKey()
//                .count()
//                .toStream()
//                .map((key, value) -> KeyValue.pair(key, String.valueOf(value)))
//                .to(outputTopic);


        KTable<String, Long> roomMax = builder.<String, String>table(classroomTopic)
                .mapValues((ValueMapper<String, Long>) Long::valueOf);

        KStream<String, String> students = builder.stream(studentTopic);

        Serde<RoomState> roomStateSerde = Serdes.serdeFrom(new RoomStateSerializer(), new RoomStateDeserializer());


        KTable<String, RoomState> aggregate = students.selectKey((student, room) -> room)
                .join(roomMax, StudentCapacity::new)
                .groupByKey()
                .<RoomState>aggregate(RoomState::new, (key, value, state) -> {
                    System.out.printf("aggregate: k=%s, v=%s, state=%s\n", key, value.capacity + " " + value.student, state);
                    return state.enter(value.student);
                }, Materialized.<String, RoomState, KeyValueStore<Bytes, byte[]>>as("aggregated-stream-store")
                        .withValueSerde(roomStateSerde));
//                .aggregate(RoomState::new, (room, studentCapacity, state) ->
//                        state.enter(studentCapacity.student)
//                )
//                .toStream()
//                .to(outputTopic);

//        rooms.join(students)
//                .groupByKey()
//                .aggregate(RoomState::new, (key, value, aggregate) -> null)


//                .reduce(new Reducer<String>() {
//            @Override
//            public String apply(String value1, String value2) {
//                return value1 + value2;
//            }
//        }).toStream().to(outputTopic);

        /**
         * TODO: Window the output to 1 second intervals b/c of this?
         *
         * 3. The application should be configured so that each input
         message is consumed and processed in around one second or
         less. For example, if the occupancy of a room increases beyond
         the maximum capacity then the corresponding output record
         should be committed to the output topic within around one
         second. When the grading script commits a message to one of
         the input topics, it will wait up to five seconds for the
         application to commit a message to the output topic.
         */
        // add code here
        //
        // ... = builder.stream(studentTopic);
        // ... = builder.stream(classroomTopic);
        // ...
        // ...to(outputTopic);


        KafkaStreams streams = new KafkaStreams(builder.build(), props);

        // this line initiates processing
        streams.start();

        // shutdown hook for Ctrl+C
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
