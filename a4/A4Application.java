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
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.state.KeyValueStore;

import java.time.Duration;
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

        boolean contains(String student) {
            return students.contains(student);
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

        /*
          roomCapacity:
          RoomA,2
          RoomB,2

          students:
          Student1,RoomA
          Student2,RoomA

          roomOccupancy:
          RoomA,2

         */

        // student -> latest room
        KTable<String, String> students = builder.table(studentTopic);

        // room -> latest max capacity
        KTable<String, Long> roomCapacity = builder.<String, String>table(classroomTopic)
                .mapValues((ValueMapper<String, Long>) Long::valueOf);

        // Current occupancy table: roomID, occupancy
        KTable<String, Long> roomOccupancy = students
                .groupBy((student, room) -> KeyValue.pair(room, student))
                .count();

        KTable<String, String> out = roomOccupancy.outerJoin(roomCapacity,
                (occupancy, capacity) -> {
                    System.out.printf("joining on room: occupancy=%s, capacity=%s\n", occupancy, capacity);
                    if (occupancy != null && capacity != null) { // Room exists for student
                        return occupancy > capacity ? String.valueOf(occupancy) : "OK";
                    } else {
                        return null;
                    }
                });

        // TODO: filter out (room, OK) messages when the occupancy or max capacity didnt actually change
        // ie: dont output OK every time a student gets added to a room that hasn't reached capacity yet
        out.toStream().filter((key, value) -> {
            System.out.printf("filter: key=%s, val=%s\n", key, value);
            return value != null;
        }).to(outputTopic);


//
//        builder.<String, String>stream(classroomTopic)
//                .groupByKey()
//                .count()
//                .toStream()
//                .map((key, value) -> KeyValue.pair(key, String.valueOf(value)))
//                .to(outputTopic);


//        KTable<String, Long> roomCapacity = builder.<String, String>table(classroomTopic)
//                .mapValues((ValueMapper<String, Long>) Long::valueOf);

//        KStream<String, String> students = builder.stream(studentTopic); // StudentId,RoomID
//        KStream<String, String> rooms = builder.t(classroomTopic); // RoomId,MaxCapacity

//        Serde<RoomState> roomStateSerde = Serdes.serdeFrom(new RoomStateSerializer(), new RoomStateDeserializer());

//        roomTable.outerJoin(studentRoomTable.selectKey((student, room) -> room), (max, student) -> {
//
//        }, JoinWindows.of(Duration.ofSeconds(1)));
//        students.map((student, room) -> KeyValue.pair(room, student))
//                .outerJoin(rooms, (student, roomCap) -> {
//                    if (student == null) return null;
//                    return student;
//                    // whatever stream just emitted is the one with a non-null value
////                    System.out.printf("joining: student = %s, cap = %s\n", student, roomCap);
////                    return KeyValue.pair(student, roomCap != null ? Long.valueOf(roomCap) : null);
//                }, JoinWindows.of(Duration.ofSeconds(1)))
//                .groupByKey();
//                .mapValues(KeyValue::toString)
//                .groupByKey()
//                .aggregate(RoomState::new, (room, studentOrCapacity, roomState) -> {
//                    String student = studentOrCapacity.key;
//                    Long capacity = studentOrCapacity.value;
//                    // Assert that at least one is non-null
//                    assert !(student == null && capacity == null);
//
//                    if (student != null) { // Student entering the room
//                        if (roomState.contains(student)) {
//
//                        }
//                    } else { // Room capacity updated
//
//                    }
//                }, Materialized.with(Serdes.String(), roomStateSerde))
//                .reduce(new Reducer<String>() {
//                    @Override
//                    public String apply(String value1, String value2) {
//                        return value1 + ", " + value2;
//                    }
//                })
//                .toStream()
//                .to(outputTopic);


//        KStream<String, String> rooms = builder.stream(classroomTopic);
//        students.join(rooms, (l, r) -> l + "/" + r, JoinWindows.of(Duration.ofSeconds(1))).to(outputTopic);

//        builder.stream(classroomTopic).join(builder.stream(studentTopic), (a, b) -> a + " || " + b, JoinWindows.of(Duration.ofSeconds(1)))
//                .to(outputTopic);
//        students
//                .selectKey((student, room) -> room)
//                .join(roomCapacity, StudentCapacity::new)
//                .groupByKey()
//                .aggregate(RoomState::new, (key, value, state) -> {
//                    System.out.printf("aggregate: k=%s, v=%s, state=%s\n", key, value.capacity + " " + value.student, state);
//                    return state.enter(value.student);
//                }, Materialized.with(Serdes.String(), roomStateSerde))
//                .toStream()
//                .to(outputTopic);

//        aggregate.toStream().mapValues(RoomState::toString).to(outputTopic);
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
