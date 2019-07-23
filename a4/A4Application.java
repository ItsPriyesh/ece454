import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.kafka.common.errors.SerializationException;
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

    public static class RoomState {

        public RoomState() {
        }

        public RoomState(long occupancy, long capacity) {
            this.occupancy = occupancy;
            this.capacity = capacity;
            System.out.println("Created RoomState " + toString());
        }

        public void setOccupancy(long occupancy) {
            this.occupancy = occupancy;
        }

        public void setCapacity(long capacity) {
            this.capacity = capacity;
        }

        long occupancy;
        long capacity;


        @Override
        public String toString() {
            return String.format("RoomState{occupancy=%s, capacity=%s}", occupancy, capacity);
        }
    }

    static class OccupancyCapacity {
        Long occupancy;
        Long capacity;

        public OccupancyCapacity() {
        }

        public OccupancyCapacity(Long occupancy, Long capacity) {
            this.occupancy = occupancy;
            this.capacity = capacity;
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

    static class JacksonSerializer<T> implements Serializer<T> {
        private final ObjectMapper mapper;

        JacksonSerializer() {
            this.mapper = new ObjectMapper();
            this.mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        }

        @Override
        public byte[] serialize(String s, T t) {
            if (t == null) return null;
            try {
                return mapper.writeValueAsBytes(t);
            } catch (Exception e) {
                throw new SerializationException(e);
            }
        }
    }

    static class JacksonDeserializer<T> implements Deserializer<T> {

        private final Class<T> valueType;
        private final ObjectMapper mapper;

        JacksonDeserializer(Class<T> valueType) {
            this.valueType = valueType;
            this.mapper = new ObjectMapper();
            this.mapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
        }

        @Override
        public T deserialize(String s, byte[] bytes) {
            if (bytes == null) return null;
            try {
                return mapper.treeToValue(mapper.readTree(bytes), valueType);
            } catch (Exception e) {
                throw new SerializationException(e);
            }
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

//        KTable<String, String> out = roomOccupancy.outerJoin(roomCapacity,
//                (occupancy, capacity) -> {
//                    System.out.printf("joining on room: occupancy=%s, capacity=%s\n", occupancy, capacity);
//                    if (occupancy != null && capacity != null) { // Room exists for student
//                        return occupancy > capacity ? String.valueOf(occupancy) : "OK";
//                    } else {
//                        return null;
//                    }
//                });

        Serde<RoomState> roomStateSerde = Serdes.serdeFrom(new RoomStateSerializer(), new RoomStateDeserializer());
        Serde<OccupancyCapacity> occCapSerde = Serdes.serdeFrom(new JacksonSerializer<>(), new JacksonDeserializer<>(OccupancyCapacity.class));

        final RoomState state = new RoomState();

        KTable<String, RoomState> c = roomOccupancy
                .outerJoin(roomCapacity, (occupancy, capacity) -> {
                    System.out.printf("joining on room: occupancy=%s, capacity=%s\n", occupancy, capacity);
                    return new OccupancyCapacity(occupancy, capacity);
//                    if (occupancy != null && capacity != null) { // Room exists for student
//                        return occupancy > capacity ? String.valueOf(occupancy) : "OK";
//                    } else {
//                        return null;
//                    }
                }, Materialized.with(Serdes.String(), occCapSerde))
                .toStream()
                .groupByKey()
                .aggregate(
                        () -> state, /* zero */
                        (room, newOccCap, roomState) -> {
                            System.out.printf("aggregate: room=%s, newOccupancy=%s, newCapacity=%s prevState=%s\n",
                                    room, newOccCap.occupancy, newOccCap.capacity, roomState.hashCode() + ", " + roomState.capacity + ", " + roomState.occupancy);

                            // TODO: WTF the aggregate state isn't changing?? But it is changing in the output??
                            // room=roomJ, newOccupancy=1, newCapacity=null state=RoomState{occupancy=0, capacity=0}
                            // output newCapacity if: newOccupancy > newOccupancy
                            // output OK if: newOccupancy <=

                            final Long occupancy = newOccCap.occupancy;
                            final Long capacity = newOccCap.capacity;

                            if (occupancy != null && capacity != null) {
                                // Room exists for student
                                // OK if prevState is overloaded and this state isnt
//                                if (occupancy > capacity) return;
                                roomState.setOccupancy(occupancy);
                                roomState.setCapacity(capacity);
                            } else if (occupancy != null) {
                                // Room doesn't exist (occupancy update only)
                                roomState.setOccupancy(occupancy);
                            } else if (capacity != null) {

                                // Student doesn't exist for room (room capacity update only)
                                roomState.setCapacity(capacity);
                            }
                            return roomState;
//                        },
//                        (room, oldOccCap, roomState) -> { /* subtractor */
//                            System.out.printf("aggregate subtract: room=%s, oldOccupancy=%s, oldCapacity=%s state=%s\n",
//                                    room, oldOccCap.occupancy, oldOccCap.capacity, roomState);
//                            return new RoomState();
                        },
                        Materialized.with(Serdes.String(), roomStateSerde)
                );


        // TODO: filter out (room, OK) messages when the occupancy or max capacity didnt actually change
        // ie: dont output OK every time a student gets added to a room that hasn't reached capacity yet
        // Maybe aggregate by room to hold state of last occupancy
        c.toStream().mapValues(RoomState::toString).filter((key, value) -> value != null).to(outputTopic);


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
