import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.List;
import java.util.StringTokenizer;
import java.util.ArrayList;


class CCServer {

    public static void main(String args[]) throws Exception {
        if (args.length != 1) {
            System.out.println("usage: java CCServer port");
            System.exit(-1);
        }
        int port = Integer.parseInt(args[0]);

        ServerSocket ssock = new ServerSocket(port);
        System.out.println("listening on port " + port);
        while (true) {
            try {
		/*
		  YOUR CODE GOES HERE
		  - accept connection from sezrver socket
		  - read requests from connection repeatedly
		  - for each request, compute an output and send a response
		  - each message has a 4-byte header followed by a payload
		  - the header is the length of the payload
		    (signed, two's complement, big-endian)
		  - the payload is a string
		    (UTF-8, big-endian)
		*/
                Socket client = ssock.accept();

                DataInputStream in = new DataInputStream(client.getInputStream());
                final int size = in.readInt();
                final byte[] inBytes = new byte[size];
                in.readFully(inBytes);

                initUnionFind(inBytes);
                String response = connectedComponents();
                final byte[] outBytes = response.getBytes("UTF-8");

                DataOutputStream out = new DataOutputStream(client.getOutputStream());
                out.writeInt(outBytes.length);
                out.write(outBytes);
                out.flush();

                out.close();
                in.close();
                client.close();

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

    }

    static  final class Component {
        int parent;
        int rank;

        Component(int parent, int rank) {
            this.parent = parent;
            this.rank = rank;
        }
    }


    private static final Map<Integer, Component> components = new HashMap<>();

    static void initUnionFind(byte[] inBytes) {
        components.clear();
        int num1 = 0, num2 = 0;
        for (byte b : inBytes){
            if (b == 32){
                num2 = num1;
                num1 = 0;
            }

            else if (b == 10){
                union(num2, num1);
                num2 = 0;
                num1 = 0;
            }

            else{
                num1 = (b - 48) + num1*10;
            }
        }
    }

    static int find(int i) {
        if (components.get(i).parent != i) {
            // Path compression - set this nodes parent to its parents parent
            components.get(i).parent = find(components.get(i).parent);
        }
        return components.get(i).parent;
    }

    static void union(int a, int b) {
        components.putIfAbsent(a, new Component(a, 0));
        components.putIfAbsent(b, new Component(b, 0));

        int rootA = find(a);
        int rootB = find(b);

        // Union by rank
        if (components.get(rootA).rank < components.get(rootB).rank) {
            components.get(rootA).parent = rootB;
        } else if (components.get(rootA).rank > components.get(rootB).rank) {
            components.get(rootB).parent = rootA;
        } else {
            components.get(rootA).parent = rootB;
            components.get(rootB).rank++;
        }
    }

    static String connectedComponents() {
        StringBuilder builder = new StringBuilder();
        for (int key : components.keySet()) {
            builder.append(key);
            builder.append(" ");
            builder.append(find(key));
            builder.append("\n");
        }
        return builder.toString();
    }
}