import java.io.*;
import java.net.*;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

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
		  - accept connection from server socket
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

                final int[][] edges = parseRequest(new String(inBytes));
                UnionFind unionFind = new UnionFind(edges);
                String response = unionFind.connectedComponents();
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

    /*
     * TODO: this adds potentially unnecessary overhead, might wanna just build
     *       the union find directly from the string instead of creating an array
     */
    private static int[][] parseRequest(String request) {
        final String[] lines = request.split("\n");
        final int[][] edges = new int[lines.length][2];
        for (int i = 0; i < lines.length; i++) {
            final String[] edge = lines[i].split(" ");
            edges[i][0] = Integer.parseInt(edge[0]);
            edges[i][1] = Integer.parseInt(edge[1]);
        }
        return edges;
    }

    private static final class UnionFind {

        // TODO: might want a 2 element array instead of an object
        private final class Component {
            int parent;
            int rank;

            Component(int parent, int rank) {
                this.parent = parent;
                this.rank = rank;
            }
        }

        private final Map<Integer, Component> components = new HashMap<>();
        private final Set<Integer> vertices = new HashSet<>();

        UnionFind(int[][] edges) {
            // TODO might not need to parse into 2d array intermediate form
            // TODO this is sus
            for (int[] edge : edges) {
                vertices.add(edge[0]);
                vertices.add(edge[1]);
            }
            for (int vertex : vertices) {
                components.put(vertex, new Component(vertex, 0));
            }
            // TODO looping through this twice kinda sus
            for (int[] edge : edges) {
                union(edge[0], edge[1]);
            }
        }

        // TODO memoize this?
        int find(int i) {
            if (components.get(i).parent != i) {
                // Path compression - set this nodes parent to its parents parent
                components.get(i).parent = find(components.get(i).parent);
            }
            return components.get(i).parent;
        }

        void union(int a, int b) {
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

        String connectedComponents() {
            StringBuilder builder = new StringBuilder();
            for (int vertex : vertices) {
                builder.append(vertex);
                builder.append(" ");
                builder.append(find(vertex));
                builder.append("\n");
            }
            return builder.toString();
        }
    }
}
