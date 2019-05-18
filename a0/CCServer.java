import java.io.*;
import java.net.*;
import java.util.Arrays;

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
                String response = makeResponse(connectedComponents(edges));
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

    private static String makeResponse(int[][] result) {
        final StringBuilder builder = new StringBuilder();
        for (int[] pair : result) {
            builder.append(pair[0]);
            builder.append(" ");
            builder.append(pair[1]);
        }
        return builder.toString();
    }

    private static int[][] connectedComponents(int[][] edges) {
        // ???
        return null;
    }
}
