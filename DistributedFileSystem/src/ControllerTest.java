import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class ControllerTest {
  public static void main(String[] args) {
    if (args.length != 4) {
      System.out.println("Usage: java ControllerTest cport R timeout rebalance_period");
      return;
    }

    int cport = Integer.parseInt(args[0]);
    int r = Integer.parseInt(args[1]);
    int timeout = Integer.parseInt(args[2]);

    try {
      ServerSocket serverSocket = new ServerSocket(cport);
      System.out.println("Test Controller started on port " + cport);

      while (true) {
        Socket socket = serverSocket.accept();
        System.out.println("New connection from " + socket.getInetAddress() + ":" + socket.getPort());

        // Handle connection in new thread
        new Thread(() -> {
          try {
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true);

            String message;
            while ((message = in.readLine()) != null) {
              System.out.println("Received: " + message);

              // Handle different message types
              if (message.equals("LIST")) {
                out.println("LIST");
              } else if (message.startsWith("STORE ")) {
                // Send STORE_TO message
                out.println("STORE_TO 12346 12347 12348");
                System.out.println("Sent STORE_TO response");
              } else if (message.startsWith("STORE_ACK ")) {
                System.out.println("Got STORE_ACK, sending STORE_COMPLETE");
                // Send STORE_COMPLETE after receiving all 3 ACKs
                if (message.contains("TestFile.txt")) {
                  out.println("STORE_COMPLETE");
                }
              }
            }
          } catch (IOException e) {
            e.printStackTrace();
          }
        }).start();
      }
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
}