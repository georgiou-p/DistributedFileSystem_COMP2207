import java.io.*;
import java.net.*;

public class Dstore {
  private final int port;
  private final int cport;
  private final int timeout;
  private final String fileFolder;
  private Socket controllerSocket;
  private ServerSocket serverSocket;
  private PrintWriter controllerOut;
  private BufferedReader controllerIn;

  public static void main(String[] args) {
    if (args.length != 4) {
      System.out.println("Usage: java Dstore port cport timeout file_folder");
      return;
    }

    int port = Integer.parseInt(args[0]);
    int cport = Integer.parseInt(args[1]);
    int timeout = Integer.parseInt(args[2]);
    String fileFolder = args[3];

    Dstore dstore = new Dstore(port, cport, timeout, fileFolder);
    dstore.start();
  }

  public Dstore(int port, int cport, int timeout, String fileFolder) {
    this.port = port;
    this.cport = cport;
    this.timeout = timeout;
    this.fileFolder = fileFolder;
  }

  public void start() {
    // Empty the file folder at startup
    emptyFolder();

    try {
      // Connect to Controller
      connectToController();

      // Start listening for connections from clients
      serverSocket = new ServerSocket(port);
      System.out.println("Dstore listening on port " + port);

      // Accept client connections in the main thread
      while (true) {
        Socket clientSocket = serverSocket.accept();
        System.out.println("New client connection from " + clientSocket.getInetAddress() + ":" + clientSocket.getPort());

        // Handle the client connection in a new thread
        new Thread(() -> handleClientConnection(clientSocket)).start();
      }

    } catch (IOException e) {
      System.err.println("Error in Dstore: " + e.getMessage());
      e.printStackTrace();
    }
  }

  private void connectToController() throws IOException {
    try {
      // Connect to Controller
      controllerSocket = new Socket("localhost", cport);
      System.out.println("Connected to Controller on port " + cport);

      // Set up streams to Controller
      controllerOut = new PrintWriter(controllerSocket.getOutputStream(), true);
      controllerIn = new BufferedReader(new InputStreamReader(controllerSocket.getInputStream()));

      // Send JOIN message
      controllerOut.println(Protocol.JOIN_TOKEN + " " + port);
      System.out.println("Sent JOIN message to Controller");

      // Start a separate thread to handle Controller messages
      new Thread(this::handleControllerMessages).start();

    } catch (IOException e) {
      System.err.println("Failed to connect to Controller: " + e.getMessage());
      throw e;
    }
  }

  private void handleControllerMessages() {
    try {
      controllerSocket.setSoTimeout(timeout); // Set socket timeout
      String message;
      while (true) {
        try {
          message = controllerIn.readLine();
          if (message == null) {
            System.out.println("Controller connection closed.");
            break;
          }

          System.out.println("Received from Controller: " + message);

          // Parse the command - split by space and get the first part
          String[] parts = message.split(" ", 2);
          String command = parts[0];

          // Use equals for exact command matching
          if (command.equals(Protocol.LIST_TOKEN)) {
            handleListRequest();
          } else if (command.equals(Protocol.REMOVE_TOKEN)) {
            handleRemoveRequest(message);
          } else if (command.equals(Protocol.REBALANCE_TOKEN)) {
            handleRebalanceRequest(message);
          } else {
            System.out.println("Unknown command from Controller: " + message);
          }
        } catch (SocketTimeoutException e) {
          // This is normal - just continue waiting
          // Reducing noise in the logs by not printing every timeout
          continue;
        }
      }
    } catch (IOException e) {
      System.err.println("Error handling Controller messages: " + e.getMessage());
      System.out.println("Connection to Controller lost");
    }
  }

  private void handleListRequest() {
    // Get list of files in the folder
    File folder = new File(fileFolder);
    File[] files = folder.listFiles();

    StringBuilder fileList = new StringBuilder(Protocol.LIST_TOKEN + " ");
    if (files != null) {
      for (File file : files) {
        if (file.isFile()) {
          fileList.append(file.getName()).append(" ");
        }
      }
    }

    // Send the list to Controller
    controllerOut.println(fileList.toString().trim());
    System.out.println("Sent file list to Controller");
  }

  private void handleRemoveRequest(String message) {
    String[] parts = message.split(" ");
    if (parts.length != 2) {
      System.err.println("Invalid REMOVE message: " + message);
      return;
    }

    String filename = parts[1];
    File file = new File(fileFolder + File.separator + filename);

    if (file.exists()) {
      if (file.delete()) {
        controllerOut.println(Protocol.REMOVE_ACK_TOKEN + " " + filename);
        System.out.println("File " + filename + " removed successfully");
      } else {
        System.err.println("Failed to remove file " + filename);
      }
    } else {
      controllerOut.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN + " " + filename);
      System.out.println("File " + filename + " does not exist");
    }
  }

  private void handleRebalanceRequest(String message) {
    // TODO: Implement rebalance handling
    controllerOut.println(Protocol.REBALANCE_COMPLETE_TOKEN);
  }

  private void handleClientConnection(Socket clientSocket) {
    try {
      BufferedReader in = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
      PrintWriter out = new PrintWriter(clientSocket.getOutputStream(), true);

      String message = in.readLine();
      if (message == null) return;

      System.out.println("Received from Client: " + message);

      if (message.startsWith(Protocol.STORE_TOKEN)) {
        // Handle STORE request from client
        handleStoreRequest(message, in, out, clientSocket);
      } else if (message.startsWith(Protocol.LOAD_DATA_TOKEN)) {
        // Handle LOAD_DATA request from client
        handleLoadDataRequest(message, out, clientSocket);
      } else if (message.startsWith(Protocol.REBALANCE_STORE_TOKEN)) {
        // Handle REBALANCE_STORE request (for future implementation)
        handleRebalanceStoreRequest(message, in, out, clientSocket);
      }

    } catch (IOException e) {
      System.err.println("Error handling client connection: " + e.getMessage());
    } finally {
      try {
        clientSocket.close();
      } catch (IOException e) {
        System.err.println("Error closing client socket: " + e.getMessage());
      }
    }
  }

  private void handleStoreRequest(String message, BufferedReader in, PrintWriter out, Socket clientSocket) throws IOException {
    // Parse STORE message: STORE filename filesize
    String[] parts = message.split(" ");
    if (parts.length != 3) {
      System.err.println("Invalid STORE message: " + message);
      return;
    }
    String filename = parts[1];
    int filesize = Integer.parseInt(parts[2]);
    // Send ACK
    out.println(Protocol.ACK_TOKEN);
    System.out.println("Sent ACK for STORE of " + filename);
    // Receive file content
    byte[] fileContent = new byte[filesize];
    InputStream inputStream = clientSocket.getInputStream();
    int bytesRead = 0;
    int totalBytesRead = 0;
    while (totalBytesRead < filesize) {
      bytesRead = inputStream.read(fileContent, totalBytesRead, filesize - totalBytesRead);
      if (bytesRead == -1) break;
      totalBytesRead += bytesRead;
    }
    System.out.println("Received " + totalBytesRead + " bytes for file " + filename);
    // Save file
    File file = new File(fileFolder + File.separator + filename);
    try (FileOutputStream fos = new FileOutputStream(file)) {
      fos.write(fileContent, 0, totalBytesRead);
    }
    // Notify Controller
    controllerOut.println(Protocol.STORE_ACK_TOKEN + " " + filename);
    System.out.println("Sent STORE_ACK to Controller for " + filename);
  }

  private void handleLoadDataRequest(String message, PrintWriter out, Socket clientSocket) throws IOException {
    // Parse LOAD_DATA message: LOAD_DATA filename
    String[] parts = message.split(" ");
    if (parts.length != 2) {
      System.err.println("Invalid LOAD_DATA message: " + message);
      return;
    }
    String filename = parts[1];
    File file = new File(fileFolder + File.separator + filename);
    if (!file.exists()) {
      // Close connection if file doesn't exist (as per specification)
      System.err.println("File " + filename + " not found for LOAD_DATA request");
      return;
    }
    System.out.println("Sending file " + filename + " to client");
    // Send file content
    try (FileInputStream fis = new FileInputStream(file)) {
      OutputStream outputStream = clientSocket.getOutputStream();
      byte[] buffer = new byte[8192];
      int bytesRead;

      while ((bytesRead = fis.read(buffer)) != -1) {
        outputStream.write(buffer, 0, bytesRead);
      }
      outputStream.flush();
      System.out.println("File " + filename + " sent successfully to client");
    } catch (IOException e) {
      System.err.println("Error sending file " + filename + ": " + e.getMessage());
      throw e;
    }
  }

  private void handleRebalanceStoreRequest(String message, BufferedReader in, PrintWriter out, Socket clientSocket) throws IOException {
    // TODO: Implement rebalance store request handling
    System.out.println("Rebalance store request not implemented yet");
  }

  private void emptyFolder() {
    File folder = new File(fileFolder);
    if (folder.exists() && folder.isDirectory()) {
      File[] files = folder.listFiles();
      if (files != null) {
        for (File file : files) {
          if (file.isFile() && !file.delete()) {
            System.err.println("Failed to delete file: " + file.getName());
          }
        }
      }
      System.out.println("Folder " + fileFolder + " emptied");
    } else {
      if (!folder.mkdirs()) {
        System.err.println("Failed to create folder: " + fileFolder);
      } else {
        System.out.println("Folder " + fileFolder + " created");
      }
    }
  }
}