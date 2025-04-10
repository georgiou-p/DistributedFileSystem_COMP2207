import java.io.*;
import java.net.*;
import java.util.*;
import java.util.concurrent.*;

public class Controller {
  private final int cport;
  private final int r;
  private final int timeout;
  private final int rebalancePeriod;
  private ServerSocket serverSocket;

  // Map to keep track of connected Dstores: port -> socket
  private final ConcurrentHashMap<Integer, Socket> dstores = new ConcurrentHashMap<>();
  // Map to track the output streams for each Dstore
  private final ConcurrentHashMap<Integer, PrintWriter> dstoreWriters = new ConcurrentHashMap<>();

  // Index structure to track files and their states
  private final ConcurrentHashMap<String, FileInfo> index = new ConcurrentHashMap<>();

  // Class to represent file information in the index
  private static class FileInfo {
    public enum State {
      STORE_IN_PROGRESS,
      STORE_COMPLETE,
      REMOVE_IN_PROGRESS
    }
    private State state;
    private final long size;
    private final Set<Integer> dstorePorts;

    public FileInfo(State state, long size) {
      this.state = state;
      this.size = size;
      this.dstorePorts = new HashSet<>();
    }

    public void addDstorePort(int port) {
      dstorePorts.add(port);
    }

    public Set<Integer> getDstorePorts() {
      return new HashSet<>(dstorePorts);
    }

    public State getState() {
      return state;
    }

    public void setState(State state) {
      this.state = state;
    }

    public long getSize() {
      return size;
    }
  }

  // Class to track store operations
  private class StoreOperation {
    private final String filename;
    private final Set<Integer> dstorePorts;
    private final Set<Integer> acknowledgedPorts;
    private final PrintWriter clientWriter;
    private final Timer timeoutTimer;

    public StoreOperation(String filename, Set<Integer> dstorePorts, PrintWriter clientWriter) {
      this.filename = filename;
      this.dstorePorts = dstorePorts;
      this.clientWriter = clientWriter;
      this.acknowledgedPorts = new HashSet<>();

      // Set up timer for timeout
      timeoutTimer = new Timer();
      timeoutTimer.schedule(new TimerTask() {
        @Override
        public void run() {
          handleTimeout();
        }
      }, timeout);
    }

    public synchronized void addAcknowledgement(int port) {
      System.out.println("Adding acknowledgement from port " + port + " for file " + filename);
      acknowledgedPorts.add(port);

      // Check if all acknowledgements received
      if (acknowledgedPorts.size() == dstorePorts.size()) {
        completeOperation();
      }
    }

    private void completeOperation() {
      System.out.println("All acknowledgements received for " + filename + ", completing store operation");
      timeoutTimer.cancel();
      // Update index
      FileInfo fileInfo = index.get(filename);
      if (fileInfo != null) {
        fileInfo.setState(FileInfo.State.STORE_COMPLETE);
        for (int port : dstorePorts) {
          fileInfo.addDstorePort(port);
        }
      }
      // Send STORE_COMPLETE to client
      clientWriter.println(Protocol.STORE_COMPLETE_TOKEN);
      System.out.println("Sent STORE_COMPLETE to client for " + filename);
      // Remove from active operations
      storeOperations.remove(filename);
    }
    private void handleTimeout() {
      System.out.println("Timeout occurred for store operation of " + filename);

      // Remove from index and active operations
      index.remove(filename);
      storeOperations.remove(filename);
    }
  }

  // Class to track remove operations
  private class RemoveOperation {
    private final String filename;
    private final Set<Integer> dstorePorts;
    private final Set<Integer> acknowledgedPorts;
    private final PrintWriter clientWriter;
    private final Timer timeoutTimer;

    public RemoveOperation(String filename, Set<Integer> dstorePorts, PrintWriter clientWriter) {
      this.filename = filename;
      this.dstorePorts = dstorePorts;
      this.clientWriter = clientWriter;
      this.acknowledgedPorts = new HashSet<>();

      // Set up timer for timeout
      timeoutTimer = new Timer();
      timeoutTimer.schedule(new TimerTask() {
        @Override
        public void run() {
          handleTimeout();
        }
      }, timeout);
    }

    public synchronized void addAcknowledgement(int port) {
      System.out.println("Adding removal acknowledgement from port " + port + " for file " + filename);
      acknowledgedPorts.add(port);

      // Check if all acknowledgements received
      if (acknowledgedPorts.size() == dstorePorts.size()) {
        completeOperation();
      }
    }

    private void completeOperation() {
      System.out.println("All removal acknowledgements received for " + filename + ", completing remove operation");
      timeoutTimer.cancel();

      // Remove file from the index
      index.remove(filename);

      // Send REMOVE_COMPLETE to client
      clientWriter.println(Protocol.REMOVE_COMPLETE_TOKEN);
      System.out.println("Sent REMOVE_COMPLETE to client for " + filename);

      // Remove from active operations
      removeOperations.remove(filename);
    }

    private void handleTimeout() {
      System.out.println("Timeout occurred for remove operation of " + filename);

      // File will remain in "remove in progress" state in the index
      // Future rebalances will try to ensure no Dstore stores this file

      // Remove from active operations
      removeOperations.remove(filename);
    }
  }

  // Map to track active store operations
  private final ConcurrentHashMap<String, StoreOperation> storeOperations = new ConcurrentHashMap<>();

  // Map to track active remove operations
  private final ConcurrentHashMap<String, RemoveOperation> removeOperations = new ConcurrentHashMap<>();

  public static void main(String[] args) {
    if (args.length != 4) {
      System.out.println("Usage: java Controller cport R timeout rebalance_period");
      return;
    }

    int cport = Integer.parseInt(args[0]);
    int r = Integer.parseInt(args[1]);
    int timeout = Integer.parseInt(args[2]);
    int rebalancePeriod = Integer.parseInt(args[3]);

    Controller controller = new Controller(cport, r, timeout, rebalancePeriod);
    controller.start();
  }

  public Controller(int cport, int r, int timeout, int rebalancePeriod) {
    this.cport = cport;
    this.r = r;
    this.timeout = timeout;
    this.rebalancePeriod = rebalancePeriod;
  }

  public void start() {
    try {
      serverSocket = new ServerSocket(cport);
      System.out.println("Controller started on port " + cport);
      System.out.println("Replication factor (R): " + r);
      System.out.println("Waiting for connections...");

      while (true) {
        Socket socket = serverSocket.accept();
        System.out.println("New connection from " + socket.getInetAddress() + ":" + socket.getPort());

        new Thread(new ConnectionHandler(socket)).start();
      }
    } catch (IOException e) {
      System.err.println("Error starting controller: " + e.getMessage());
      e.printStackTrace();
    }
  }

  // Check if enough Dstores are connected
  private boolean enoughDstores() {
    return dstores.size() >= r;
  }

  // Select R Dstores for storing a file
  private Set<Integer> selectDstoresForStore() {
    if (dstores.size() < r) {
      return null;
    }

    // Simple selection - just pick the first R Dstores
    // In a more sophisticated implementation, you would balance load
    return new HashSet<>(new ArrayList<>(dstores.keySet()).subList(0, r));
  }

  // Helper method to select a Dstore for loading
  private int selectDstoreForLoad(Set<Integer> availablePorts) {
    // Filter to only include currently connected Dstores
    Set<Integer> activePorts = new HashSet<>();
    for (int port : availablePorts) {
      if (dstores.containsKey(port)) {
        activePorts.add(port);
      }
    }

    if (activePorts.isEmpty()) {
      return -1;
    }

    // Select one randomly
    int index = new Random().nextInt(activePorts.size());
    return new ArrayList<>(activePorts).get(index);
  }

  private class ConnectionHandler implements Runnable {
    private final Socket socket;
    private BufferedReader in;
    private PrintWriter out;

    public ConnectionHandler(Socket socket) {
      this.socket = socket;
      try {
        in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
        out = new PrintWriter(socket.getOutputStream(), true);
      } catch (IOException e) {
        System.err.println("Error setting up streams: " + e.getMessage());
      }
    }

    @Override
    public void run() {
      try {
        String message;
        while ((message = in.readLine()) != null) {
          System.out.println("Received: " + message);

          // Parse the command - split by space and get the first part
          String[] parts = message.split(" ", 2);
          String command = parts[0];

          // Use equals() for exact command matching
          if (command.equals(Protocol.STORE_ACK_TOKEN)) {
            handleStoreAck(message);
          } else if (command.equals(Protocol.REMOVE_ACK_TOKEN)) {
            handleRemoveAck(message);
          } else if (command.equals(Protocol.STORE_TOKEN)) {
            handleStore(message);
          } else if (command.equals(Protocol.JOIN_TOKEN)) {
            handleJoin(message);
          } else if (command.equals(Protocol.LIST_TOKEN)) {
            handleList();
          } else if (command.equals(Protocol.LOAD_TOKEN)) {
            handleLoad(message);
          } else if (command.equals(Protocol.REMOVE_TOKEN)) {
            handleRemove(message);
          } else if (command.equals(Protocol.RELOAD_TOKEN)) {
            handleReload(message);
          } else {
            System.out.println("Unknown command: " + message);
          }
        }
      } catch (IOException e) {
        handleDisconnect();
      } finally {
        try {
          socket.close();
        } catch (IOException e) {
          System.err.println("Error closing socket: " + e.getMessage());
        }
      }
    }

    private void handleJoin(String message) {
      String[] parts = message.split(" ");
      if (parts.length != 2) {
        System.err.println("Invalid JOIN message: " + message);
        return;
      }

      int dstorePort = Integer.parseInt(parts[1]);
      dstores.put(dstorePort, socket);
      dstoreWriters.put(dstorePort, out);

      System.out.println("Dstore joined on port " + dstorePort);
      System.out.println("Total Dstores connected: " + dstores.size());

      if (dstores.size() >= r) {
        System.out.println("System now has " + dstores.size() + " Dstores (>= R=" + r + ")");
        System.out.println("Ready to serve client requests");
      }
    }

    private void handleStore(String message) {
      if (!enoughDstores()) {
        out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        return;
      }

      String[] parts = message.split(" ");
      if (parts.length != 3) {
        System.err.println("Invalid STORE message: " + message);
        return;
      }

      String filename = parts[1];
      long filesize = Long.parseLong(parts[2]);

      // Check if file already exists
      if (index.containsKey(filename)) {
        out.println(Protocol.ERROR_FILE_ALREADY_EXISTS_TOKEN);
        return;
      }

      // Add to index as "store in progress"
      index.put(filename, new FileInfo(FileInfo.State.STORE_IN_PROGRESS, filesize));

      // Select R Dstores
      Set<Integer> selectedDstores = selectDstoresForStore();

      // Create store operation
      StoreOperation storeOp = new StoreOperation(filename, selectedDstores, out);
      storeOperations.put(filename, storeOp);

      // Build STORE_TO message
      StringBuilder storeToMsg = new StringBuilder(Protocol.STORE_TO_TOKEN);
      for (int port : selectedDstores) {
        storeToMsg.append(" ").append(port);
      }

      // Send STORE_TO message to client
      out.println(storeToMsg.toString());
      System.out.println("Sent STORE_TO message for " + filename + " to client");
    }

    private void handleStoreAck(String message) {
      String[] parts = message.split(" ");
      if (parts.length != 2) {
        System.err.println("Invalid STORE_ACK message: " + message);
        return;
      }

      String filename = parts[1];
      System.out.println("Processing STORE_ACK for " + filename);

      // Get the store operation
      StoreOperation storeOp = storeOperations.get(filename);
      if (storeOp == null) {
        System.err.println("No active store operation for " + filename);
        return;
      }

      // Find which Dstore sent this message
      int dstorePort = -1;
      for (Map.Entry<Integer, Socket> entry : dstores.entrySet()) {
        if (entry.getValue() == socket) {
          dstorePort = entry.getKey();
          break;
        }
      }

      if (dstorePort == -1) {
        System.err.println("Could not identify Dstore for STORE_ACK");
        return;
      }

      // Add acknowledgement
      storeOp.addAcknowledgement(dstorePort);
    }

    private void handleList() {
      if (!enoughDstores()) {
        out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        return;
      }

      // Build list of completed files
      StringBuilder fileList = new StringBuilder(Protocol.LIST_TOKEN);
      for (Map.Entry<String, FileInfo> entry : index.entrySet()) {
        if (entry.getValue().getState() == FileInfo.State.STORE_COMPLETE) {
          fileList.append(" ").append(entry.getKey());
        }
      }

      out.println(fileList.toString());
    }

    private void handleLoad(String message) {
      if (!enoughDstores()) {
        out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        return;
      }

      // Parse the load request: LOAD filename
      String[] parts = message.split(" ");
      if (parts.length != 2) {
        System.err.println("Invalid LOAD message: " + message);
        return;
      }

      String filename = parts[1];

      // Check if file exists and is in STORE_COMPLETE state
      FileInfo fileInfo = index.get(filename);
      if (fileInfo == null || fileInfo.getState() != FileInfo.State.STORE_COMPLETE) {
        out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
        return;
      }

      // Get the set of Dstores that have this file
      Set<Integer> dstorePorts = fileInfo.getDstorePorts();
      if (dstorePorts.isEmpty()) {
        out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
        return;
      }

      // Select one Dstore randomly
      int selectedPort = selectDstoreForLoad(dstorePorts);
      if (selectedPort == -1) {
        out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
        return;
      }

      // Send LOAD_FROM message to client
      out.println(Protocol.LOAD_FROM_TOKEN + " " + selectedPort + " " + fileInfo.getSize());
      System.out.println("Sent LOAD_FROM for " + filename + " directing to Dstore port " + selectedPort);
    }

    private void handleRemove(String message) {
      if (!enoughDstores()) {
        out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        return;
      }

      // Parse the remove request: REMOVE filename
      String[] parts = message.split(" ");
      if (parts.length != 2) {
        System.err.println("Invalid REMOVE message: " + message);
        return;
      }

      String filename = parts[1];

      // Check if file exists and is in STORE_COMPLETE state
      FileInfo fileInfo = index.get(filename);
      if (fileInfo == null || fileInfo.getState() != FileInfo.State.STORE_COMPLETE) {
        out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
        return;
      }

      // Update index state to "remove in progress"
      fileInfo.setState(FileInfo.State.REMOVE_IN_PROGRESS);

      // Get the set of Dstores that have this file
      Set<Integer> dstorePorts = fileInfo.getDstorePorts();

      // Filter to only include currently connected Dstores
      Set<Integer> activePorts = new HashSet<>();
      for (int port : dstorePorts) {
        if (dstores.containsKey(port)) {
          activePorts.add(port);
        }
      }

      if (activePorts.isEmpty()) {
        out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
        return;
      }

      // Create remove operation
      RemoveOperation removeOp = new RemoveOperation(filename, activePorts, out);
      removeOperations.put(filename, removeOp);

      // Send REMOVE message to all relevant Dstores
      for (int port : activePorts) {
        PrintWriter dstoreWriter = dstoreWriters.get(port);
        if (dstoreWriter != null) {
          dstoreWriter.println(Protocol.REMOVE_TOKEN + " " + filename);
          System.out.println("Sent REMOVE message for " + filename + " to Dstore port " + port);
        }
      }
    }

    private void handleReload(String message) {
      if (!enoughDstores()) {
        out.println(Protocol.ERROR_NOT_ENOUGH_DSTORES_TOKEN);
        return;
      }

      // Parse the reload request: RELOAD filename
      String[] parts = message.split(" ");
      if (parts.length != 2) {
        System.err.println("Invalid RELOAD message: " + message);
        return;
      }

      String filename = parts[1];

      // Check if file exists and is in STORE_COMPLETE state
      FileInfo fileInfo = index.get(filename);
      if (fileInfo == null || fileInfo.getState() != FileInfo.State.STORE_COMPLETE) {
        out.println(Protocol.ERROR_FILE_DOES_NOT_EXIST_TOKEN);
        return;
      }

      // Get the set of Dstores that have this file
      Set<Integer> dstorePorts = fileInfo.getDstorePorts();

      // Filter out the Dstore that was just tried (if we can track this)
      // For now, just select a different one if possible

      if (dstorePorts.isEmpty()) {
        out.println(Protocol.ERROR_LOAD_TOKEN);
        return;
      }

      // Select one Dstore
      int selectedPort = selectDstoreForLoad(dstorePorts);
      if (selectedPort == -1) {
        out.println(Protocol.ERROR_LOAD_TOKEN);
        return;
      }

      // Send LOAD_FROM message to client
      out.println(Protocol.LOAD_FROM_TOKEN + " " + selectedPort + " " + fileInfo.getSize());
      System.out.println("Sent LOAD_FROM for " + filename + " directing to Dstore port " + selectedPort);
    }

    private void handleRemoveAck(String message) {
      String[] parts = message.split(" ");
      if (parts.length != 2) {
        System.err.println("Invalid REMOVE_ACK message: " + message);
        return;
      }

      String filename = parts[1];
      System.out.println("Processing REMOVE_ACK for " + filename);

      // Get the remove operation
      RemoveOperation removeOp = removeOperations.get(filename);
      if (removeOp == null) {
        System.err.println("No active remove operation for " + filename);
        return;
      }

      // Find which Dstore sent this message
      int dstorePort = -1;
      for (Map.Entry<Integer, Socket> entry : dstores.entrySet()) {
        if (entry.getValue() == socket) {
          dstorePort = entry.getKey();
          break;
        }
      }

      if (dstorePort == -1) {
        System.err.println("Could not identify Dstore for REMOVE_ACK");
        return;
      }

      // Add acknowledgement
      removeOp.addAcknowledgement(dstorePort);
    }

    private void handleDisconnect() {
      // Check if this was a Dstore
      for (Map.Entry<Integer, Socket> entry : dstores.entrySet()) {
        if (entry.getValue() == socket) {
          int dstorePort = entry.getKey();
          System.out.println("Dstore on port " + dstorePort + " disconnected");
          dstores.remove(dstorePort);
          dstoreWriters.remove(dstorePort);

          // TODO: Handle rebalance if needed
          break;
        }
      }
    }
  }
}