import java.io.IOException;
import java.net.*;
import java.util.HashMap;

/**
 * Created by shobhitgarg on 4/3/17.
 */

/**
 * This class implements bully algorithm which is used to elect a leader among a group.
 * The code takes the process id and the leader id as a command line input and proceeds to
 * send heartbeat messages to the client. If it doesn't receive any response the leader within
 * some specified period of time, it initiates an election.
 */
public class Bully1 extends Thread{

    HashMap<Integer, InetAddress> systemMap;
    static int leader = -1;
    static int id;
    static DatagramSocket socket;
    static DatagramSocket recSocket;
    static int sendPort = 7000;
    static int recPort = 8000;
    int threadID;
    static long electionTime;
    static boolean election = false;
    static boolean okReceived = false;
    static boolean heartBeatResponseReceived = false;
    static boolean coorMsgSent = false;
    static String electionMsgData;

    /**
     * Costructor function is used to initialize the system map which contains information
     * about the other members of the group.
     * @throws UnknownHostException
     */
    public Bully1() throws UnknownHostException {
        systemMap = new HashMap<>();
        systemMap.put(0,InetAddress.getByName("129.21.22.196")); //glados
        systemMap.put(1,InetAddress.getByName("129.21.30.37")); //queeg
        //systemMap.put(2, InetAddress.getByName("129.21.86.56")); //self
        systemMap.put(2,InetAddress.getByName("129.21.34.80")); // comet
        systemMap.put(3,InetAddress.getByName("129.21.37.49")); // rhea
        systemMap.put(4,InetAddress.getByName("129.21.37.42")); // domino
//        systemMap.put(5,InetAddress.getByName("129.21.37.55")); // gorgon
//        systemMap.put(6,InetAddress.getByName("129.21.37.30")); // kinks
    }

    /**
     * main function takes in the command line arguments. It also starts three threads to simulate
     * the bully algorithm
     * @param args          command line arguments
     * @throws UnknownHostException
     * @throws SocketException
     */
    public static void main(String[] args) throws UnknownHostException, SocketException{
        id = Integer.parseInt(args[0]) ;
        leader = Integer.parseInt(args[1]);

        recSocket = new DatagramSocket(recPort);
        socket = new DatagramSocket(sendPort);
        Bully1 obj1 = new Bully1(); obj1.threadID = 1;
        Bully1 obj2 = new Bully1(); obj2.threadID = 2;
        Bully1 obj3 = new Bully1(); obj3.threadID = 3;

        obj1.start();
        obj2.start();
        obj3.start();
    }

    /**
     * run method is called when a thread is started. Depending upon the id of the thread, it makes the thread
     * perform a specific task.
     */
    public void run()   {
        while (true)    {
            if(threadID == 1)   {
                // send thread
                try {
                    Thread.sleep(2000);
                    sendFunction();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            else if(threadID == 2){
                //receive thread
                try {
                    receiveData();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            else {
                // hold election
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if(election)    {
                    election = false;
                    holdElection();
                }
            }
        }
    }

    /**
     * holdElection function holds the election. In that it sends an election message to
     * all the processes whose id is greater. It then waits for sometime, if it doesn't
     * receive any confirmation within that time, it self proclaims itself as the new
     * leader and sends a coordination message to all the other members of the group.
     */
    void holdElection() {
        // start election
        System.out.println("Holding the election");

        electionMsgData = "Election " + id;
        sendElectionMsg();
        try {
            Thread.sleep(5000);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        if(!okReceived) {
            leader = id;
            System.out.println("\nSELF PROCLAIMED NEW LEADER!!\n");
            // self ia coordinator, send coordinator msg now
            for(int key: systemMap.keySet())    {
                if(key != id)   {
                   String msg = "COOR " + id;
                    byte[] sendBuffer = msg.getBytes();
                    System.out.println("Sending coordinator message to: " + key);
                    DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, systemMap.get(key), recPort);
                    try {
                        socket.send(sendPacket);
                    } catch (IOException e) {
                        System.out.println("\nIssue in sending packet");
                    }
                }
            } // coor message sent
        } // okay msg not received, coor sent
    }

    /**
     * sendElectionMsg() sends the election message to all the processes in the group whose id is greater than its id.
     */
    void sendElectionMsg()  {
        okReceived = false;
        for (int key : systemMap.keySet()) {
            if (key > id) {
                // greater
                byte[] sendBuffer = electionMsgData.getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, systemMap.get(key), recPort);
                try {
                    socket.send(sendPacket);
                } catch (IOException e) {
                    System.out.println("\nIssue in sending packet");
                }
                System.out.println("sent election message to: " + key );
            }
        }
        coorMsgSent = false;
        electionTime = System.currentTimeMillis();
    }

    /**
     * sendFunction() is used to send the heartbeat message to the leader. After sending the heartbeat it waits
     * for some specified period of time. If it doesn't receive a heartbeat response within that time, it sets a
     * flag, which in turn makes the other thread hold an election to elect a new leader.
     * @throws InterruptedException
     */
    void sendFunction() throws InterruptedException {
        // if itself is not the leader of the group
     if(leader != id && leader != -1)    {
            if(!heartBeatResponseReceived) {
                String msg = "HeartBeat " + id;
                byte[] sendBuffer = msg.getBytes();
                DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, systemMap.get(leader), recPort);
                try {
                    socket.send(sendPacket);
                    heartBeatResponseReceived = false;
                    System.out.println("Sending heartbeat to " + leader);
                } catch (IOException e) {
                    System.out.println("Error in sending heartbeat packet");
                }

                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                if (!heartBeatResponseReceived) {
                  election = true;
                  leader = -1;
                    System.out.println("Heartbeat not received hence prompting to start the election");
                } // heartbeat response not received
            }
            else {
                Thread.sleep(2000);
                heartBeatResponseReceived = false;
            }
        }
    }

    /**
     * receiveData function is used to receive the data from other members of the group.
     * It receives various kinds of messages, for example an election message or a coordination
     * message. Depending upon the type of message, it performs an action.
     * @throws InterruptedException
     */
    void receiveData() throws InterruptedException {
        byte[] receiveBuffer = new byte[2048];
        DatagramPacket receivePacket = new DatagramPacket(receiveBuffer, receiveBuffer.length);
        try {
            recSocket.receive(receivePacket);
            String dataReceived = new String(receivePacket.getData()).trim();
            System.out.println("DATA RECEIVED: " + dataReceived);
            String [] data = dataReceived.split(" ");
            if(data[0].equals("Election"))  {
                // sending reply to the election
                sendElectionReply(data);
              election = true;
              leader = -1;
            }
            else if(data[0].equals("OK"))   {
                // itself not a leader
                okReceived = true;
                election = false;
            }
            else if(data[0].equals("COOR")){
                // set the new leader
                leader = Integer.parseInt(data[1]);
                System.out.println("\nNEW LEADER ELECTED: " + leader + "\n");
            }
            else if(data[0].equals("HeartBeat")){
                // reply to heartbeat
                replyToHeartBeat(data);
            }
            else {

                // reply to sent heartbeat received "HeartBeatOK"
                heartBeatResponseReceived = true;
            }
        }
        catch (IOException e)   {
            System.out.println("Error in receiving packet");
        }
    }

    /**
     * sendElectionReply function is used to reply to an election message.
     * @param data          data received from the message
     */
    void sendElectionReply(String data[])    {

        StringBuilder sb = new StringBuilder("OK from" + id);
        byte []sendBuffer = sb.toString().getBytes();
        DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, systemMap.get(Integer.parseInt(data[1])), recPort);
        try {
            socket.send(sendPacket);
        }
        catch (IOException e)   {
            System.out.println("Error in sending packet");
        }
    }

    /**
     * replyToHeartBeat message is used to reply to a heartbeat message. This function is only used when
     * the node is the leader of the group.
     * @param data          data received from the message
     */
    void replyToHeartBeat(String data[])    {
        String msg = "HeartBeatOK " + id;
        byte[] sendBuffer = msg.getBytes();
        System.out.println("Sending heartbeat reply to: " + data[1]);
        DatagramPacket sendPacket = new DatagramPacket(sendBuffer, sendBuffer.length, systemMap.get(Integer.parseInt(data[1])), recPort);
        try {
            socket.send(sendPacket);
        }
        catch (IOException e)   {
            System.out.println("Error in sending packet");
        }
    }
}
