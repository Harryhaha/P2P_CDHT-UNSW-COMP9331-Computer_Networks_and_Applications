import java.io.BufferedReader;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.DatagramPacket;
import java.net.DatagramSocket;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Timer;
import java.util.TimerTask;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
public class cdht_ex {
	int identifer;
	int successor;
	int nextSuccessor;
	int predecessor;
	int beforePredecessor;
	DatagramSocket udpSocket;
	ServerSocket tcpServerSocket;
	boolean flagStopUdpSendThread = false;
	boolean flagStopUdpRecvThread = false;
	boolean flagStopTcpSendThread = false;
	boolean flagStopTcpRecvThread = false;
	int seq1 = 0;
	int seq2 = 0;
	int ack1 = 0;
	int ack2 = 0;
	boolean promptLeaveFromPre = false;
	boolean promptLeaveFromBeforePre = false;
	BufferedReader br = new BufferedReader(new InputStreamReader(System.in));  // input
	//boolean flagStartUdpPing = false;
	public cdht_ex(int identifier, int successor, int nextSuccessor) throws IOException{
		this.identifer = identifier;
		this.successor = successor;
		this.nextSuccessor = nextSuccessor;
		this.udpSocket = new DatagramSocket(identifer+50000);
		this.tcpServerSocket = new ServerSocket(identifier+50000);
	}
	
	// UDP send 
	public void updSendToPeer(String type, int portNumOfPeer, String relation, int seq) throws IOException{   
		InetAddress host = InetAddress.getByName("127.0.0.1");
		String udpRequestStr = "";
    	if(type.equals("request")){     		// send request
    		//requestStr = "send request";
    		udpRequestStr = "request"+" "+relation+" "+identifer+" "+seq; // udp request message format
    	}else{									// send response
    		udpRequestStr = "response"+" "+relation+" "+identifer+" "+seq;// udp response message format
    	}
    	byte[] udpRequestBuf = udpRequestStr.getBytes();
        DatagramPacket sendToPeer = new DatagramPacket(udpRequestBuf, udpRequestBuf.length, host, portNumOfPeer);
        udpSocket.send(sendToPeer);
	}
	// UDP recv
	public void udpRecvFromPeerAndSend() throws IOException{  //receive
        byte[] buffer = new byte[1000];
        DatagramPacket recvFromPeer = new DatagramPacket(buffer, 100);
        udpSocket.receive(recvFromPeer);
        int portNumOfPeer = recvFromPeer.getPort()-50000;
        String strContent = new String(buffer, 0, recvFromPeer.getLength()); 
        if(strContent.startsWith("request")){		// It is a ping request
        	String[] messageFrame = strContent.split(" ");
        	String messageRelation = messageFrame[1];   // successor or nextSuccessor
        	String messageIdentifier = messageFrame[2];       // identifier
        	String messageSeq = messageFrame[3];       // sequence number
            System.out.println("A ping request message was received from Peer " + portNumOfPeer + ".");
            //determine its predecessor and beforePredecessor
	   		int portOfOneOfNextTwoPeer = 0;
	   	    portOfOneOfNextTwoPeer = Integer.parseInt(messageIdentifier);
	   	    int NumericSeq = 0;
	   	    String relation = ""; 
        	if(messageRelation.equals("successor")){
        		this.predecessor = portOfOneOfNextTwoPeer;
        		relation = "successor";
        		NumericSeq = Integer.parseInt(messageSeq);
        	}else{
        		this.beforePredecessor = portOfOneOfNextTwoPeer;
        		relation = "nextSuccessor";
        		NumericSeq = Integer.parseInt(messageSeq);
        	}
        	//determine its predecessor and beforePredecessor
        	updSendToPeer("response", portNumOfPeer+50000, relation, NumericSeq); //last param is useless for response
        }
        else if(strContent.startsWith("response")){										// It is a ping response
        	System.out.println("A ping response message was received from Peer" + portNumOfPeer + ".");
        	String[] messageFrame = strContent.split(" ");
        	String messageRelation = messageFrame[1];   		// successor or nextSuccessor
        	//String messageIdentifier = messageFrame[2];       // identifier
        	String messageSeq = messageFrame[3];      			// sequence number
    		
        	if(messageRelation.equals("successor")){
        		int ack1_tmp = Integer.parseInt(messageSeq);
        		if(seq1 >= ack1_tmp){
        			ack1 = seq1;
        		}
        	}
        	else if(messageRelation.equals("nextSuccessor")){
        		int ack2_tmp = Integer.parseInt(messageSeq);
        		if(seq2 >= ack2_tmp){
        			ack2 = seq2;
        		}
        	}
        }
	}
	
	// TCP send
	public void tcpSendToPeer() throws NumberFormatException, UnknownHostException, IOException{
		//BufferedReader br = new BufferedReader(new InputStreamReader(System.in));  // input
        //if(br.ready()){
	    String str = br.readLine(); 
	    String[] input = str.split("\\s+");
	    String firstCommand = input[0];
	    if(firstCommand.equals("request")){
	    	Pattern pattern = Pattern.compile("[^0-9]");
		    Matcher matcher = pattern.matcher(str);
		    String fileName = matcher.replaceAll("");
		    int fileHashValue = (Integer.parseInt(fileName)+1) % 256;
		    //int realfileHashValue = fileHashValue % 256;
		  
		    String fileHashValueStr = String.valueOf(fileHashValue);
		    Socket clientSocket = new Socket("127.0.0.1", successor+50000);
		    DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
		    outToServer.writeBytes("Forward "+fileName+" "+fileHashValueStr+" "+identifer+" notFound"); // message format !!!
		    System.out.println("File request message for "+fileName+" has been sent to my successor.");
		    //Thread.sleep(1000);
		    clientSocket.close();
	        //}
	        //br.close();
	    }
	    else if(firstCommand.equals("quit")){   // quit and update
	    	// notify its predecessor to change its successors
	    	Socket clientSocket = new Socket("127.0.0.1", predecessor+50000);
		    DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
		    int newSuccessor = successor;
		    int newNextSuccessor = nextSuccessor;
		    outToServer.writeBytes("Departure forPredecessors "+identifer+" "+newSuccessor+" "+newNextSuccessor); // message format !!!
		    clientSocket.close();
		    // notify its beforePredecessor to change its successors
		    clientSocket = new Socket("127.0.0.1", beforePredecessor+50000);
		    outToServer = new DataOutputStream(clientSocket.getOutputStream());
		    newSuccessor = -1;  // the newSuccessor of its beforePredecessor is its former peer!
		    newNextSuccessor = successor;
		    outToServer.writeBytes("Departure forPredecessors "+identifer+" "+newSuccessor+" "+newNextSuccessor); // message format !!!
		    //System.out.println("");
		    clientSocket.close();
		    
		    // notify its successor to change its predecessors
		    clientSocket = new Socket("127.0.0.1", successor+50000);
		    outToServer = new DataOutputStream(clientSocket.getOutputStream());
		    int newPredecessor = predecessor;
		    int newBeforePredecessor = beforePredecessor;
		    outToServer.writeBytes("Departure forSuccessors "+identifer+" "+newPredecessor+" "+newBeforePredecessor); // message format !!!
		    clientSocket.close();
		 	// notify its nextSuccessor to change its predecessors
		    clientSocket = new Socket("127.0.0.1", nextSuccessor+50000);
		    outToServer = new DataOutputStream(clientSocket.getOutputStream());
		    newPredecessor = -1;
		    newBeforePredecessor = predecessor;
		    outToServer.writeBytes("Departure forSuccessors "+identifer+" "+newPredecessor+" "+newBeforePredecessor); // message format !!!
		    clientSocket.close();
		    //br.close();
		    
		    // stop both UDP and TCP send and recv threads of own peer
		    flagStopUdpSendThread = true;   // stop 
		    flagStopUdpRecvThread = true;   // stop 
		    flagStopTcpSendThread = true;   // stop 
		    flagStopTcpRecvThread = true;   // stop 
		    
	    }
        //}
	}
	public void tcpRecvFromPeerAndSend() throws IOException{
		String clientReqStr;
		while(true) {
			Socket connectionSocket = tcpServerSocket.accept();
			BufferedReader inFromClient = new BufferedReader(new InputStreamReader(connectionSocket.getInputStream()));
			clientReqStr = inFromClient.readLine();
			if(null != clientReqStr){
				String[] messageFrame = clientReqStr.split(" ");
				// for file position
				if(clientReqStr.startsWith("Destination") || clientReqStr.startsWith("Forward")){
					String messageHead = messageFrame[0];   // Forward or Destination
					String messageFileName = messageFrame[1];   // fileName
					String messageFileHash = messageFrame[2];  // fileHashValueStr
					String messageReqPeer = messageFrame[3];   // ReqPeer
					String messageDestine = messageFrame[4];   // Destine
					if(messageHead.equals("Destination")){
						System.out.println("Received a response message from peer "+messageDestine+", which has the file "+messageFileName + ".");
					}
					else{
						if(identifer > successor){    // this is the final(largest) peer, then the file must belong to it 
							System.out.println("File "+messageFileName+" is here.");   
							System.out.println("A response message, destined for peer "+ messageReqPeer+", has been sent.");
							// TCP send to the peer who make original request 
							Socket clientSocket = new Socket("127.0.0.1", Integer.parseInt(messageReqPeer)+50000);
				    	    DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
				    	    outToServer.writeBytes("Destination "+messageFileName+" "+messageFileHash+" "+messageReqPeer+" "+identifer);
				    	    clientSocket.close();
						}else{
							if((Integer.parseInt(messageFileHash) >= identifer) && (Integer.parseInt(messageFileHash) < successor)){
								System.out.println("File "+messageFileName+" is here.");   //belong to this node 
								System.out.println("A response message, destined for peer "+ messageReqPeer+", has been sent.");
								// TCP send to the peer who make original request 
								Socket clientSocket = new Socket("127.0.0.1", Integer.parseInt(messageReqPeer)+50000);
								DataOutputStream outToClient = new DataOutputStream(clientSocket.getOutputStream());
								outToClient.writeBytes("Destination "+messageFileName+" "+messageFileHash+" "+messageReqPeer+" "+identifer);
								clientSocket.close();
							}else{
								System.out.println("File "+messageFileName+" is not stored here.");
								System.out.println("File request message has been forwarded to my successor.");
								// TCP send to the its next peer
								Socket clientSocket = new Socket("127.0.0.1", successor+50000);
								DataOutputStream outToClient = new DataOutputStream(clientSocket.getOutputStream());
								outToClient.writeBytes("Forward "+messageFileName+" "+messageFileHash+" "+messageReqPeer+" notFound");
								clientSocket.close();
							}	
						}
					}
				}
				else if(clientReqStr.startsWith("AskNextPeer")){
					String messageDestine = messageFrame[1]; 
					String whichSuccessor = messageFrame[2];
					int numericDestine = Integer.parseInt(messageDestine);
					Socket clientSocket = new Socket("127.0.0.1", numericDestine+50000);
		    	    DataOutputStream outToServer = new DataOutputStream(clientSocket.getOutputStream());
		    	    outToServer.writeBytes("ReplyPeerQuery " + successor + " " + whichSuccessor);
		    	    clientSocket.close();
				}
				else if(clientReqStr.startsWith("ReplyPeerQuery")){
					String messageNextSuccessorAnswer = messageFrame[1];
					String whichSuccessor = messageFrame[2];
					int curNextSuccessor = Integer.parseInt(messageNextSuccessorAnswer);
					if(whichSuccessor.equals("successor")){
						seq1 = 0;
		        		ack1 = 0;
		        		nextSuccessor = curNextSuccessor;
		        		System.out.println("My first successor is now peer "+successor+".");
						System.out.println("My second successor is now peer "+curNextSuccessor+".");
					}
					else if(whichSuccessor.equals("nextSuccessor") && (curNextSuccessor != nextSuccessor)){
						seq2 = 0;
		        		ack2 = 0;
		        		System.out.println("Peer "+ nextSuccessor +" is no longer alive.");
		        		nextSuccessor = curNextSuccessor;
		        		System.out.println("My first successor is now peer "+successor+".");
						System.out.println("My second successor is now peer "+curNextSuccessor+".");
					}
					// then we need to update the successors of my predecessor
					
				}
				else if(clientReqStr.startsWith("Departure")){   // quit
					//String messageHead = messageFrame[0];   // Forward or Destination
					String messageToPreOrSuc = messageFrame[1];     // ToPredecessorOrSuccessor
					String messageDepartedPeer = messageFrame[2];   // DepartedPeer
					String messageNewSucOrPre = messageFrame[3];  // NewSuccessor
					String messageNewNextSucOrBeforePre = messageFrame[4];   // NewNextSuccessor 
					if(messageToPreOrSuc.equals("forPredecessors")){   //received by its predecessors
						System.out.println("Peer "+messageDepartedPeer+" will depart from the network.");
						if(messageNewSucOrPre.equals("-1")){ // this is beforePredecessor
							Socket clientSocket = new Socket("127.0.0.1", Integer.parseInt(messageDepartedPeer)+50000);
							DataOutputStream outToClient = new DataOutputStream(clientSocket.getOutputStream());
							outToClient.writeBytes("PromptLeave from predecessor");
							clientSocket.close();
							
							System.out.println("My first successor is now peer "+successor + ".");
							System.out.println("My second successor is now peer "+messageNewNextSucOrBeforePre + ".");
							this.nextSuccessor = Integer.parseInt(messageNewNextSucOrBeforePre);
						}else{		
							Socket clientSocket = new Socket("127.0.0.1", Integer.parseInt(messageDepartedPeer)+50000);
							DataOutputStream outToClient = new DataOutputStream(clientSocket.getOutputStream());
							outToClient.writeBytes("PromptLeave from beforePredecessor");
							clientSocket.close();
							
							// this is predecessor
							System.out.println("My first successor is now peer "+messageNewSucOrPre + ".");
							System.out.println("My second successor is now peer "+messageNewNextSucOrBeforePre + ".");
							this.successor = Integer.parseInt(messageNewSucOrPre);
							this.nextSuccessor = Integer.parseInt(messageNewNextSucOrBeforePre);
						}
					}
					else{  //"forSuccessors" , which means received by its successors
						if(messageNewSucOrPre.equals("-1")){ // this is beforePredecessor
							this.beforePredecessor = Integer.parseInt(messageNewNextSucOrBeforePre);
						}else{								  // this is predecessor
							this.predecessor = Integer.parseInt(messageNewSucOrPre);
							this.beforePredecessor = Integer.parseInt(messageNewNextSucOrBeforePre);
						}
					}
				}
				else if(clientReqStr.equals("PromptLeave from predecessor")){   // actively quit 
					promptLeaveFromPre = true;
					if(promptLeaveFromPre == true && promptLeaveFromBeforePre == true){
						br.close();
						System.exit(0);
					}
				}
				else if(clientReqStr.equals("PromptLeave from beforePredecessor")){   // actively quit
					promptLeaveFromBeforePre = true;
					if(promptLeaveFromPre == true && promptLeaveFromBeforePre == true){
						br.close();
						System.exit(0);
					}
				}
			}
		}
	}
	
	public static void main(String[] args) throws InterruptedException, IOException{
		int firstArgs = Integer.parseInt(args[0]);
		int secondArgs = Integer.parseInt(args[1]);
		int thirdArgs = Integer.parseInt(args[2]);
		cdht_ex dht = new cdht_ex(firstArgs, secondArgs, thirdArgs);
		// thread start 
		// UDP 
		udpSend threadUdpSend = dht.new udpSend();  
		Thread thread1 = new Thread(threadUdpSend);  
		thread1.start(); 
		udpRecv threadUdpRec = dht.new udpRecv();  
		Thread thread2 = new Thread(threadUdpRec);  
		thread2.start(); 
		
		// TCP
		tcpSend threadTcpSend = dht.new tcpSend(); 
		Thread thread3 = new Thread(threadTcpSend); 
		thread3.start(); 
		tcpRecv threadTcpRec = dht.new tcpRecv(); 
		Thread thread4 = new Thread(threadTcpRec); 
		thread4.start(); 
	}	
		
	
	// UDP send
	class udpSend implements Runnable{
		@Override
		public void run() {
			while (true) {
				//the successor part
	        	if((seq1 - ack1) > 3){  // consider that the successor is killed
	        		System.out.println("Peer "+ successor +" is no longer alive.");
	        		successor = nextSuccessor;
	        		Socket clientSocket = null;
					try {
						clientSocket = new Socket("127.0.0.1", successor+50000);
					} catch (IOException e) {
						e.printStackTrace();
					}
	    		    DataOutputStream outToServer = null;
					try {
						outToServer = new DataOutputStream(clientSocket.getOutputStream());
					} catch (IOException e1) {
						e1.printStackTrace();
					}
					try {
						outToServer.writeBytes("AskNextPeer " + identifer + " successor");
					} catch (IOException e) {
						e.printStackTrace();
					}
					try {
						clientSocket.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
	        	}
				if(flagStopUdpSendThread == true){    // stop this thread and placing two flags into there for before each send
					break;
				}else{
					try {
						updSendToPeer("request", successor+50000, "successor", seq1);
					} catch (IOException e) {
						e.printStackTrace();
					} //send request to its successor
					seq1 += 1;
					try {
						Thread.sleep(5000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}  
				}
				
				//the nextSuccessor part
				if((seq2 - ack2) > 3){ // consider that the nextSuccessor is killed
					Socket clientSocket = null;
					try {
						clientSocket = new Socket("127.0.0.1", successor+50000);
					} catch (IOException e) {
						e.printStackTrace();
					}
					DataOutputStream outToServer = null;
					try {
						outToServer = new DataOutputStream(clientSocket.getOutputStream());
					} catch (IOException e1) {
						e1.printStackTrace();
					}
					try {
						outToServer.writeBytes("AskNextPeer " + identifer + " nextSuccessor");
					} catch (IOException e) {
						e.printStackTrace();
					}
					try {
						clientSocket.close();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
					
				if(flagStopUdpSendThread == true){
					break;
				}else{
					try {
						updSendToPeer("request", nextSuccessor+50000, "nextSuccessor", seq2);
					} catch (IOException e) {
						e.printStackTrace();
					} //send request to its next successor
					seq2 += 1;
					try {
						Thread.sleep(5000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
				}
			}
		}
	}
	// UDP recv
	class udpRecv implements Runnable{
		@Override
		public void run() {
			while (true) {
				if(flagStopUdpRecvThread == true){
					break;
				}else{
					try {
						udpRecvFromPeerAndSend();  //receive
					} catch (IOException e) {
						e.printStackTrace();
					}    
				}
			}
		}
	}
	
	// TCP send  
	class tcpSend implements Runnable{
		@Override
		public void run() {
			while(true){
				try {
					tcpSendToPeer();
				} catch (NumberFormatException | IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	// TCP recv
	class tcpRecv implements Runnable{
		@Override
		public void run() {
			while(true){
				if(flagStopTcpRecvThread == true){
					break;
				}else{
					try {
						tcpRecvFromPeerAndSend();  // receive TCP
					} catch (IOException e) {
						e.printStackTrace();
					}  
				}
			}
		}
	}
}

