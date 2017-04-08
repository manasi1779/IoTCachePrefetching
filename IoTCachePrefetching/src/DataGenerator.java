import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;
import java.util.Scanner;


public class DataGenerator extends Thread{
	
	String context;
	int ID;
	static int value;
	static Object valueObject = new Object();
	static int temporalCoherencyRequirement;
	static int precisionCoherencyRequirement;
	int counter;
	static int noOfChanges;
	static String leafNode;
	static long startTime;
	static Random random = new Random();
	static String predecessorHostName;
	static String successorHostName;
	static ServerSocket serverSocket;
	static boolean needToken = false;
	static Integer cachedValue = 0;
	static Object changingPosition = new Object();
	
	//Fog Node
	String rootHostName = "glados.cs.rit.edu";
	
	public static void main(String args[]){
		Scanner s = new Scanner(System.in);
		System.out.println("########################################");
		System.out.println("	 	Initializing IoT");
		System.out.println("########################################");
		System.out.println("Enter Context");
		String context = s.nextLine();
		System.out.println("Enter TCR in miliseconds");
		int TCR = Integer.parseInt(s.nextLine());
		System.out.println("Enter PCR");
		int PCR = Integer.parseInt(s.nextLine());
		System.out.println("Enter ID");
		int ID = Integer.parseInt(s.nextLine());
		s.close();
		DataGenerator iot = new DataGenerator(context, TCR, PCR, ID);
		iot.init();
	}
	
	private void init() {
		register();
		try{
			serverSocket = new ServerSocket(12345);
			Thread  dataGeneratorThread = new Thread(this, "dataGenerator");
			dataGeneratorThread.start();
			Thread requestServer = new Thread(this, "requestServer");
			requestServer.start();
		} catch (IOException e){
			e.printStackTrace();
		}		
	}

	public DataGenerator(){
		
	}
	
	public DataGenerator(String context, int TCR, int PCR, int ID){
		this.context = context; 
		temporalCoherencyRequirement = TCR;
		precisionCoherencyRequirement = PCR;
		this.ID = ID;
		this.counter = 0;
		startTime = System.currentTimeMillis();		
	}

	public void register(){
		try (Socket root = new Socket(rootHostName, 12345)){
			System.out.println("Registering to "+rootHostName+" CEP Engine");
			PrintWriter bw = new PrintWriter(root.getOutputStream(), true);
			System.out.println("Sending add command to CEP Engine");
			bw.println("addIoT");
			BufferedReader din = new BufferedReader (
					new InputStreamReader (root.getInputStream()));
			String data; 
			while((data = din.readLine()) == null);
			if(data.equals("getContext")){
				bw.println(context);
			}
			System.out.println("Sent context information");
			//If this IoT is not the root IoT of the context connect to root and there by connect to context overlay
			data = null;
			while((data = din.readLine()) == null);
			if(!data.equals("Root")){
				String contextRoot;
				while((contextRoot = din.readLine()) == null);
				System.out.println("Connecting to context root "+contextRoot);
				Socket rootConnection = new Socket(contextRoot, 12345);
				PrintWriter pw = new PrintWriter(rootConnection.getOutputStream(), true);
				pw.println("addIoT");
				BufferedReader rootIn = new BufferedReader (
						new InputStreamReader(rootConnection.getInputStream()));
				System.out.println("Getting predescessor from context root");
				data = null;
				while((data = rootIn.readLine()) == null);
				predecessorHostName = data;
				rootConnection.close();
			}
			System.out.println("Registered to CEPEngine "+rootHostName);
		} catch (IOException e){
			e.printStackTrace();
		}		
	}
	
	public void serveRequest(Socket socket){
		try{
	//	System.out.println(socket.getInetAddress().getHostName()+".cs.rit.edu connected");
		BufferedReader din = new BufferedReader (
				new InputStreamReader (socket.getInputStream()));
	//	System.out.println("Waiting for command");
		String command;
		while((command = din.readLine()) == null);
		PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
		switch(command){
			case "checkValue":{
				pw.println(checkValue());
		//		System.out.println("Sending value "+value);
				socket.close();
				break;
			}
			case "sendData":{
				System.out.println("Send data request received");
				String data = sendData();
				pw.println(data);
				socket.close();
				break;
			}
			case "changePredecessor":{
				String data;
				while((data = din.readLine()) == null);
				if(data.equals("null"))
					predecessorHostName = null;					
				else
					predecessorHostName = data;				
				System.out.println("Changed predecessor to "+predecessorHostName);
				socket.close();
				break;
			}
			case "changeSuccessor":{
				String data;
				while((data = din.readLine()) == null);
				if(data.equals("null"))
					successorHostName = null;
				else
					successorHostName = data;
				System.out.println("Changed successor to "+successorHostName);
				socket.close();
				break;
			}
			case "getPredecessor":{
				if(predecessorHostName == null)
					pw.println("null");
				else
					pw.println(predecessorHostName);
				socket.close();
				break;
			}
			case "getSuccessor":{
				if(successorHostName == null)
					pw.println("null");
				else
					pw.println(successorHostName);
				socket.close();
				break;
			}
			case "insert":{
				System.out.println("Inserting IoT "+socket.getInetAddress().getHostName()+".cs.rit.edu");
				synchronized(changingPosition){
					insert(socket);
					socket.close();
					Thread.sleep(1000);
				}
				break;
			}
			case "getUpdate":{
				pw.println(getUpdate());
				socket.close();
				break;
			}
			case "addIoT":{
				synchronized(changingPosition){
					addIoT(socket);
					socket.close();
					Thread.sleep(1000);
				}
				break;
			}
			case "useToken":{
			//	System.out.println("Waiting for changes");
				String rollingchanges;
				pw.println("getChanges");
				while((rollingchanges = din.readLine()) == null);
				synchronized(changingPosition){
					if(needToken){
						System.out.println("using token");
						adjustPosition();
					}
					else{
						System.out.println("Not using token");
					}
					noOfChanges += Integer.parseInt(rollingchanges);				
					if(successorHostName != null){
						System.out.println("Passing token to successor "+successorHostName);
						Socket succ = new Socket(successorHostName, 12345);
						PrintWriter newPw = new PrintWriter(succ.getOutputStream(), true);					
						newPw.println("useToken");
						System.out.println("Waiting for no of changes");
						din = new BufferedReader (
								new InputStreamReader (succ.getInputStream()));
						String data;
						while((data = din.readLine()) == null);
						if(data.equals("getChanges"))
							newPw.println(noOfChanges);
						succ.close();
						System.out.println("Token sent to "+successorHostName);
					}
					else{					
						System.out.println("Giving token back to root");
						try(Socket rootsocket = new Socket(rootHostName, 12345)){
							PrintWriter bw = new PrintWriter(rootsocket.getOutputStream(), true);
							bw.println("releaseToken");
							String data;
							System.out.println("Waiting for command 1");
							din = new BufferedReader (
									new InputStreamReader (rootsocket.getInputStream()));
							while((data = din.readLine()) == null);
							if(data.equals("getContext")){
								bw.println(context);
							}
							System.out.println("Waiting for command 2");
							data = null;
							while((data = din.readLine()) == null);
							if(data.equals("getChanges"))
							bw.println(noOfChanges);
							System.out.println("Gave token back to root");
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
					noOfChanges = 0;
					socket.close();
				}
					}
			}		
		}
		catch(IOException e){
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	
	public void run(){
		while(true){
			if(Thread.currentThread().getName().equals("dataGenerator")){
				createRandomValue();
			}
			else{
				//System.out.println("Serve request thread");
				try(Socket socket = serverSocket.accept()){			
					serveRequest(socket);	
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
		
	public void addIoT(Socket socket) {
		try{
			System.out.println("Adding "+socket.getInetAddress().getHostName()+".cs.rit.edu"+" to the system");
			PrintWriter bw = new PrintWriter(socket.getOutputStream(), true);
			//bw.println("changePredecessor");
			if(leafNode == null){
				bw.println(InetAddress.getLocalHost().getHostName()+".cs.rit.edu");
				successorHostName = socket.getInetAddress().getHostName()+".cs.rit.edu";
				leafNode = successorHostName;
			}
			else{
				bw.println(leafNode);			
				Socket leafSocket = new Socket(leafNode, 12345);
				PrintWriter oldLeaf = new PrintWriter(leafSocket.getOutputStream(), true);
				oldLeaf.println("changeSuccessor");
				oldLeaf.println(socket.getInetAddress().getHostName()+".cs.rit.edu");
				leafNode = socket.getInetAddress().getHostName()+".cs.rit.edu";
				leafSocket.close();
			}
		} catch (IOException e){
			e.printStackTrace();
		}
	}
	
	public void updateEngine(){
		try(Socket socket = new Socket(rootHostName, 12345)){
			PrintWriter bw = new PrintWriter(socket.getOutputStream(), true);
			bw.println(noOfChanges);			
			System.out.println("Sent update of "+noOfChanges);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}	
	
	public int getUpdate(){
		int changes = 0;
		try{
			if(successorHostName == null){
				return noOfChanges;
			}
			else{
				Socket successorSocket = new Socket(successorHostName, 12345);
				PrintWriter bw = new PrintWriter(successorSocket.getOutputStream(), true);
				bw.println("getUpdate");
				BufferedReader din = new BufferedReader (
						new InputStreamReader (successorSocket.getInputStream()));
				String data;
				while((data = din.readLine()) == null);
				changes = Integer.parseInt(data);
				successorSocket.close();
			}			
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("Sending number of changes data "+(noOfChanges + changes));
		return noOfChanges + changes;
	}

	
	/*
	 * Deals with new positions replacement
	 */
	public void insert(Socket successor){
		// update insert as new successor
		String oldSuccessor = successorHostName; 
		System.out.println("Changing successor");
		successorHostName = successor.getInetAddress().getHostName()+".cs.rit.edu";
		System.out.println("New successor "+successorHostName);
		try{			
			if(oldSuccessor != null){
				Socket oldSuccessorSocket = new Socket(oldSuccessor, 12345);
				// update predecessor of successor as this new insert;
				PrintWriter oldSuccessorpw = new PrintWriter(oldSuccessorSocket.getOutputStream(), true);
				oldSuccessorpw.println("changePredecessor");
				oldSuccessorpw.println(successorHostName);
				oldSuccessorSocket.close();
			}
			PrintWriter successorpw = new PrintWriter(successor.getOutputStream(), true);			
			// update successor of insert as old successor
			//successorpw.println("changeSuccessor");
			if(oldSuccessor == null){				
				successorpw.println("null");
			}
			else{
				successorpw.println(oldSuccessor);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	public int checkValue(){
		return cachedValue;
	}
	
	public void createRandomValue(){
		long now = System.currentTimeMillis();
		if(now - startTime > temporalCoherencyRequirement){
			startTime = now;	
			int oldValue = value;
			synchronized(valueObject){
				value = random.nextInt(100);
				if(cachedValue == 0)
					cachedValue = value;
				if(Math.abs(oldValue-value) >= precisionCoherencyRequirement )
					needToken = true;
			}
			System.out.println("Generated value "+value);
		}
	}
	
	/**
	 * Command by CEPEngine to new Context root
	 */
	public void updateContextRoot(){
		try(Socket root = new Socket(rootHostName, 12345)){
			PrintWriter bw = new PrintWriter(root.getOutputStream(), true);
			bw.println("updateContextRoot");	
			BufferedReader din = new BufferedReader (
					new InputStreamReader (root.getInputStream()));			
			String data;
			while((data = din.readLine()) == null);
			if(data.equals("getContext")){
				System.out.println("Sending context");
				bw.println(context);
			}			
			bw.println("getMySuccessor");
			data = null;
			while((data = din.readLine()) == null);
			successorHostName = data;
			System.out.println("Changed successor to"+data);
			predecessorHostName = null;
			System.out.println("Changed predecessor to null");
		} catch (IOException e){
			e.printStackTrace();
		}
	}	
	
	public String checkPredesessor(){
		int predecessorValue;
		Socket predecessorSocket = null;
		String pred = predecessorHostName;
		if(pred == null){
			return null;
		}
		//This is root of context overlay
		try {
			while(true){
				predecessorSocket = new Socket(pred, 12345);
				PrintWriter newpw = new PrintWriter(predecessorSocket.getOutputStream(), true);
				newpw.println("checkValue");
				System.out.println("Waiting for checking value from "+pred);
				BufferedReader din = new BufferedReader (
						new InputStreamReader (predecessorSocket.getInputStream()));			
				String data;
				while((data = din.readLine()) == null);
				predecessorValue = Integer.parseInt(data);
				System.out.println("Predecessor value is "+predecessorValue+" own value is "+value);
				if(predecessorValue > value)
					break;
				noOfChanges++;
				predecessorSocket.close();
				predecessorSocket = new Socket(pred, 12345);
				newpw = new PrintWriter(predecessorSocket.getOutputStream(), true);
				newpw.println("getPredecessor");
				din = new BufferedReader (
						new InputStreamReader (predecessorSocket.getInputStream()));
				pred = null;
				while((pred = din.readLine()) == null);		
				predecessorSocket.close();
				if(pred.equals("null")){
					return pred;
				}	
			}			
						
		} catch (IOException e) {
			e.printStackTrace();
		}
		//noOfChanges--;
		if(pred.equals(predecessorHostName))
			return null;
		else
			return pred;
	}
	
	public String checkSuccessor(){
		int successorValue;
		String succHostName = successorHostName;
		Socket successorSocket = null;
		if(succHostName == null){
			return null;
		}
		try {
			while(true){
			//	System.out.println("reaching for successor "+succHostName);
				successorSocket = new Socket(succHostName, 12345);
				PrintWriter newpw = new PrintWriter(successorSocket.getOutputStream(), true);
				newpw.println("checkValue");
				System.out.println("Waiting for checking value from "+succHostName);
				BufferedReader din = new BufferedReader (
						new InputStreamReader (successorSocket.getInputStream()));
				String data;
				while((data = din.readLine()) == null);
				successorValue = Integer.parseInt(data);
				noOfChanges++;
				successorSocket.close();				
				System.out.println("Successor value is "+successorValue+" own value is "+value);
				if(successorValue < value){
					if(noOfChanges == 1)						
						return null;
					else
						return succHostName;
				}				
				successorSocket = new Socket(succHostName, 12345);
				newpw = new PrintWriter(successorSocket.getOutputStream(), true);
				newpw.println("getSuccessor");
				succHostName = null;
				din = new BufferedReader (
							new InputStreamReader (successorSocket.getInputStream()));
				while((succHostName = din.readLine()) == null);
				if(succHostName.equals("null")){
					break;
				}				
				noOfChanges++;
			}
			if(succHostName.equals("null")){
				succHostName = successorSocket.getInetAddress().getHostName();
				successorSocket.close();
				return succHostName;
			}
			Socket newPredecessor = new Socket(succHostName, 12345);
			PrintWriter bw = new PrintWriter(newPredecessor.getOutputStream(), true);
			bw.println("getPredecessor");
			BufferedReader din = new BufferedReader(
					new InputStreamReader (newPredecessor.getInputStream()));
			succHostName = null;
			while((succHostName = din.readLine()) == null);
			newPredecessor.close();
		} catch (IOException e) {
			e.printStackTrace();
		}		
		noOfChanges--;
		return succHostName;
	}
	
	public void adjustPosition(){		
		cachedValue = value;
		try {
		String changeWith = checkPredesessor();
		PrintWriter bw;
			if(changeWith == null){
				//No need to change
				noOfChanges = 0;
				changeWith = checkSuccessor();
				if(changeWith == null){
					noOfChanges = 0;
					System.out.println("Need valueObject semaphore");
					synchronized(valueObject){
						needToken = false;
					}
					return;
				}
			}
			System.out.println("Adjusting position in network");
			if(changeWith.equals("null")){
				System.out.println("Changing context root node");
				removeSelf();
				updateContextRoot();				
				return;
			}			
			System.out.println("Contacting "+changeWith);
			Socket newPredecessor = new Socket(changeWith, 12345);
			bw = new PrintWriter(newPredecessor.getOutputStream(), true);
			removeSelf();
			predecessorHostName = changeWith;
			bw.println("insert");
			BufferedReader din = new BufferedReader(new InputStreamReader (newPredecessor.getInputStream()));
			String successor;
			while((successor = din.readLine()) == null);
			if(successor.equals("null"))
				successorHostName = null;
			else
				successorHostName = successor;
			newPredecessor.close();
			synchronized(valueObject){
				needToken = false;
			}
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}	
	
	/**
	 * Deals with old positions
	 */
	public void removeSelf() {
		try{
			if(predecessorHostName != null){
				Socket pred = new Socket(predecessorHostName, 12345);
				PrintWriter bw = new PrintWriter(pred.getOutputStream(), true);
				bw.println("changeSuccessor");
				if(successorHostName == null)
					bw.println("null");
				else
					bw.println(successorHostName);
				pred.close();
			}
			else{
				//Node which is changing position is root node
				Socket cep = new Socket(rootHostName, 12345);
				BufferedReader din = new BufferedReader(new InputStreamReader (cep.getInputStream()));
				PrintWriter bw = new PrintWriter(cep.getOutputStream(), true);
				bw.println("updateContextRootPassive");
				String data;
				while((data = din.readLine()) == null);
				if(data.equals("getContext")){
					bw.println(context);
				}						
				bw.println(successorHostName);
				cep.close();
			}			
			if(successorHostName != null){
				Socket succ = new Socket(successorHostName, 12345);
				PrintWriter bw = new PrintWriter(succ.getOutputStream(), true);
				bw.println("changePredecessor");
				if(predecessorHostName == null){
					bw.println("null");
					System.out.println("Changing predecessor of 2nd node to null");
				}
				else
					bw.println(predecessorHostName);
				succ.close();
			}
		} catch (IOException e)	 {
			e.printStackTrace();
		}		
	}

	public String sendData(){
		String data  = "";		
		try {			
			if(successorHostName == null){
				return ID+":"+cachedValue;
			}
			else{
				Socket successorSocket = new Socket(successorHostName, 12345);
				PrintWriter bw = new PrintWriter(successorSocket.getOutputStream(), true);
				bw.println("sendData");
				BufferedReader din = new BufferedReader (
						new InputStreamReader (successorSocket.getInputStream()));
				data = null;
				while((data = din.readLine()) == null);
				successorSocket.close();
			}			
		} catch (IOException e) {
			e.printStackTrace();
		}
		System.out.println("Sending data "+data+" "+ID+" "+cachedValue);
		return ID+":"+cachedValue+" "+data;
	}
	
	public void writeToSocket(String hostName, String command){
		try {
			Socket socket = new Socket(hostName, 12345);
			PrintWriter bw = new PrintWriter(socket.getOutputStream(), true);
			bw.println(command);
			socket.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	
	}
	
	public String readFromSocket(String hostName){
		Socket socket;
		String data = null;
		try {
			socket = new Socket(hostName, 12345);
			BufferedReader din = new BufferedReader(new InputStreamReader (socket.getInputStream()));
			while((data = din.readLine()) == null);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		 
		return data;
	}
	
}
