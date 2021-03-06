import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Random;
import java.util.Scanner;

/**
 * IoT class
 * @author Manasi
 *
 */
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
	static String rootHostName;
	
	public static void main(String args[]){
		Scanner s = new Scanner(System.in);
		System.out.println("########################################");
		System.out.println("	 	Initializing IoT");
		System.out.println("########################################");
		System.out.println("Enter Context:");
		String context = s.nextLine();
	/*	System.out.println("Enter TCR in miliseconds");
		int TCR = Integer.parseInt(s.nextLine());
		System.out.println("Enter PCR");
		int PCR = Integer.parseInt(s.nextLine());
	*/	System.out.println("Enter ID:");
		int ID = Integer.parseInt(s.nextLine());
		System.out.println("Default Temporal coherency requirement set to: 10000");
		System.out.println("Default Precision coherency requirement set to: 10");
		System.out.println("Enter Router to connect:");
		String router = s.nextLine();
		rootHostName = router + ".cs.rit.edu";
		s.close();
		DataGenerator iot = new DataGenerator(context, 10000, 10, ID);
		iot.init();
	}
	
	/**
	 * Initializes IoT device by registering to router
	 * Starts thread for generating data continuously and serving requests
	 */
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

	/**
	 * Registers to Router specified by user
	 */
	public void register(){
		try (Socket root = new Socket(rootHostName, 12345)){
			System.out.println("Registering to "+rootHostName+" CEP Engine");
			PrintWriter bw = new PrintWriter(root.getOutputStream(), true);
			System.out.println("Sending add command to CEP Engine");
			bw.println(Messages.addIoT);
			BufferedReader din = new BufferedReader (
					new InputStreamReader (root.getInputStream()));
			String data; 
			while((data = din.readLine()) == null);
			if(data.equals(Messages.getContext)){
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
				pw.println(Messages.addIoT);
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
	
	/**
	 * Serves requests coming from different IoTs or Router
	 * @param socket
	 */
	public void serveRequest(Socket socket){
		try{
		BufferedReader din = new BufferedReader (
				new InputStreamReader (socket.getInputStream()));
		String command;
		while((command = din.readLine()) == null);
		PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
		switch(command){
			case Messages.checkValue:{
				pw.println(checkValue());
				socket.close();
				break;
			}
			case Messages.sendData:{
				System.out.println("Send data request received");
				String data = sendData();
				pw.println(data);
				socket.close();
				break;
			}
			case Messages.changePredecessor:{
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
			case Messages.changeSuccessor:{
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
			case Messages.getPredecessor:{
				if(predecessorHostName == null)
					pw.println("null");
				else
					pw.println(predecessorHostName);
				socket.close();
				break;
			}
			case Messages.getSuccessor:{
				if(successorHostName == null)
					pw.println("null");
				else
					pw.println(successorHostName);
				socket.close();
				break;
			}
			case Messages.insert:{
				System.out.println("Inserting IoT "+socket.getInetAddress().getHostName()+".cs.rit.edu");
				synchronized(changingPosition){
					insert(socket);
					socket.close();
					Thread.sleep(1000);
				}
				break;
			}
			case Messages.getUpdate:{
				pw.println(getUpdate());
				socket.close();
				break;
			}
			case Messages.addIoT:{
				synchronized(changingPosition){
					addIoT(socket);
					socket.close();
					Thread.sleep(1000);
				}
				break;
			}
			//TODO
			/*
			 * Change to remember old successor before changing position. 
			 * If position was changed with any of the successors 
			 * then token should be passed to new successor. 
			 * If position was changed with any of the predecessors
			 * then also token should be passed to old successor instead of newly updated successor.
			 */
			case Messages.useToken:{
				String rollingchanges;
				pw.println(Messages.getChanges);
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
						newPw.println(Messages.useToken);
						System.out.println("Waiting for no of changes");
						din = new BufferedReader (
								new InputStreamReader (succ.getInputStream()));
						String data;
						while((data = din.readLine()) == null);
						if(data.equals(Messages.getChanges))
							newPw.println(noOfChanges);
						succ.close();
						System.out.println("Token sent to "+successorHostName);
					}
					else{					
						System.out.println("Giving token back to root");
						try(Socket rootsocket = new Socket(rootHostName, 12345)){
							PrintWriter bw = new PrintWriter(rootsocket.getOutputStream(), true);
							bw.println(Messages.releaseToken);
							String data;
							System.out.println("Waiting for command 1");
							din = new BufferedReader (
									new InputStreamReader (rootsocket.getInputStream()));
							while((data = din.readLine()) == null);
							if(data.equals(Messages.getContext)){
								bw.println(context);
							}
							System.out.println("Waiting for command 2");
							data = null;
							while((data = din.readLine()) == null);
							if(data.equals(Messages.getChanges))
							bw.println(noOfChanges);
							System.out.println("Gave token back to root");
						} catch (IOException e) {
							e.printStackTrace();
						}
					}
					noOfChanges = 0;
					socket.close();
				}
				break;
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
				try(Socket socket = serverSocket.accept()){			
					serveRequest(socket);	
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * This method is invoked if this IoT is root node	
	 * @param socket
	 */
	public void addIoT(Socket socket) {
		try{
			System.out.println("Adding "+socket.getInetAddress().getHostName()+".cs.rit.edu"+" to the system");
			PrintWriter bw = new PrintWriter(socket.getOutputStream(), true);
			if(leafNode == null){
				bw.println(InetAddress.getLocalHost().getHostName()+".cs.rit.edu");
				successorHostName = socket.getInetAddress().getHostName()+".cs.rit.edu";
				leafNode = successorHostName;
			}
			else{
				bw.println(leafNode);			
				Socket leafSocket = new Socket(leafNode, 12345);
				PrintWriter oldLeaf = new PrintWriter(leafSocket.getOutputStream(), true);
				oldLeaf.println(Messages.changeSuccessor);
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
				bw.println(Messages.getUpdate);
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

	
	/**
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
				oldSuccessorpw.println(Messages.changePredecessor);
				oldSuccessorpw.println(successorHostName);
				oldSuccessorSocket.close();
			}
			PrintWriter successorpw = new PrintWriter(successor.getOutputStream(), true);			
			// update successor of insert as old successor
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
	
	/**
	 * Generates random data
	 */
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
			bw.println(Messages.updateContextRoot);	
			BufferedReader din = new BufferedReader (
					new InputStreamReader (root.getInputStream()));			
			String data;
			while((data = din.readLine()) == null);
			if(data.equals(Messages.getContext)){
				System.out.println("Sending context");
				bw.println(context);
			}			
			bw.println(Messages.getMySuccessor);
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
	
	/**
	 * While adjusting position in context path checks if all predecessor have value greater than own value
	 * If a previous IoT has lesser value, gets its predecessor 
	 * @return
	 */
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
				newpw.println(Messages.checkValue);
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
				newpw.println(Messages.getPredecessor);
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
	
	/**
	 * Checks if all successor have value greater than own value
	 * If a successor has greater value gets its info 
	 * @return
	 */
	public String checkSuccessor(){
		int successorValue;
		String succHostName = successorHostName;
		Socket successorSocket = null;
		if(succHostName == null){
			return null;
		}
		try {
			while(true){
				successorSocket = new Socket(succHostName, 12345);
				PrintWriter newpw = new PrintWriter(successorSocket.getOutputStream(), true);
				newpw.println(Messages.checkValue);
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
				newpw.println(Messages.getSuccessor);
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
			bw.println(Messages.getPredecessor);
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
	
	/**
	 * Checks value with predecessor and successor for finding correct position for inserting
	 */
	public void adjustPosition(){		
		cachedValue = value;
		try {
		String changeWith = checkPredesessor();
		PrintWriter bw;
			if(changeWith == null){
				//No need to change with any preceding IoT
				noOfChanges = 0;
				changeWith = checkSuccessor();
				if(changeWith == null){
					noOfChanges = 0;
					System.out.println("Need value Object semaphore");
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
			bw.println(Messages.insert);
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
				bw.println(Messages.changeSuccessor);
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
				bw.println(Messages.updateContextRootPassive);
				String data;
				while((data = din.readLine()) == null);
				if(data.equals(Messages.getContext)){
					bw.println(context);
				}						
				bw.println(successorHostName);
				cep.close();
			}			
			if(successorHostName != null){
				Socket succ = new Socket(successorHostName, 12345);
				PrintWriter bw = new PrintWriter(succ.getOutputStream(), true);
				bw.println(Messages.changePredecessor);
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

	/**
	 * Sends sorted data by collecting data from successor
	 * @return
	 */
	public String sendData(){
		String data  = "";		
		try {			
			if(successorHostName == null){
				return ID+":"+cachedValue;
			}
			else{
				Socket successorSocket = new Socket(successorHostName, 12345);
				PrintWriter bw = new PrintWriter(successorSocket.getOutputStream(), true);
				bw.println(Messages.sendData);
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
	
}
