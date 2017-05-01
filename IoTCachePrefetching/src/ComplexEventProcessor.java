import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Random;
/**
 * Thread to serve requests at router 
 * @author Manasi
 *
 */
public class ComplexEventProcessor extends Thread{
	
	// list of clients connected
	static ArrayList<String> clientList = new ArrayList<String>();
	// list of router connected
	static ArrayList<String> connList = new ArrayList<String>();
	// Context, No of IoT devices map
	static HashMap<String, Integer> sizeMap = new HashMap<String, Integer>();
	// Level, List of sockets for different contexts
 	static HashMap<Integer, ArrayList<RootNode>> levelMap = new HashMap<Integer, ArrayList<RootNode>>();
 	// Context, associated context map
 	static HashMap<String, ArrayList<String>> associativeMap = new HashMap<String, ArrayList<String>>();
 	// Context, number of requests count map
 	static HashMap<String, Integer> popularityMap = new HashMap<String, Integer>();
 	// Context, Level map
 	static HashMap<String, Integer> contextMap = new HashMap<String, Integer>();
 	static HashMap<String, Integer> currentUpdates = new HashMap<String, Integer>();
 	static HashMap<String, String> cache = new HashMap<String, String>();
 	// context and caching client map
 	static HashMap<String, String> cacheClient = new HashMap<String, String>();
 	// objects to synchronize different operations with IoT 
 	static HashMap<String, Object> lockSockets = new HashMap<String, Object>();
 	static long startTime; 
 	String operation;
 	Socket clnt;
 	int threshold = 20;
 	static boolean checkingUpdate = false;
 	static HashMap<String, Object> updatingRoot = new HashMap<String, Object>();
 	
 	static{
 		levelMap.put(0, new ArrayList<RootNode>());
 		levelMap.put(1, new ArrayList<RootNode>());
 		levelMap.put(2, new ArrayList<RootNode>());
 	}
 	
 	//TODO 
 	/**
 	 * 1. Passing leaf node to the new context root so that IoTs need not be registered at initialization of application only.
 	 * 2. Currently data is pulled from root node which makes recursive requests to successors for locally saved sorted data when request comes. 
 	 * However, this might return unsorted data based on position of token in the overlay.
 	 * To solve this there is need to wait for token to be returned and completing data request before forwarding token again.
 	 * 
 	 */
 	
	public ComplexEventProcessor(String operation, Socket clnt) {
		this.operation = operation;
		this.clnt = clnt;		
	}

	public ComplexEventProcessor(){
		
	}

	public void run(){
		if(Thread.currentThread().getName().equals("UpdateChecker"))
			checkUpdates();
		else
			serveRequest(operation, clnt);
	}	
	
	/**
	 * Serves request from other routers, clients or data generators
	 * @param operation
	 * @param clnt
	 */
	private void serveRequest(String operation, Socket clnt) {
		try{
		switch(operation){
			case Messages.addRouter:{
				connList.add(clnt.getInetAddress().getHostName()+".cs.rit.edu");
				System.out.println("Added router "+clnt.getInetAddress().getHostName());
				System.out.println("Number of routers = "+connList.size());
				clnt.close();
				break;
			}
			case Messages.addIoT:{
				addIoT();
				clnt.close();
				break;
			}
			case Messages.requestData:{				
				requestData();
				clnt.close();
				break;
			}
			case Messages.updateContextRootPassive:{
				updateContextRootPassive();
				clnt.close();
				break;
			}
			case Messages.updateContextRoot:{
				updateContextRoot();
				clnt.close();
				break;
			}	
			case Messages.createContext:{
				createLevel();
				break;
			}
			case Messages.connect:{
				clientList.add(clnt.getInetAddress().getHostName()+".cs.rit.edu");
				clnt.close();
				System.out.println("Added client "+clnt.getInetAddress().getHostName());
				System.out.println("Number of clients = "+clientList.size());
				clnt.close();
				break;
			}
			case Messages.releaseToken:{
					PrintWriter pw = new PrintWriter(clnt.getOutputStream(), true);
					pw.println(Messages.getContext);
					BufferedReader reader = new BufferedReader(new InputStreamReader(clnt.getInputStream()));
					String context;
					while((context = reader.readLine()) == null);
					String changes;
					pw.println(Messages.getChanges);
					while((changes = reader.readLine()) == null);
					currentUpdates.put(context, Integer.parseInt(changes));
					synchronized(updatingRoot.get(context)){
						rollToken(context);
					}
					clnt.close();
					break;
			}
		}		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}	
	
	/**
	 * Stores the data and context at client chosen for particular context
	 * @param context
	 * @param level
	 * @param data
	 */
	public void storeContextProfile(String context, String data) {
		if(clientList.isEmpty()) {
			System.out.println("No Clients present to Store Context");
		} else {
			
			try {
				String clientIP = cacheClient.get(context);
				Socket cli = new Socket(clientIP, 54321);
				PrintWriter bw = new PrintWriter(cli.getOutputStream(), true);
				System.out.println("Sending store request to "+clientIP);
				bw.println(Messages.store);
				BufferedReader reader = new BufferedReader(new InputStreamReader(cli.getInputStream()));
				String command;
				while((command = reader.readLine()) == null);
				if(command.equals(Messages.sendData)){
					bw.println(context+"#"+data);
				}
				cli.close();
 			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	
	/**
	 * Method to create context and storing corresponding client at routers
	 * @param cpName
	 * @param cliName
	 * @param level
	 */
	public void createInRouters(Integer level, String context, String cachingClient) {
		
		for( String conn : connList) {
			try {
				Socket rsock = new Socket(conn, 12345);
				PrintWriter	pw = new PrintWriter(rsock.getOutputStream(), true);
				BufferedReader reader = new BufferedReader(new InputStreamReader(rsock.getInputStream()));
				pw.println(Messages.createContext);
				String command;
				while((command = reader.readLine()) == null);
				if(command.equals(Messages.sendCachingInfo)){
					pw.println(level.toString()+" "+context+" "+cachingClient);
				}
				rsock.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}			
	    }
	}
	
	/**
	 * Creates context as per data received from remote router
	 */
	public void createLevel(){
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(clnt.getInputStream()));
			PrintWriter pw = new PrintWriter(clnt.getOutputStream(), true);
			pw.println(Messages.sendCachingInfo);
			String info;
			while((info = reader.readLine()) == null);
			String splitInfo[] = info.split(" ");
			popularityMap.put(splitInfo[1], 0);
			contextMap.put(splitInfo[1], Integer.parseInt(splitInfo[0]));
			cacheClient.put(splitInfo[1], splitInfo[2]);
			System.out.println("Created caching context "+ splitInfo[1]+" at "+splitInfo[2]);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}		
	}
	
	/**
	 * When root leaves its position to insert itself somewhere else,
	 * this method adds root's successor as new root without contacting it
	 */
	public void updateContextRootPassive() {
		try {
			PrintWriter pw = new PrintWriter(clnt.getOutputStream(), true);
			pw.println(Messages.getContext);
			BufferedReader reader = new BufferedReader(new InputStreamReader(clnt.getInputStream()));
			String context;
			while((context = reader.readLine()) == null);			
			String newRoot;
			while((newRoot = reader.readLine()) == null);
			int level = contextMap.get(context);
			for(RootNode root: levelMap.get(level)){
				if(root.context.equals(context)){
					root.hostName = newRoot;
					break;	
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * Sends the token to root of context
	 * @param context
	 */
	public void rollToken(String context){
		int level = contextMap.get(context);
		String contextRoot = null;
		for(RootNode root: levelMap.get(level)){
			if(root.context.equals(context)){
				contextRoot = root.hostName;
				try {
					Socket rootSocket = new Socket(contextRoot, 12345);
					PrintWriter pw = new PrintWriter(rootSocket.getOutputStream(), true);
					pw.println(Messages.useToken);
					pw.println("0");
					rootSocket.close();
				} catch (UnknownHostException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
				break;	
			}
		}
	}
	
	
	/**
	 * Set requester as root of the context and set current context root as successor of new context root
	 */
	public void updateContextRoot(){
		try {
			PrintWriter pw = new PrintWriter(clnt.getOutputStream(), true);
			pw.println(Messages.getContext);
			BufferedReader reader = new BufferedReader(new InputStreamReader(clnt.getInputStream()));
			String context;
			while((context = reader.readLine()) == null);
			synchronized(updatingRoot.get(context)){
			System.out.println("Received context "+context);
			int level = contextMap.get(context);
			String contextRoot = null;
			for(RootNode root: levelMap.get(level)){
				if(root.context.equals(context)){
					contextRoot = root.hostName;
					root.hostName = clnt.getInetAddress().getHostName()+".cs.rit.edu";
					break;	
				}
			}
			
			for(RootNode root: levelMap.get(level)){
				if(root.context.equals(context)){
					break;	
				}
			}
			String command;
			while((command = reader.readLine()) == null);
			if(command.equals(Messages.getMySuccessor))
				pw.println(contextRoot);			
			pw.close();
			clnt.close();
			Socket newRoot = new Socket(contextRoot, 12345);
			PrintWriter pwNew = new PrintWriter(newRoot.getOutputStream(), true);
			pwNew.println(Messages.changePredecessor);
			pwNew.println(clnt.getInetAddress().getHostName()+".cs.rit.edu");
			newRoot.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}

	/**
	 * To get updates at 10s interval in pull based system
	 */
	public void checkUpdates(){
		long now = System.currentTimeMillis();
		System.out.println("###################################");
		System.out.println("    Checking updates");
		System.out.println("###################################");
		if(now - startTime > 10000){
			for(Integer level: levelMap.keySet()){
				for(RootNode root: levelMap.get(level)){
					//int update = getUpdate(root.hostName);
					//	currentUpdates.put(root.context, update);
					System.out.println(root.context+": "+currentUpdates.get(root.context));
				}
			}
		}
		startTime = now;
		checkingUpdate = false;
	}
	
	/**
	 * Adds new IoT device to the system
	 */
	public void addIoT(){
		try {
			PrintWriter bw = new PrintWriter(clnt.getOutputStream(), true);
			bw.println(Messages.getContext);
			BufferedReader din = new BufferedReader (
					new InputStreamReader (clnt.getInputStream()));
			String context;
			while((context = din.readLine()) == null);
			context = context.trim();
			if(contextMap.containsKey(context)){
				synchronized(updatingRoot.get(context)){
					bw.println("Nonroot");
					// Gives this device contact of root so that root can add this device as new leaf device
					ArrayList<RootNode> list = levelMap.get(contextMap.get(context));
						for(RootNode root: list){
							if(root.context.equals(context)){
								addIoT(root.hostName, clnt);
							}
						}
					sizeMap.put(context, sizeMap.get(context)+1);
					// Added delay so that before root adds new device to the system, token should not be rolled
					Thread.sleep(5000);
					if(sizeMap.get(context) == 3){
						rollToken(context);
					}
				}
			}
			else{
				bw.println("Root");
				contextMap.put(context, 0);
				popularityMap.put(context, 0);
				associativeMap.put(context, new ArrayList<String>());
				updatingRoot.put(context, new Object());
				sizeMap.put(context, 1);			
				// Choose client randomly to cache data
				int i = Random.class.newInstance().nextInt(clientList.size());
				cacheClient.put(context, clientList.get(i));
				RootNode node = new RootNode(context, clnt.getInetAddress().getHostName()+".cs.rit.edu");
				levelMap.get(0).add(node);
				System.out.println("level: 0 Size: "+levelMap.get(0).size());
				lockSockets.put(context, new Object());
				clnt.close();
				createInRouters(0, context, clientList.get(i));
			}
			System.out.println("Added IoT "+clnt.getInetAddress().getHostName());
		} catch (IOException e) {
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * 
	 * @param root
	 * @param clnt
	 */
	private void addIoT(String root, Socket clnt) {
		PrintWriter bw;
		try{
			bw = new PrintWriter(clnt.getOutputStream(), true);
			//Sending context root information to connecting IoT
			bw.println(root);
			clnt.close();
		} catch (IOException e) {
			e.printStackTrace();
		}	
	}	
	

	/**
	 * Client requesting for a data of particular context from IoTs
	 * @param clnt
	 */
	public void requestData(){
		System.out.println("Received request for data from "+clnt.getInetAddress().getHostName());
		PrintWriter bw;
		try{
			bw = new PrintWriter(clnt.getOutputStream(), true);
			BufferedReader din = new BufferedReader (
					new InputStreamReader (clnt.getInputStream()));
			bw.println(Messages.getContext);
			String context;
			while((context = din.readLine()) == null);
			popularityMap.put(context.trim(), popularityMap.get(context.trim()) + 1);	
			String data = prepareDataForContext(context);
			if(data.contains("#")){
				data = data.substring(1);				
				System.out.println(data);
				bw.println(data);
				clnt.close();
				storeContextProfile(context, data);
				analysePattern(data, context);
			}
			else{
				System.out.println(data);
				bw.println(data);
				clnt.close();
			}
			if(popularityMap.get(context) > 3){
				updateLevel(context);
			}
			System.out.println("Sent data to client");								
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
	
	/**
	 * Get data for particular context from local IoT devices or clients where it is cached
	 * @param context
	 * @return
	 */
	public String prepareDataForContext(String context){
		String data = "";
		System.out.println("Preparing data for "+context);
		boolean contextNotFound = true;
		if(contextMap.keySet().contains(context)){
			for(RootNode root: levelMap.get(contextMap.get(context))){
				if(root.context.equals(context)){
					System.out.println("Context found at "+root.hostName);
					contextNotFound = false;
					synchronized(lockSockets.get(context)){
						data = prepareData(root.hostName);
						cache.put(context, data);
						data = "#"+data;						
					}					
					/*for(String otherContext: associativeMap.get(context)){
						cacheContext(otherContext);
					}*/
				}
			}			
		}
		if(contextNotFound){
			String client = cacheClient.get(context);
			System.out.println("Fetching data from client in other CEP "+client);
			try {
				Socket socket = new Socket(client, 54321);
				PrintWriter bw = new PrintWriter(socket.getOutputStream(), true);
				bw.println(Messages.getData);
				bw.println(context);
				BufferedReader din = new BufferedReader(new InputStreamReader (socket.getInputStream()));
				data = null;
				while((data = din.readLine()) == null);					
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}				
		}
		return data;
	}

	/**
	 * Identify associativity between different contexts by finding 
	 * edit distance defined by longest common subsequence
	 * @param data
	 * @param context
	 */
	public void analysePattern(String data, String context) {
		String[] pattern1 = getOnlyIoTIDs(data);
		System.out.println(data);
		for(String otherContext: cache.keySet()){
			//System.out.println("Checking pattern between for context "+context +" and "+otherContext);
			if(!otherContext.equals(context)){
				String[] pattern2 = getOnlyIoTIDs(cache.get(otherContext));
				System.out.println("Finding subsequence between "+context +" and "+otherContext);
				int matchLength = getLongestMatchingSubSequence(pattern1, pattern2);
				if(matchLength > cache.get(context).split(" ").length/2){
					System.out.println("Found associativity between "+context +" and "+otherContext);
					associativeMap.get(context).add(otherContext);
					associativeMap.get(otherContext).add(context);
				}
				else{
					System.out.println("No associativity between "+context +" and "+otherContext);
				}				
			}
		}		
	}
	
	/**
	 * Parsing data
	 * @param data
	 * @return
	 */
	public String[] getOnlyIoTIDs(String data){
		String blocks[] = data.split(" ");
		String output[] = new String[blocks.length];
		int i =0;
		for(String block:blocks){
			output[i] = block.substring(0, block.indexOf(":"));
			i++;
		}
		return output;
	}
	
	/**
	 * Finds longest matching subsequence from data of 2 different contexts
	 * @param pattern1
	 * @param pattern2
	 * @return
	 */
	public static int getLongestMatchingSubSequence(String[] pattern1, String[] pattern2){
		String subsequence = "";
		int matchSize[][]  = new int[pattern1.length + 1][pattern2.length + 1];
		for(int i = 0; i < pattern1.length+1; i++){
			for(int j = 0; j < pattern2.length+1; j++){
				if (i == 0 || j == 0)
					matchSize[i][j] = 0;
				else if(pattern1[i-1].equals(pattern2[j-1])){
					matchSize[i][j] = matchSize[i-1][j-1] + 1;
				}
				else{
					int max = Math.max(matchSize[i][j-1], matchSize[i-1][j]);
					matchSize[i][j] = max;
				}
			}
		}
		for(int j = 0; j < pattern2.length; j++){
			if(matchSize[pattern1.length][j] +1 == matchSize[pattern1.length][j+1]){
				subsequence += ":"+pattern2[j];
			}
		}
		System.out.println("Matching pattern: "+subsequence);
		return matchSize[pattern1.length][pattern2.length];
	}

	/**
	 * Connects to root for fetching data
	 * @param rootHostName
	 * @return
	 */
	private String prepareData(String rootHostName){
		PrintWriter bw;
		String data  = "";		
		try(Socket socket = new Socket(rootHostName, 12345)){
			System.out.println("Preparing data with "+ socket.getInetAddress().getHostName());
			bw = new PrintWriter(socket.getOutputStream(), true);
			bw.println(Messages.sendData);
			BufferedReader din = new BufferedReader(new InputStreamReader (socket.getInputStream()));
			data = null;
			while((data = din.readLine()) == null);
		}
		catch (IOException e) {
			e.printStackTrace();
		}		
		return data.trim();
	}

	/**
	 * Updates associativity map
	 */
	public void updateAssociativity(){
		for(String context1: currentUpdates.keySet()){
			for(String context2: currentUpdates.keySet()){
				if(!context1.equals(context2)){
					if(currentUpdates.get(context1) == currentUpdates.get(context2)){
						associativeMap.get(context1).add(context2);
						associativeMap.get(context2).add(context1);
					}
				}
			}
		}
	}
	
	/**
	 * Develop later: can be substitute to longest common subsequence 
	 */
	public int getEditDistance(String context1, String context2){
		int editDistance = 0;
		if(contextMap.keySet().contains(context1))
			for(RootNode root: levelMap.get(contextMap.get(context1))){
				if(root.context.equals(context1)){
					
				}
			}
		return editDistance;
	}
	
	/**
	 * Update level of context profile if popularity increases
	 * @param context
	 */
	public void updateLevel(String context){
		System.out.println("Updated level of context: "+context);
		ArrayList<RootNode> levelContexts = levelMap.get(contextMap.get(context));
		int index = 0;
		for(RootNode root: levelContexts){
			index++;
			if(root.context.equals(context)){
				break;
			}			
		}
		popularityMap.put(context, 0);
		System.out.println("Updated level of "+context+" to "+index);
		/*RootNode root = levelMap.get(context).remove(index);
		levelMap.get(contextMap.get(context)).add(root);*/
	}	
	
	/**
	 * Pulls data of associated contexts
	 * @param otherContext
	 */
	public void cacheContext(String otherContext){
		if(contextMap.keySet().contains(otherContext))
		for(RootNode root: levelMap.get(contextMap.get(otherContext))){
			if(root.context.equals(otherContext)){
				cache.put(otherContext, prepareData(otherContext));
			}
		}
	}
}

/**
 * 
 * @author Manasi
 *
 */
class RootNode{
	String context;
	String hostName;
	
	public RootNode(String context, String hostName){
		this.context = context;
		this.hostName = hostName;
	}
}
