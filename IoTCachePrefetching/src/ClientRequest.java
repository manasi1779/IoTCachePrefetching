import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Scanner;

/**
 * Thread to serve operations at client
 * @author Manasi
 *
 */
public class ClientRequest extends Thread{
	
	static String serverAddress;
	static Scanner scanner = new Scanner(System.in);	
	static Object usingScanner = new Object();
	static HashMap<String, String> cache = new HashMap();
	
	public static void main(String[] args) {
		ClientRequest client = new ClientRequest();		
	}
	
	public ClientRequest(){
		connect();
		Thread dataRequestor = new Thread(this);
		dataRequestor.setName("DataRequestor");
		dataRequestor.start();
		Thread requestServer = new Thread(this);   
		requestServer.setName("requestServer");
		requestServer.start();
	}
	
	public void run(){
		if(Thread.currentThread().getName().equals("DataRequestor"))
			requestData();
		else
			serveRequest();
	}
	
	public void requestData(){
		try {			
			while(true){
				String context; 
				synchronized(usingScanner){
					System.out.println("Enter Context to request data");
					context = scanner.nextLine();
				}
					Socket socket = new Socket(serverAddress, 12345);
					PrintWriter	bw = new PrintWriter(socket.getOutputStream(), true);
					bw.println(Messages.requestData);
					System.out.println("Sending context info "+context);
					BufferedReader din = new BufferedReader (
							new InputStreamReader(socket.getInputStream()));
					String command;
					while((command = din.readLine()) == null);
					if(command.equals(Messages.getContext)){
						bw.println(context);
					}				
					String status;
					while((status = din.readLine()) == null);
					System.out.println(status);
					socket.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
	
	public void connect(){
		System.out.println("Registering client");
		String cep;
		System.out.println("Enter the CEP to connect to");
		cep = scanner.nextLine();
		try{		
				serverAddress = cep+".cs.rit.edu";
				Socket socket = new Socket(serverAddress, 12345);
				PrintWriter	bw = new PrintWriter(socket.getOutputStream(), true);
				bw.println(Messages.connect);
				socket.close();
				System.out.println("Welcome to IoT system");
			} catch (IOException e){
				e.printStackTrace();
			}
	}
	
	public void serveRequest(){
		try {				
			ServerSocket server = new ServerSocket(54321);
			while(true){
				Socket socket = server.accept();
				System.out.println(socket.getInetAddress().getHostName()+" connected");
				BufferedReader reader = new BufferedReader(new InputStreamReader(socket.getInputStream()));
				String operation;
				while((operation = reader.readLine()) == null);
				System.out.println("Received request "+operation +" from "+socket.getInetAddress().getHostName());
				switch(operation){
					case Messages.store:{
						PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
						pw.println(Messages.sendData);					
						String data;
						while((data = reader.readLine()) == null);
						String splitData[] = data.split("#");
						cache.put(splitData[0], splitData[1]);
						System.out.println("Stored "+splitData[0]);
						break;
					}
					case Messages.getData:{
						PrintWriter pw = new PrintWriter(socket.getOutputStream(), true);
						String context;
						while((context = reader.readLine()) == null);
						pw.println(cache.get(context));
						break;
					}
				}
				socket.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}		
	}
}
