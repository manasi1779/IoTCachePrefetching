import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.UnknownHostException;
import java.util.Scanner;

public class CEPEngine extends Thread{
	
	static ServerSocket aServerSocket;
 	int port = 12345;
 	long timer = 0;

	public static void main(String[] args) {
		CEPEngine cep = new CEPEngine();
		cep.start();
		System.out.println("Starting Complex Event Procesing Engine");
		cep.timer = System.currentTimeMillis(); 
	}
	
	public CEPEngine(){
		try {
			aServerSocket = new ServerSocket(port);
		} catch (IOException e){
			e.printStackTrace();
		}
	}
	
	public void run(){
		Scanner s = new Scanner(System.in);
		System.out.println("Enter router name to connect to:");
		String router2 = s.nextLine();
		try {
			if(!router2.equals("null")){
				Socket router = new Socket(router2, 12345);
				PrintWriter pw = new PrintWriter(router.getOutputStream(), true); 
				pw.println(Messages.addRouter);
				Thread processorInstance = new ComplexEventProcessor(Messages.addRouter, router);
				processorInstance.start();
				router.close();
			}
		} catch (UnknownHostException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		while(true){
			try{
				Socket clnt = aServerSocket.accept();
		//		System.out.println(clnt.getInetAddress().getHostName()+".cs.rit.edu " + "connected");
				BufferedReader din = new BufferedReader (
						new InputStreamReader (clnt.getInputStream()));
		//		System.out.println("Waiting for operation");
				String operation;
				while((operation = din.readLine()) == null);
		//		System.out.println("Received operation: "+operation);
		//		System.out.println("Starting Complex Event Processor thread");
				Thread processorInstance = new ComplexEventProcessor(operation, clnt);
				processorInstance.start();
				//din.close();
		//		long now = System.currentTimeMillis();
				/*if(now - timer > 5000 && !ComplexEventProcessor.checkingUpdate){
					timer = now;
					ComplexEventProcessor.checkingUpdate = true;
					Thread updateChecker = new Thread(new ComplexEventProcessor(), "UpdateChecker");
					updateChecker.start();
				}*/
			} catch (IOException e){
				e.printStackTrace();
			}		
		}
	}

}
