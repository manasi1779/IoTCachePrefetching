import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;

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
		while(true){
			try{
				Socket clnt = aServerSocket.accept();
			//	System.out.println(clnt.getInetAddress().getHostName()+".cs.rit.edu " + "connected");
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
				long now = System.currentTimeMillis();
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
