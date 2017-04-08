import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.Scanner;

public class Client{
	
	String serverAddrss = "glados.cs.rit.edu";
	Scanner scanner = new Scanner(System.in);	

	public static void main(String[] args) {
		Client client = new Client();
		client.connect();
	}
	
	public void connect(){
		try(Socket server = new Socket(serverAddrss, 12345)){
			PrintWriter	bw = new PrintWriter(server.getOutputStream(), true);
			bw.println("requestData");
			System.out.println("Enter context");
			bw.println(scanner.nextLine());
			BufferedReader din = new BufferedReader (
					new InputStreamReader(server.getInputStream()));
			String status;
			while((status = din.readLine()) == null);
			System.out.println(status);
		} catch (IOException e){
			e.printStackTrace();
		}
	}

}
