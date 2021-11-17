import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.*;


import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;



public class queueApp {

    /*
    * Subir o LocalStack na porta 4566 com nome da fila: queueJava
    * */
    private static String LOCALSTACK_HOST = "http://localhost:4566";
    private static String QUEUE_URL = "http://localhost:4566/000000000000/queueJava";

    private static SqsClient createClient(Region region){
        SqsClient sqsClient = SqsClient.builder()
                .region(region)
                .endpointOverride(URI.create(LOCALSTACK_HOST))
                .build();
        return sqsClient;
    }

    private static List<Message> getMessages(SqsClient queueClient ,int waitTime, int maxNumberMessages){
        System.out.println("Reading messages sent to your queue");
        ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
                .queueUrl(QUEUE_URL)
                .waitTimeSeconds(waitTime)
                .maxNumberOfMessages(maxNumberMessages)
                .build();
        List<Message> messageList =  queueClient.receiveMessage(receiveMessageRequest).messages();
        System.out.println("Messages read");
        return messageList;
    }

    private static void printAndDelete(SqsClient queueClient, List<Message> messageList){
        for(Message m : messageList){
            System.out.println("Your message is: ");
            System.out.println(m.body());
            DeleteMessageRequest deleteMessageRequest = DeleteMessageRequest.builder()
                    .queueUrl(QUEUE_URL)
                    .receiptHandle(m.receiptHandle())
                    .build();
            queueClient.deleteMessage(deleteMessageRequest);
        }
    }

    private static void sendMessage(SqsClient queueClient, String message, int delaySeconds){
        queueClient.sendMessage(SendMessageRequest.builder()
                .queueUrl(QUEUE_URL)
                .messageBody(message)
                .delaySeconds(delaySeconds)
                .build());
    }

    public static void main(String[] args){

        Scanner in = new Scanner(System.in);
        SqsClient sqsClient = createClient(Region.US_EAST_1);
        List<Message> messageList = new ArrayList<>();

        boolean running = true;
        String cmd = "";
        while (running) {
            System.out.println("Write a command");
            cmd = in.nextLine();
            switch (cmd) {
                case "read" -> messageList.addAll(getMessages(sqsClient, 0, 5));
                case "consume" -> printAndDelete(sqsClient, messageList);
                case "send" -> {
                    System.out.println("Write your message: ");
                    String message = in.nextLine();
                    System.out.println("Your message is" + message);
                    sendMessage(sqsClient, message, 0);
                }
                case "exit" -> running = false;
                default -> System.out.println("Enter a valid command");
            }
        }

    }
}
