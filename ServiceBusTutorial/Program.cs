using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using System;
using System.Configuration;
using System.IO;
using System.Text;
using System.Threading.Tasks;


namespace ServiceBusTutorial
{
    class Program
    {
        static string SendPolicyConnectionString = ConfigurationManager.AppSettings["SendPolicyConnectionString"];
        static string ListenPolicyConnectionString = ConfigurationManager.AppSettings["ListenPolicyConnectionString"];
        static QueueClient sendClient;
        static QueueClient receiveClient;

        static void Main(string[] args)
        {
            Console.WriteLine("======================================================");
            Console.WriteLine(" SERVICE BUS - QUEUES DEMO");
            Console.WriteLine("======================================================");

            MainAsync().GetAwaiter().GetResult();
        }

        private static async Task MainAsync()
        {
            const int numberOfMessages = 10;
            sendClient = QueueClient.CreateFromConnectionString(SendPolicyConnectionString);

            Console.WriteLine("======================================================");
            Console.WriteLine("Press ENTER key to exit after sending all the messages.");
            Console.WriteLine("======================================================");

            // Send messages.
            await SendMessagesAsync(numberOfMessages);

            Console.ReadKey();

            //Receive messages.
            // Register the queue message handler and receive messages in a loop
            ReceiveMessagesAsync();

            Console.WriteLine("======================================================");
            Console.WriteLine("Press ENTER key to exit after receiving all the messages.");
            Console.WriteLine("======================================================");

            Console.ReadKey();

            await sendClient.CloseAsync();

        }

        static async Task SendMessagesAsync(int numberOfMessagesToSend)
        {
            dynamic data = new[]
            {
                new {firstName = "Gisela", lastName = "Torres"},
                new {firstName = "Mykola", lastName = "Kovalenko"},
                new {firstName = "Miguel Ángel", lastName = "Rivera"},
                new {firstName = "Veronica", lastName = "Bas"},
                new {firstName = "Cristabel", lastName = "Talavera"},
                new {firstName = "Daniel", lastName = "Urbina"},
                new {firstName = "David", lastName = "Cervigón"},
                new {firstName = "Jose Ángel", lastName = "Fernandez"},
                new {firstName = "Juan Manuel", lastName = "Rey"},
                new {firstName = "Jose", lastName = "Maldonado"}
            };

            try
            {
                for (var i = 0; i < data.Length; i++)
                {
                    // Create a new message to send to the queue.
                    var message = new BrokeredMessage(new MemoryStream(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(data[i]))))
                    {
                        ContentType = "application/json",
                        Label = "Test",
                        MessageId = i.ToString(),
                        TimeToLive = TimeSpan.FromMinutes(2)
                    };

                    // Send the message to the queue.
                    await sendClient.SendAsync(message);

                    lock (Console.Out)
                    {
                        Console.ForegroundColor = ConsoleColor.Yellow;
                        Console.WriteLine("Message sent: Id = {0}", message.MessageId);
                        Console.ResetColor();
                    }
                }
            }
            catch (Exception exception)
            {
                Console.WriteLine($"{DateTime.Now} :: Exception: {exception.Message}");
            }
        }

        private static void ReceiveMessagesAsync()
        {
            receiveClient = QueueClient.CreateFromConnectionString(ListenPolicyConnectionString);

            receiveClient.OnMessageAsync(ProcessMessage, new OnMessageOptions { AutoComplete = false, MaxConcurrentCalls = 1 });
        }

        private static async Task ProcessMessage(BrokeredMessage message)
        {
            try
            {
                var body = message.GetBody<Stream>();

                dynamic csa = JsonConvert.DeserializeObject(new StreamReader(body, true).ReadToEnd());

                lock (Console.Out)
                {
                    Console.ForegroundColor = ConsoleColor.Cyan;
                    Console.WriteLine(
                               "\t\t\t\tMessage received: \n\t\t\t\t\t\tMessageId = {0}, \n\t\t\t\t\t\tSequenceNumber = {1}, \n\t\t\t\t\t\tEnqueuedTimeUtc = {2}," +
                               "\n\t\t\t\t\t\tExpiresAtUtc = {5}, \n\t\t\t\t\t\tContentType = \"{3}\", \n\t\t\t\t\t\tSize = {4},  \n\t\t\t\t\t\tContent: [ firstName = {6}, lastName = {7} ]",
                               message.MessageId,
                               message.SequenceNumber,
                               message.EnqueuedTimeUtc,
                               message.ContentType,
                               message.Size,
                               message.ExpiresAtUtc,
                               csa.firstName,
                               csa.lastName);
                    Console.ResetColor();
                }

                await message.CompleteAsync();

            }
            catch (Exception ex)
            {

                throw ex;
            }

        }

    }
}
