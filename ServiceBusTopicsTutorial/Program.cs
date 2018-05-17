using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using System;
using System.IO;
using System.Text;
using System.Threading.Tasks;

namespace ServiceBusTopicsTutorial
{
    class Program
    {
        static TopicClient sendClient;
        static SubscriptionClient sportsSubscription;
        static SubscriptionClient newsSubscription;
        static SubscriptionClient weatherSubscription;

        public static void Main()
        {
            Console.WriteLine("======================================================");
            Console.WriteLine(" SERVICE BUS - TOPICS DEMO");
            Console.WriteLine("======================================================");

            MainAsync().GetAwaiter().GetResult();
        }


        static async Task MainAsync()
        {
            const string sendPolicyConnectionString = "Endpoint=sb://queuesdemo.servicebus.windows.net/;SharedAccessKeyName=SendPolicy;SharedAccessKey=RkN8xIdtJKwRmSSIz6j6LAwSPsqUVK7B6cVBETGyoO8=;EntityPath=mytopic";
            const string listenPolicyConnectionString = "Endpoint=sb://queuesdemo.servicebus.windows.net/;SharedAccessKeyName=ListenPolicy;SharedAccessKey=ZPIrr7wDFPnGipss6nPAzDNVtPAioCc62TdWK0dyusg=;EntityPath=mytopic";

            sendClient = TopicClient.CreateFromConnectionString(sendPolicyConnectionString);

            sportsSubscription = SubscriptionClient.CreateFromConnectionString(listenPolicyConnectionString, "mytopic", "sports");
            newsSubscription = SubscriptionClient.CreateFromConnectionString(listenPolicyConnectionString, "mytopic", "breakingNews");
            weatherSubscription = SubscriptionClient.CreateFromConnectionString(listenPolicyConnectionString, "mytopic", "weather");

            InitializeReceiver(sportsSubscription, ConsoleColor.Cyan);
            InitializeReceiver(newsSubscription, ConsoleColor.Green);
            InitializeReceiver(weatherSubscription, ConsoleColor.Yellow);

            await SendMessagesAsync();

            await Task.WhenAny(
                Task.Run(() => Console.ReadKey()),
                Task.Delay(TimeSpan.FromSeconds(10))
            );

            await sportsSubscription.CloseAsync();
            await newsSubscription.CloseAsync();
            await weatherSubscription.CloseAsync();

        }

        private static async Task SendMessagesAsync()
        {
            dynamic data = new[]
              {
                new {name = "Einstein", firstName = "Albert"},
                new {name = "Heisenberg", firstName = "Werner"},
                new {name = "Curie", firstName = "Marie"},
                new {name = "Hawking", firstName = "Steven"},
                new {name = "Newton", firstName = "Isaac"},
                new {name = "Bohr", firstName = "Niels"},
                new {name = "Faraday", firstName = "Michael"},
                new {name = "Galilei", firstName = "Galileo"},
                new {name = "Kepler", firstName = "Johannes"},
                new {name = "Kopernikus", firstName = "Nikolaus"}
            };


            for (int i = 0; i < data.Length; i++)
            {
                var message = new BrokeredMessage(new MemoryStream(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(data[i]))))
                {
                    ContentType = "application/json",
                    Label = "Scientist",
                    MessageId = i.ToString(),
                    TimeToLive = TimeSpan.FromMinutes(2)
                };

                await sendClient.SendAsync(message);

                lock (Console.Out)
                {
                    Console.ForegroundColor = ConsoleColor.Yellow;
                    Console.WriteLine("Message sent: Id = {0}", message.MessageId);
                    Console.ResetColor();
                }
            }
        }

        static void InitializeReceiver(SubscriptionClient receiver, ConsoleColor color)
        {
            // register the OnMessageAsync callback
            receiver.OnMessageAsync(
                async message =>
                {

                    if (message.Label != null &&
                        message.ContentType != null &&
                        message.Label.Equals("Scientist", StringComparison.InvariantCultureIgnoreCase) &&
                        message.ContentType.Equals("application/json", StringComparison.InvariantCultureIgnoreCase))
                    {
                        var body = message.GetBody<Stream>();

                        dynamic scientist = JsonConvert.DeserializeObject(new StreamReader(body, true).ReadToEnd());

                        lock (Console.Out)
                        {
                            Console.ForegroundColor = color;
                            Console.WriteLine(
                                "\t\t\t\tMessage received: \n\t\t\t\t\t\tMessageId = {0}, \n\t\t\t\t\t\tSequenceNumber = {1}, \n\t\t\t\t\t\tEnqueuedTimeUtc = {2}," +
                                "\n\t\t\t\t\t\tExpiresAtUtc = {5}, \n\t\t\t\t\t\tContentType = \"{3}\", \n\t\t\t\t\t\tSize = {4},  \n\t\t\t\t\t\tContent: [ firstName = {6}, name = {7} ]",
                                message.MessageId,
                                message.SequenceNumber,
                                message.EnqueuedTimeUtc,
                                message.ContentType,
                                message.Size,
                                message.ExpiresAtUtc,
                                scientist.firstName,
                                scientist.name);
                            Console.ResetColor();
                        }
                    }
                    await message.CompleteAsync();
                },
                new OnMessageOptions { AutoComplete = false, MaxConcurrentCalls = 1 });
        }
    }
}