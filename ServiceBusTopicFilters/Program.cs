using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using System;
using System.Configuration;
using System.Text;
using System.Threading.Tasks;

namespace ServiceBusTopicFilters
{
    class Program
    {
        //https://github.com/Azure/azure-service-bus/tree/master/samples/DotNet/Microsoft.Azure.ServiceBus/TopicFilters

        const string TopicName = "TopicFilterSampleTopic";
        const string SubscriptionAllMessages = "AllOrders";
        const string SubscriptionColorBlueSize10Orders = "ColorBlueSize10Orders";
        const string SubscriptionColorRed = "ColorRed";
        const string SubscriptionHighPriorityOrders = "HighPriorityOrders";
        static string ServiceBusconnectionString = ConfigurationManager.AppSettings["connectionString"];
        static NamespaceManager namespaceManager;
        public static void Main(string[] args)
        {
            CreateTopicAndSubscriptionsWithFilters();
            Run().GetAwaiter().GetResult();
            DeleteTopic();
            Console.WriteLine("All done!");
            Console.ReadLine();
        }

        private static void DeleteTopic()
        {
            namespaceManager.DeleteTopic(TopicName);
        }

        private static void CreateTopicAndSubscriptionsWithFilters()
        {
            // PART 1 - CREATE THE TOPIC
            Console.WriteLine("Creating the topic");
            namespaceManager = NamespaceManager.CreateFromConnectionString(ServiceBusconnectionString);
            if (!namespaceManager.TopicExists(TopicName))
            {
                // Configure Topic Settings.
                var topic = new TopicDescription(TopicName)
                {
                    MaxSizeInMegabytes = 1024,
                    DefaultMessageTimeToLive = TimeSpan.FromMinutes(5)
                };

                namespaceManager.CreateTopic(topic);
            }

            Console.WriteLine("Done!");
            Console.WriteLine("Creating subscriptions");

            //PART 2 - CREATE THE SUBSCRIPTIONS

            namespaceManager.CreateSubscription(TopicName, SubscriptionAllMessages, new SqlFilter("1=1"));
            namespaceManager.CreateSubscription(TopicName, SubscriptionColorBlueSize10Orders, new SqlFilter("color = 'blue' AND quantity = 10"));
            namespaceManager.CreateSubscription(TopicName, SubscriptionColorRed, new SqlFilter("color = 'red'"));
            namespaceManager.CreateSubscription(TopicName, SubscriptionHighPriorityOrders, new CorrelationFilter("high"));
        }

        public static async Task Run()
        {
            // This sample demonstrates how to use advanced filters with ServiceBus topics and subscriptions.
            // The sample creates a topic and 3 subscriptions with different filter definitions.
            // Each receiver will receive matching messages depending on the filter associated with a subscription.

            await SendAndReceiveTestsAsync($"{ServiceBusconnectionString};EntityPath={TopicName}");

            Console.WriteLine();

        }

        static async Task SendAndReceiveTestsAsync(string connectionString)
        {
            // Send sample messages.
            await SendMessagesToTopicAsync(connectionString);

            // Receive messages from subscriptions.
            await ReceiveAllMessageFromSubscription(connectionString, SubscriptionAllMessages);
            await ReceiveAllMessageFromSubscription(connectionString, SubscriptionColorBlueSize10Orders);
            await ReceiveAllMessageFromSubscription(connectionString, SubscriptionColorRed);
            await ReceiveAllMessageFromSubscription(connectionString, SubscriptionHighPriorityOrders);
        }


        static async Task SendMessagesToTopicAsync(string connectionString)
        {
            // Create client for the topic.
            var topicClient = TopicClient.CreateFromConnectionString(connectionString);

            // Create a message sender from the topic client.

            Console.WriteLine("\nSending orders to topic.");

            // Now we can start sending orders.
            await Task.WhenAll(
                SendOrder(topicClient, new Order()),
                SendOrder(topicClient, new Order { Color = "blue", Quantity = 5, Priority = "low" }),
                SendOrder(topicClient, new Order { Color = "red", Quantity = 10, Priority = "high" }),
                SendOrder(topicClient, new Order { Color = "yellow", Quantity = 5, Priority = "low" }),
                SendOrder(topicClient, new Order { Color = "blue", Quantity = 10, Priority = "low" }),
                SendOrder(topicClient, new Order { Color = "blue", Quantity = 5, Priority = "high" }),
                SendOrder(topicClient, new Order { Color = "blue", Quantity = 10, Priority = "low" }),
                SendOrder(topicClient, new Order { Color = "red", Quantity = 5, Priority = "low" }),
                SendOrder(topicClient, new Order { Color = "red", Quantity = 10, Priority = "low" }),
                SendOrder(topicClient, new Order { Color = "red", Quantity = 5, Priority = "low" }),
                SendOrder(topicClient, new Order { Color = "yellow", Quantity = 10, Priority = "high" }),
                SendOrder(topicClient, new Order { Color = "yellow", Quantity = 5, Priority = "low" }),
                SendOrder(topicClient, new Order { Color = "yellow", Quantity = 10, Priority = "low" })
                );

            Console.WriteLine("All messages sent.");
        }

        static async Task SendOrder(TopicClient topicClient, Order order)
        {
            var message = new BrokeredMessage(Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(order)))
            {
                CorrelationId = order.Priority,
                Label = order.Color,
                Properties =
                {
                    { "color", order.Color },
                    { "quantity", order.Quantity },
                    { "priority", order.Priority }
                }
            };
            await topicClient.SendAsync(message);

            Console.WriteLine("Sent order with Color={0}, Quantity={1}, Priority={2}", order.Color, order.Quantity, order.Priority);
        }

        static async Task ReceiveAllMessageFromSubscription(string connectionString, string subsName)
        {
            var receivedMessages = 0;

            // Create subscription client.
            var subscriptionClient = SubscriptionClient.CreateFromConnectionString(connectionString, TopicName, subsName);

            // Create a receiver from the subscription client and receive all messages.
            Console.WriteLine("\nReceiving messages from subscription {0}.", subsName);

            while (true)
            {
                var receivedMessage = await subscriptionClient.ReceiveAsync(TimeSpan.FromSeconds(10));
                if (receivedMessage != null)
                {
                    foreach (var prop in receivedMessage.Properties)
                    {
                        Console.Write("{0}={1},", prop.Key, prop.Value);
                    }
                    Console.WriteLine("CorrelationId={0}", receivedMessage.CorrelationId);
                    receivedMessages++;
                }
                else
                {
                    // No more messages to receive.
                    break;
                }
            }
            Console.WriteLine("Received {0} messages from subscription {1}.", receivedMessages, subsName);
        }
    }
}
