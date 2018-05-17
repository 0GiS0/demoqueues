using Microsoft.Azure;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Queue;
using System;

namespace QueueTutorial
{
    class Program
    {
        static void Main(string[] args)
        {
            //1. Retrieve storage account from connection string
            var storageAccount = CloudStorageAccount.Parse(CloudConfigurationManager.GetSetting("StorageConnectionString"));

            //2. Create the queue client.
            var queueClient = storageAccount.CreateCloudQueueClient();

            //3. Retrieve a reference to a queue
            var queue = queueClient.GetQueueReference("myqueue");

            //4. Create the queue if it doesn't already exist
            queue.CreateIfNotExists();

            //5. Create a message and add it to the queue
            var message = new CloudQueueMessage("Hello, World");
            queue.AddMessage(message);

            //6. You can peek at the message in front of a queue without removing it from the queue by calling the PeekMessage method.
            var peekedMessage = queue.PeekMessage();

            //7. Display message
            Console.WriteLine($"Peeked message: {peekedMessage.AsString}");

            Console.ReadLine();

            //8. You can change the contents of a message in-place in the queue.
            // If the message represents a work task, you could use this feature to update the status of the work task.
            //8.1 Get the message from the queue and update the message contents
            message = queue.GetMessage();

            Console.WriteLine($"Message before the update: {message.AsString}");

            message.SetMessageContent("Updated contents.");
            queue.UpdateMessage(message,
                TimeSpan.FromSeconds(0), // Make it invisible for another 60 seconds.
            MessageUpdateFields.Content | MessageUpdateFields.Visibility);


            //You can get an estimate of the number of messages in a queue. The FetchAttributes method asks the Queue service to 
            //retireve the queue attributes, including the message count.
            queue.FetchAttributes();

            Console.WriteLine($"Number of messages in queue: {queue.ApproximateMessageCount}");

            //9. Your code de-queues a message from a queue in two steps.
            //A message returned from GetMessage becomes invisible to any other code reading messages from this queue.
            var retrievedMessage = queue.GetMessage();

            Console.WriteLine($"Retrieved message: {retrievedMessage.AsString}");

            //To finish removing the message from the queue, you must also call DeleteMessage.
            queue.DeleteMessage(retrievedMessage);

            //Delete a queue
            queue.Delete();

            Console.WriteLine("Queue deleted");

            Console.ReadLine();
        }
    }
}