using System;
using System.Collections.Generic;
using System.Configuration;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace BatchServer
{
    class Program
    {
        static void Main(string[] args)
        {
            var messagingFactory =
                MessagingFactory.CreateFromConnectionString(ConfigurationManager.ConnectionStrings["AzureSB"]
                    .ConnectionString);
            messagingFactory.RetryPolicy = new RetryExponential(TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(5), 10);

            var batchClient = messagingFactory.CreateQueueClient(ConfigurationManager.AppSettings["InQ"]);
            batchClient.RegisterSessionHandler(typeof(ScanerSessionHandler), new SessionHandlerOptions { AutoComplete = false });

            var statusClient = messagingFactory.CreateQueueClient(ConfigurationManager.AppSettings["StatusQ"]);
            var configClient = messagingFactory.CreateQueueClient(ConfigurationManager.AppSettings["OutQ"]);

            statusClient.OnMessage(message =>
            {
                if (message.ContentType.Equals("text/plain"))
                {
                    Console.WriteLine($"Service: {message.CorrelationId} now is on '{message.GetBody<string>()}' stage");
                    message.Complete();
                }

                message.DeadLetter();

            }, new OnMessageOptions{AutoComplete = false});

            var configPath = Path.Combine(Environment.CurrentDirectory,
                ConfigurationManager.AppSettings["ConfigFolder"]);

            if (!Directory.Exists(configPath))
            {
                Directory.CreateDirectory(configPath);
                using (var file = File.CreateText(Path.Combine(configPath, ConfigurationManager.AppSettings["ConfigFileName"])))
                {
                    file.Write("{\"timeout\": \"00:00:05\", \"stopWord\": \"ಠ_ಠ\"}");
                }

            }

            var watcher = new FileSystemWatcher(configPath, ConfigurationManager.AppSettings["ConfigFileName"])
                {
                    IncludeSubdirectories = false,
                    EnableRaisingEvents = true
                };

            watcher.Changed += (sender, eventArgs) =>
            {
                Task.Delay(TimeSpan.FromSeconds(1)).Wait();
                configClient.Send(new BrokeredMessage(new MemoryStream(File.ReadAllBytes(eventArgs.FullPath))) { ContentType = "application/json" });
            };

            while (true)
            {
                Task.Delay(TimeSpan.FromMilliseconds(50)).Wait();
            }
        }
    }
}
