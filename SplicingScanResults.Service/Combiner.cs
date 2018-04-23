using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Configuration;
using System.Drawing;
using System.IO;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.ServiceBus;
using Microsoft.ServiceBus.Messaging;
using MigraDoc.DocumentObjectModel;
using Newtonsoft.Json.Linq;
using PdfSharp.Drawing;
using PdfSharp.Pdf;
using Topshelf;
using ZXing;

namespace SplicingScanResults.Service
{
    public class Combiner : ServiceControl, IDisposable
    {
        private Regex validator;
        private FileSystemWatcher watcher;
        private BarcodeReader barcodeReader = new BarcodeReader();
        private ManualResetEventSlim workToDo = new ManualResetEventSlim(false);
        private ManualResetEventSlim fileSynchronizer = new ManualResetEventSlim(true);
        private CancellationTokenSource tokenSource;
        //private ConcurrentQueue<FileSystemEventArgs> fileQueue = new ConcurrentQueue<FileSystemEventArgs>();

        private FileSystemEventArgs eventArg;

        private MessagingFactory messagingFactory;
        private MessageSender imageSender;
        private MessageSender statusSender;
        private MessageReceiver configReciver;



        private readonly Guid serviceId;

        private const long maxMessageSize = 256 * 1024;

        public TimeSpan Timeout { get; set; }
        private string StopWord { get; set; }
        public string RootDirectory { get; }


        public Combiner(string scanerRootPath)
        {
            if (!Directory.Exists(scanerRootPath))
            {
                Directory.CreateDirectory(scanerRootPath);
            }

            RootDirectory = scanerRootPath;

            validator = new Regex(@"^img_(\d{3})\.(jpg|png|bmp)$", RegexOptions.Compiled | RegexOptions.IgnoreCase);

            watcher = new FileSystemWatcher(RootDirectory)
            {
                IncludeSubdirectories = false,
                EnableRaisingEvents = false
            };

            watcher.Created += (source, e) => 
            {
                workToDo.Set();
                eventArg = e;
            };

            serviceId = Guid.NewGuid();

            Timeout = TimeSpan.FromSeconds(5);
        }

        public bool Start(HostControl hostControl)
        {
            watcher.EnableRaisingEvents = true;
            tokenSource = new CancellationTokenSource();

            InitConncection();

            configReciver.OnMessage(message =>
            {
                if (message.ContentType.Equals("application/json"))
                {
                    using (var stream = new StreamReader(message.GetBody<Stream>()))
                    {
                        var newConfiguration = JObject.Parse(stream.ReadToEnd());

                        Timeout = TimeSpan.Parse(newConfiguration["timeout"].Value<string>());
                        StopWord = newConfiguration["stopWord"].Value<string>();
                    }

                    statusSender.Send(new BrokeredMessage("Configuration changed") { ContentType = "text/plain", CorrelationId = serviceId.ToString() });

                    message.Complete();
                }

                message.DeadLetter("Msssage type", "Message type need to be json");
            }, new OnMessageOptions {AutoComplete = false});

            Task.Run(() =>
            {
                Match match;
                Result result;
                HashSet<string> listOfImages = new HashSet<string>();
                int prevNumber = -1;

                while (!tokenSource.IsCancellationRequested)
                {
                    statusSender.Send(new BrokeredMessage("Waiting for the new files") { ContentType = "text/plain", CorrelationId = serviceId.ToString() });
                    if (workToDo.Wait(Timeout))
                    {
                        statusSender.Send(new BrokeredMessage("File processing") { ContentType = "text/plain", CorrelationId = serviceId.ToString() });
                        workToDo.Reset();

                        match = validator.Match(eventArg.Name);

                        if (match.Success)
                        {
                            var imageNumber = int.Parse(match.Groups[1].Value);

                            if (prevNumber == -1 || imageNumber - prevNumber == 1)
                            {
                                try
                                {
                                    Task.Delay(TimeSpan.FromMilliseconds(100)).Wait();
                                    fileSynchronizer.Wait();
                                    using (var imageStream = new FileStream(eventArg.FullPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
                                    using (var image = new Bitmap(imageStream))
                                    {
                                        result = barcodeReader.Decode(image);
                                    }
                                    fileSynchronizer.Set();

                                    prevNumber = imageNumber;

                                    listOfImages.Add(eventArg.FullPath);

                                    if (result?.Text != null) // .Equals(StopWord))
                                    {
                                        InitTask(listOfImages);

                                        prevNumber = -1;
                                    }
                                }
                                catch (IOException ex) when (ex.Message.Contains("because it is being used by another process"))
                                {
                                    Console.WriteLine(ex.Message);
                                }
                            }
                            else
                            {
                                InitTask(listOfImages);

                                prevNumber = imageNumber;

                                listOfImages.Add(eventArg.FullPath);
                            }
                        }
                        else
                        {
                            File.Delete(eventArg.FullPath);
                        }

                    }
                    else
                    {
                        statusSender.Send(new BrokeredMessage("Timeout expired") { ContentType = "text/plain", CorrelationId = serviceId.ToString() });

                        if (listOfImages.Count > 0)
                        {
                            InitTask(listOfImages);
                        }

                        prevNumber = -1;
                    }
                }
            }, tokenSource.Token);

            return true;
        }

        public bool Stop(HostControl hostControl)
        {
            watcher.EnableRaisingEvents = false;

            tokenSource.Cancel();

            messagingFactory.Close();
            imageSender.Close();
            configReciver.Close();

            return true;
        }

        private void InitTask(HashSet<string> imageList)
        {
            Task pdfCreationTask = new Task(images => Send((List<string>)images),
                new List<string>(imageList));

            pdfCreationTask.Start();
            imageList.Clear();
        }

        private void Send(List<string> images)
        {
            statusSender.Send(new BrokeredMessage("Batch processing") { ContentType = "text/plain", CorrelationId = serviceId.ToString() });

            int i = 0;
            try
            {
                Guid sessionId = Guid.NewGuid();

                for (; i < images.Count; i++)
                {
                    var image = images[i];

                    fileSynchronizer.Wait();
                    var file = new FileInfo(image);
                    var bytes = File.ReadAllBytes(image);
                    fileSynchronizer.Set();

                    Guid correlationId = Guid.NewGuid();

                    if (file.Length > maxMessageSize)
                    {
                       

                        var slices = Math.Ceiling((double)file.Length / maxMessageSize);
                        int take = (int)Math.Floor(bytes.Length / slices);
                        int skip = 0;

                        while (slices != 0)
                        {
                            var filePart = bytes.Skip(skip).Take(take > bytes.Length ? bytes.Length - skip : take).ToArray();
                            skip += take;

                            imageSender.Send(new BrokeredMessage(filePart)
                            {
                                ContentType = "application/octet-stream",
                                Label = "Success",
                                SessionId = sessionId.ToString(),
                                CorrelationId = correlationId.ToString()
                            });

                            slices--;
                        } 
                    }
                    else
                    {
                        imageSender.Send(new BrokeredMessage(bytes)
                        {
                            ContentType = "application/octet-stream",
                            Label = "Success",
                            SessionId = sessionId.ToString(),
                            CorrelationId = correlationId.ToString()
                        });
                    }

                }

                fileSynchronizer.Wait();
                images.ForEach(File.Delete);
                fileSynchronizer.Set();
            }
            catch(Exception ex)
            {
                fileSynchronizer.Set();

                Console.WriteLine(ex.Message);

                OnError(images, i, ex.Message);
            }
        }

        private void OnError(List<string> files, int index, string message)
        {
            var errorDirectory =  Directory.CreateDirectory(Path.Combine(RootDirectory, "Broken", DateTime.Today.ToString("dd-MM-yy"), Guid.NewGuid().ToString()));

            for (var i = 0; i < files.Count; i++)
            {
                var fileName = Path.GetFileName(files[i]);

                fileSynchronizer.Wait();
                File.Move(Path.Combine(RootDirectory, fileName), Path.Combine(errorDirectory.FullName, i == index ? $"{message}_{fileName}" : fileName));
                fileSynchronizer.Set();
            }
        }

        private void InitConncection()
        {
            messagingFactory = MessagingFactory.CreateFromConnectionString(ConfigurationManager.ConnectionStrings["AzureSB"].ConnectionString);
            messagingFactory.RetryPolicy = new RetryExponential(TimeSpan.FromSeconds(1), TimeSpan.FromMinutes(5), 10);


            imageSender = messagingFactory.CreateMessageSender(ConfigurationManager.AppSettings["OutQ"]);
            statusSender = messagingFactory.CreateMessageSender(ConfigurationManager.AppSettings["StatusQ"]);
            configReciver = messagingFactory.CreateMessageReceiver(ConfigurationManager.AppSettings["InQ"]);
        }

        public void Dispose()
        {
            watcher?.Dispose();
        }
    }
}
