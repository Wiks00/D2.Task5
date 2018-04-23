using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Microsoft.ServiceBus.Messaging;
using PdfSharp.Drawing;
using PdfSharp.Pdf;

namespace BatchServer
{
    class ScanerSessionHandler : IMessageSessionHandler
    {
        private Dictionary<string, List<byte>> sessionFiels = new Dictionary<string, List<byte>>();

        public void OnCloseSession(MessageSession session)
        {
            CreatePDF(sessionFiels);
            sessionFiels = null;
        }

        public void OnMessage(MessageSession session, BrokeredMessage message)
        {
            if (message.ContentType.Equals("application/octet-stream", StringComparison.InvariantCultureIgnoreCase))
            {
                var body = message.GetBody<byte[]>();

                if (sessionFiels.TryGetValue(message.CorrelationId, out List<byte> fileArray))
                {
                    fileArray.AddRange(body);
                }
                else
                {
                    sessionFiels.Add(message.CorrelationId, body.ToList());
                }
            }
             
            message.Complete();
        }

        public void OnSessionLost(Exception exception)
        {
            Console.WriteLine($"MyMessageSessionHandler {exception.Message} OnSessionLost");

            OnError(sessionFiels);
        }

        private void CreatePDF(Dictionary<string, List<byte>> images)
        {
            var resultDirectory = Directory.CreateDirectory(Path.Combine(Environment.CurrentDirectory, "Result", DateTime.Today.ToString("dd-MM-yy")));
            var pdfName = Path.Combine(resultDirectory.FullName, $"{Guid.NewGuid().ToString()}.pdf");

            int i = 0;
            try
            {
                PdfDocument document = new PdfDocument();

                foreach (var fileArray in images.Values.Select(value => value.ToArray()))
                {
                    var page = document.AddPage();
                    using (var stream = new MemoryStream(fileArray))
                    {
                        XGraphics gfx = XGraphics.FromPdfPage(page);

                        gfx.DrawImage(XImage.FromStream(stream), new XRect(0, 0, page.Width, page.Height));
                    }
                }

                document.Save(pdfName);
                document.Dispose();
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
        }


        private void OnError(Dictionary<string, List<byte>> images)
        {
            var errorDirectory = Directory.CreateDirectory(Path.Combine(Environment.CurrentDirectory, "Broken", DateTime.Today.ToString("dd-MM-yy"), Guid.NewGuid().ToString()));

            foreach (var keyValue in images)
            {
                File.WriteAllBytes(Path.Combine(errorDirectory.FullName, keyValue.Key), keyValue.Value.ToArray());
            }
        }
    }
}
