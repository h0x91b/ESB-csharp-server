using ProtoBuf;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using ZeroMQ;

namespace ESBServer
{
    public class Publisher
    {
        public int port { get; internal set; }
        public string host { get; internal set; }
        public string guid { get; internal set; }

        ZmqContext ctx = null;
        ZmqSocket socket = null;

        public Publisher(string _guid, string _host, int _port)
        {
            guid = _guid;
            host = _host;
            port = _port;

            ctx = ZmqContext.Create();
            socket = ctx.CreateSocket(SocketType.PUB);
            socket.Bind(String.Format("tcp://*:{0}", port));
            Console.Out.WriteLine("Publisher successfuly binded on port {0}", port);
            socket.SendHighWatermark = 1000000;
            socket.SendBufferSize = 512 * 1024;
        }

        public void Publish(string channel, Message msg)
        {
            var c = Proxy.StringToByteArray(channel);
            byte[] data;
            using (var ms = new MemoryStream())
            {
                Serializer.Serialize(ms, msg);
                data = ms.ToArray();
                //
                byte[] buf = new byte[c.Length + 1 + data.Length];

                Array.Copy(c, buf, c.Length);
                Array.Copy(ASCIIEncoding.ASCII.GetBytes("\t"), 0, buf, channel.Length, 1);
                Array.Copy(data, 0, buf, c.Length + 1, data.Length);

                socket.Send(buf, buf.Length, SocketFlags.DontWait);
            }
        }
    }
}
