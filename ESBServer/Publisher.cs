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
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        public int port { get; internal set; }
        public string host { get; internal set; }
        public string guid { get; internal set; }

        public ZmqContext ctx { get; internal set; }
        ZmqSocket socket = null;

        public Publisher(string _guid, string _host, int _port, ZmqContext _ctx)
        {
            guid = _guid;
            host = _host;
            port = _port;

            if (_ctx == null)
                ctx = ZmqContext.Create();
            else
                ctx = _ctx;
            socket = ctx.CreateSocket(SocketType.PUB);
            var bindString = String.Empty;
            if(port > 0)
                bindString = String.Format("tcp://*:{0}", port);
            else
                bindString = String.Format("inproc://{0}", guid.ToLower());
            socket.Bind(bindString);
            log.InfoFormat("Publisher successfuly binded on {0}", bindString);
            socket.SendHighWatermark = 1000000;
            socket.SendBufferSize = 128 * 1024;
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
