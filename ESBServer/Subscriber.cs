using ProtoBuf;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using ZeroMQ;

namespace ESBServer
{
    public class Subscriber : IDisposable
    {
        public string connectionString { get; internal set; }
        string guid;
        string targetGuid;

        ZmqContext ctx = null;
        ZmqSocket socket = null;
        byte[] buf;

        public int lastActiveTime;

        public List<String> subscribeChannels;

        public Subscriber(string _guid, string _targetGuid, string _connectionString)
        {
            guid = _guid;
            targetGuid = _targetGuid;
            connectionString = _connectionString;
            buf = new byte[1024 * 1024];
            subscribeChannels = new List<string>();

            ctx = ZmqContext.Create();
            socket = ctx.CreateSocket(SocketType.SUB);
            Console.Out.WriteLine("Subscriber connecting to: `{0}`", connectionString);
            socket.Subscribe(Proxy.StringToByteArray(guid));
            socket.Connect(connectionString);
            socket.ReceiveHighWatermark = 1000000;
            socket.ReceiveBufferSize = 512 * 1024;

            lastActiveTime = Proxy.Unixtimestamp();
            Console.Out.WriteLine("Connected");
        }

        public void Subscribe(string channel)
        {
            if (subscribeChannels.Contains(channel))
            {
                Console.Out.WriteLine("Subscriber {0} already subscribed on `{1}`", targetGuid, channel);
                return;
            }
            Console.Out.WriteLine("Subscriber {0} subscribe on `{1}`", targetGuid, channel);
            subscribeChannels.Add(channel);
            socket.Subscribe(Proxy.StringToByteArray(channel));
        }

        public void Unsubscribe(string channel)
        {
            if (!subscribeChannels.Contains(channel))
            {
                return;
            }
            Console.Out.WriteLine("Subscriber {0} unsubscribe on `{1}`", targetGuid, channel);
            subscribeChannels.Remove(channel);
            socket.Unsubscribe(Proxy.StringToByteArray(channel));
        }

        public void Dispose()
        {
            Console.Out.WriteLine("The end of life for subscriber `{0}` `{1}`", connectionString, targetGuid);
            socket.Close();
            ctx.Terminate();
        }

        public Message Poll()
        {
            var size = socket.Receive(buf, SocketFlags.DontWait);
            var status = socket.ReceiveStatus;
            if (status == ReceiveStatus.TryAgain)
            {
                return null;
            }
            var start = Array.IndexOf(buf, (byte)9);
            if (start == -1) throw new Exception("Can not find the Delimiter \\t");
            lastActiveTime = Proxy.Unixtimestamp();
            start++;
            MemoryStream stream = new MemoryStream(buf, start, size - start, false);
            var respMsg = Serializer.Deserialize<Message>(stream);
            return respMsg;
        }
    }
}
