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
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        public string connectionString { get; internal set; }
        public string host { get; internal set; }
        public int port { get; internal set; }
        string guid;
        public string targetGuid { get; internal set; }

        ZmqContext ctx = null;
        bool shardedCtx;
        ZmqSocket socket = null;
        byte[] buf;

        public int lastActiveTime;
        public int lastPingTime;

        public List<String> subscribeChannels;

        public Subscriber(string _guid, string _targetGuid, string _host, int _port, ZmqContext _ctx)
        {
            guid = _guid;
            targetGuid = _targetGuid;
            host = _host;
            port = _port;
            if (port > 0) 
                connectionString = String.Format("tcp://{0}:{1}", host, port);
            else
                connectionString = String.Format("inproc://{0}", targetGuid.ToLower());
            buf = new byte[1024 * 1024];
            subscribeChannels = new List<string>();

            if (_ctx == null)
            {
                shardedCtx = false;
                ctx = ZmqContext.Create();
            }
            else
            {
                shardedCtx = true;
                ctx = _ctx;
            }
            socket = ctx.CreateSocket(SocketType.SUB);
            if (log.IsDebugEnabled) log.DebugFormat("Subscriber connecting to: `{0}`", connectionString);
            socket.Subscribe(Proxy.StringToByteArray(guid));
            socket.Connect(connectionString);
            socket.ReceiveHighWatermark = 1000000;
            socket.ReceiveBufferSize = 128 * 1024;

            lastActiveTime = Proxy.Unixtimestamp();
            log.InfoFormat("Connected successfuly to: `{0}` `{1}`", connectionString, targetGuid);
        }

        public void Subscribe(string channel)
        {
            if (subscribeChannels.Contains(channel))
            {
                if(log.IsDebugEnabled) log.DebugFormat("{0} Already subscribed on `{1}`", targetGuid, channel);
                return;
            }
            if (log.IsDebugEnabled) log.DebugFormat("{0} Subscribe on `{1}`", targetGuid, channel);
            subscribeChannels.Add(channel);
            socket.Subscribe(Proxy.StringToByteArray(channel));
        }

        public void Unsubscribe(string channel)
        {
            //if (!subscribeChannels.Contains(channel))
            //{
            //    return;
            //}
            if (log.IsDebugEnabled) log.DebugFormat("{0} Unsubscribe on `{1}`", targetGuid, channel);
            subscribeChannels.Remove(channel);
            socket.Unsubscribe(Proxy.StringToByteArray(channel));
        }

        public void Dispose()
        {
            log.InfoFormat("The end of life for subscriber `{0}` `{1}`", connectionString, targetGuid);
            socket.Close();
            if(!shardedCtx) ctx.Terminate();
        }

        public Message Poll()
        {
            var size = socket.Receive(buf, SocketFlags.DontWait);
            var status = socket.ReceiveStatus;
            if (status == ReceiveStatus.TryAgain)
            {
                return null;
            }
            try
            {
                var start = Array.IndexOf(buf, (byte)9);
                if (start == -1) throw new Exception("Can not find the Delimiter \\t");
                lastActiveTime = Proxy.Unixtimestamp();
                start++;
                MemoryStream stream = new MemoryStream(buf, start, size - start, false);
                var respMsg = Serializer.Deserialize<Message>(stream);
                return respMsg;
            }
            catch (Exception e)
            {
                log.ErrorFormat("Error while decoding message from subscriber Poll {0}", e);
                return null;
            }
        }
    }
}
