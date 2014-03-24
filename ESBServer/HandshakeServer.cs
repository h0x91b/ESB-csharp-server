using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace ESBServer
{
    public class HandshakeServer
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        Socket server;
        public int port { get; internal set; }
        public int publisherPort { get; internal set; }
        public delegate void ClientCallback(string ip, int port, string targetGuid);
        public ClientCallback cb;
        byte[] buf;
        List<Socket> connectedClients;
        public HandshakeServer(int _port, int _publisherPort, ClientCallback _cb)
        {
            port = _port;
            publisherPort = _publisherPort;
            cb = _cb;
            buf = new byte[1024];
            connectedClients = new List<Socket>();
            server = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            var ep = new IPEndPoint(IPAddress.Any, port);
            server.Bind(ep);
            log.InfoFormat("HanshakeServer succesfully binded on port {0}", port);
            server.Listen(10);
        }

        public void Poll()
        {
            try
            {
                var list = new ArrayList();
                list.Add(server);

                Socket.Select(list, null, null, 1);

                if (list.Count > 0)
                {
                    if (log.IsDebugEnabled) log.DebugFormat("got connection");
                    var client = server.Accept();
                    client.Blocking = false;
                    client.ReceiveTimeout = 5000;
                    client.SendTimeout = 5000;
                    connectedClients.Add(client);
                }

                var removeSocketList = new List<Socket>();

                foreach (var c in connectedClients)
                {
                    if (c.Poll(1, SelectMode.SelectError))
                    {
                        log.ErrorFormat("Error on socket");
                        removeSocketList.Add(c);
                        continue;
                    }

                    if (c.Poll(1, SelectMode.SelectRead) && c.Available > 0)
                    {
                        try
                        {
                            if (log.IsDebugEnabled) log.DebugFormat("Available for read {0} bytes", c.Available);
                            int len = c.Receive(buf);
                            if (log.IsDebugEnabled) log.DebugFormat("Read {0} bytes", len);
                            var portStr = Encoding.ASCII.GetString(buf, 0, len);
                            var rEp = c.RemoteEndPoint as IPEndPoint;

                            var parameters = portStr.Split('#');
                            log.InfoFormat("Node publisher is: tcp:{0}:{1}", rEp.Address.ToString(), Convert.ToInt32(parameters[0]));

                            cb(rEp.Address.ToString(), Convert.ToInt32(parameters[0]), parameters[1]);
                        }
                        catch (Exception e)
                        {
                            log.ErrorFormat("Exception while reading from socket: {0}", e.ToString());
                        }
                    }

                    if (c.Poll(1, SelectMode.SelectWrite))
                    {
                        try
                        {
                            var rEp = c.RemoteEndPoint as IPEndPoint;
                            if (log.IsDebugEnabled) log.DebugFormat("Available for write to {0}", rEp.Address.ToString());
                            c.Send(Encoding.ASCII.GetBytes(String.Format("{0}", publisherPort)));
                            c.Shutdown(SocketShutdown.Both);
                            c.Close();
                            removeSocketList.Add(c);
                        }
                        catch (Exception e)
                        {
                            log.InfoFormat("Exception while writting to socket: {0}", e.ToString());
                        }
                    }
                }

                foreach (var c in removeSocketList)
                {
                    connectedClients.Remove(c);
                }
            }
            catch (Exception e)
            {
                log.ErrorFormat("Exception on Poll {0}", e);
            }
        }
    }
}
