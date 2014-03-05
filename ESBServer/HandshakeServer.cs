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
            Console.Out.WriteLine("HanshakeServer succesfully binded on port {0}", port);
            server.Listen(10);
        }

        public void Poll()
        {
            var list = new ArrayList();
            list.Add(server);

            Socket.Select(list, null, null, 1);

            if (list.Count > 0)
            {
                Console.Out.WriteLine("got connection");
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
                    Console.Out.WriteLine("Error on socket");
                    removeSocketList.Add(c);
                    continue;
                }

                if (c.Poll(1, SelectMode.SelectRead) && c.Available > 0)
                {
                    try
                    {
                        Console.Out.WriteLine("Available for read {0} bytes", c.Available);
                        int len = c.Receive(buf);
                        Console.Out.WriteLine("Read {0} bytes", len);
                        var portStr = Encoding.ASCII.GetString(buf, 0, len);
                        var rEp = c.RemoteEndPoint as IPEndPoint;
                        Console.Out.WriteLine("Remote addr: {0}", rEp.Address.ToString());

                        var parameters = portStr.Split('#');

                        cb(rEp.Address.ToString(), Convert.ToInt32(parameters[0]), parameters[1]);
                    }
                    catch (Exception e)
                    {
                        Console.Out.WriteLine("Exception while reading from socket: {0}", e.ToString());
                    }
                }

                if (c.Poll(1, SelectMode.SelectWrite))
                {
                    try
                    {
                        var rEp = c.RemoteEndPoint as IPEndPoint;
                        Console.Out.WriteLine("Available for write to {0}", rEp.Address.ToString());
                        c.Send(Encoding.ASCII.GetBytes(String.Format("{0}", publisherPort)));
                        c.Shutdown(SocketShutdown.Both);
                        c.Close();
                        removeSocketList.Add(c);
                    }
                    catch (Exception e)
                    {
                        Console.Out.WriteLine("Exception while writting to socket: {0}", e.ToString());
                    }
                }
            }

            foreach (var c in removeSocketList)
            {
                connectedClients.Remove(c);
            }
        }
    }
}
