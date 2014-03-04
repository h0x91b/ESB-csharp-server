using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using ProtoBuf;
using ServiceStack.Redis;


namespace ESBServer
{
    public class Proxy
    {
        bool isWork = false;
        string guid;
        Publisher publisher;
        HandshakeServer handshakeServer;
        Dictionary<string, Subscriber> subscribers;
        Dictionary<string, Dictionary<string, Method>> localMethods; //key is identifier
        Dictionary<string, InvokeResponse> invokeResponses; //cmdGuid, source_proxy_guid
        DateTime lastRedisUpdate;
        RedisClient registryRedis = null;
        Random random;
        public Proxy()
        {
            random = new Random(BitConverter.ToInt32(Guid.NewGuid().ToByteArray(), 0));
            guid = GenGuid();
            Console.Out.WriteLine("New ESBServer {0}", guid);
            
            publisher = new Publisher(guid, GetFQDN(), 7001);
            localMethods = new Dictionary<string, Dictionary<string, Method>>();
            invokeResponses = new Dictionary<string, InvokeResponse>();
            
            subscribers = new Dictionary<string, Subscriber>();
            registryRedis = new RedisClient("esb-redis", 6379);
            lastRedisUpdate = DateTime.Now.AddHours(-1);
            isWork = true;

            handshakeServer = new HandshakeServer(7002, 7001, (ip, port, targetGuid) =>
            {
                Console.Out.WriteLine("New client requests my port from IP {0}, his port is {1}, guid: {2}", ip, port, targetGuid);
                var sub = new Subscriber(guid, targetGuid, String.Format("tcp://{0}:{1}", ip, port));
                subscribers.Add(targetGuid, sub);
            });
            (new Thread(new ThreadStart(MainLoop))).Start();
        }

        void MainLoop()
        {
            while (isWork)
            {
                try
                {
                    var nothingToDo = true;

                    handshakeServer.Poll();

                    //var msgReq = responder.Poll();
                    //if (msgReq != null)
                    //{
                    //    nothingToDo = false;
                    //    switch (msgReq.cmd)
                    //    {
                    //        case Message.Cmd.NODE_HELLO:
                    //            NodeHello(msgReq); 
                    //            break;
                    //        default:
                    //            throw new Exception("Unknown command");
                    //    }
                    //}
                    int time = Unixtimestamp();
                    var listOfDeadSubscribers = new List<string>();
                    foreach (var k in subscribers)
                    {
                        var subscriber = k.Value;
                        for (int i = 0; i<10000; i++)
                        {
                            var msg = subscriber.Poll();
                            if (msg == null) break;
                            switch (msg.cmd)
                            {
                                case Message.Cmd.PING:
                                    Pong(msg);
                                    break;
                                case Message.Cmd.REGISTER_INVOKE:
                                    RegisterInvoke(msg);
                                    break;
                                case Message.Cmd.INVOKE:
                                    Invoke(msg);
                                    break;
                                case Message.Cmd.RESPONSE:
                                case Message.Cmd.ERROR_RESPONSE:
                                    Response(msg);
                                    break;
                                default:
                                    Console.Out.WriteLine("Unknown command received, cmd payload: {0}", ByteArrayToString(msg.payload));
                                    throw new Exception("Unknown command received");
                            }
                        }
                        if (time - subscriber.lastActiveTime > 5)
                        {
                            //5 sec timeout
                            listOfDeadSubscribers.Add(k.Key);
                        }
                    }

                    foreach (var guid in listOfDeadSubscribers)
                    {
                        int totalRemovedMethods = KillSubscriber(guid);
                        Console.Out.WriteLine("Removed {0} methods from registry of dead node {1}", totalRemovedMethods, guid);
                    }

                    if ((DateTime.Now - lastRedisUpdate).TotalSeconds >= 3)
                    {
                        lastRedisUpdate = DateTime.Now;
                        RedisPing();
                    }
                    if(nothingToDo)
                        Thread.Sleep(1);
                }
                catch (Exception e)
                {
                    Console.Out.WriteLine("Exception in MainLoop: {0}", e.ToString());
                    Thread.Sleep(10);
                }
            }
        }

        int KillSubscriber(string guid)
        {
            var sub = subscribers[guid];
            subscribers.Remove(guid);
            sub.Dispose();

            int totalRemoved = 0;
            foreach (var d in localMethods)
            {
                var toRemove = new List<string>();
                foreach (var m in d.Value)
                {
                    if (m.Value.proxyGuid == guid)
                    {
                        Console.Out.WriteLine("Remove method `{0}` with guid {1}", m.Value.identifier, m.Key);
                        toRemove.Add(m.Key);
                    }
                }
                foreach (var k in toRemove)
                {
                    d.Value.Remove(k);
                    totalRemoved++;
                }
            }
            return totalRemoved;
        }

        void RegisterInvoke(Message cmdReq)
        {
            if (!localMethods.ContainsKey(cmdReq.identifier))
            {
                localMethods[cmdReq.identifier] = new Dictionary<string, Method>();
            }
            if (!localMethods[cmdReq.identifier].ContainsKey(ByteArrayToString(cmdReq.payload)))
            {
                Console.Out.WriteLine("RegisterInvoke guid `{0}`, identifier `{1}`, node guid `{2}`", ByteArrayToString(cmdReq.payload), cmdReq.identifier, cmdReq.source_proxy_guid);
                localMethods[cmdReq.identifier][ByteArrayToString(cmdReq.payload)] = new Method
                {
                    identifier = cmdReq.identifier,
                    methodGuid = ByteArrayToString(cmdReq.payload),
                    proxyGuid = cmdReq.source_proxy_guid
                };
            }

            var respMsg = new Message
            {
                cmd = Message.Cmd.REGISTER_INVOKE_OK,
                payload = StringToByteArray("\"good\""), //this is JSON encoded string
                source_proxy_guid = guid,
                guid_to = cmdReq.guid_from
            };
            publisher.Publish(cmdReq.source_proxy_guid, respMsg);
        }

        void Response(Message msg)
        {
            if (!invokeResponses.ContainsKey(msg.guid_to))
            {
                Console.Out.WriteLine("Response {0} not found in queue", msg.guid_to);
                return;
            }
            var resp = invokeResponses[msg.guid_to];
            var respMsg = new Message
            {
                cmd = msg.cmd,
                payload = msg.payload, //this is JSON encoded string
                source_proxy_guid = guid,
                guid_to = msg.guid_to
            };
            publisher.Publish(resp.sourceGuid, respMsg);
            invokeResponses.Remove(msg.guid_to);
        }

        void Invoke(Message cmdReq)
        {
            //Console.Out.WriteLine("Got Invoke from {0} - {1}", cmdReq.source_proxy_guid, cmdReq.identifier);
            if (!localMethods.ContainsKey(cmdReq.identifier) || localMethods[cmdReq.identifier].Count < 1)
            {
                var respMsg = new Message
                {
                    cmd = Message.Cmd.ERROR,
                    payload = StringToByteArray(String.Format("Invoke method \"{0}\" not found", cmdReq.identifier)),
                    source_proxy_guid = guid,
                    guid_to = cmdReq.guid_from
                };
                publisher.Publish(cmdReq.source_proxy_guid, respMsg);
                return;
            }
            var method = RandomValueFromDictionary<string, Method>(localMethods[cmdReq.identifier]).First();

            invokeResponses[cmdReq.guid_from] = new InvokeResponse {
                expireDate = (DateTime.Now.AddSeconds(30)),
                sourceGuid = cmdReq.source_proxy_guid
            };

            var msg = new Message
            {
                cmd = Message.Cmd.INVOKE,
                source_proxy_guid = guid,
                guid_to = method.methodGuid,
                guid_from = cmdReq.guid_from,
                payload = cmdReq.payload,
            };
            publisher.Publish(method.proxyGuid, msg);
        }

        void RedisPing()
        {
            registryRedis.ZAdd("ZSET:PROXIES", Unixtimestamp(), StringToByteArray(String.Format("{0}#{1}:{2}", guid, GetFQDN(), 7002)));
        }

        void Pong(Message cmdReq)
        {
            //Console.Out.WriteLine("Got ping from {0}", cmdReq.source_proxy_guid);
            var respMsg = new Message
            {
                cmd = Message.Cmd.RESPONSE,
                payload = StringToByteArray("\"pong!\""), //this is JSON encoded string
                source_proxy_guid = guid,
                guid_to = cmdReq.guid_from
            };
            publisher.Publish(cmdReq.source_proxy_guid, respMsg);
        }

        void NodeHello(Message cmdReq)
        {
            try
            {
                var payload = System.Text.Encoding.UTF8.GetString(cmdReq.payload);
                Console.Out.WriteLine("Got NODE_HELLO: {0}", payload);
                //guid#connectStr
                var t = payload.Split('#');
                var targetGuid = t[0];
                var connectString = t[1];

                var toKill = new List<string>();
                foreach (var s in subscribers)
                {
                    if (s.Value.connectionString == connectString)
                    {
                        Console.Out.WriteLine("Kill subscriber {0} because he have a same connection string: {1}, probably old one is dead...", s.Key, connectString);
                        toKill.Add(s.Key);
                    }
                }
                foreach (var g in toKill)
                {
                    int totalRemovedMethods = KillSubscriber(g);
                    Console.Out.WriteLine("removed {0} methods from registry", totalRemovedMethods);
                }

                var subsciber = new Subscriber(guid, targetGuid, connectString);
                subscribers[targetGuid] = subsciber;
                var respMsg = new Message
                {
                    cmd = Message.Cmd.RESPONSE,
                    payload = StringToByteArray(String.Format("{0}#{1}", publisher.host, publisher.port)),
                    source_proxy_guid = guid
                };
                //responder.SendResponse(respMsg);
            }
            catch (Exception e)
            {
                Console.Out.WriteLine("Something bad happen on NodeHello: {0}", e.ToString());
                var respMsg = new Message
                {
                    cmd = Message.Cmd.ERROR,
                    payload = StringToByteArray(e.ToString()),
                    source_proxy_guid = guid
                };
                //responder.SendResponse(respMsg);
            }
        }

        public static string GetFQDN()
        {
            string domainName = System.Net.NetworkInformation.IPGlobalProperties.GetIPGlobalProperties().DomainName;
            string hostName = Dns.GetHostName();
            string fqdn = "";
            if (!hostName.Contains(domainName))
                fqdn = hostName + "." + domainName;
            else
                fqdn = hostName;

            return fqdn;
        }

        public string GenGuid()
        {
            //var g = Guid.NewGuid();
            //return g.ToString().Replace("-", string.Empty).ToUpper().Substring(0, 16);
            var chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
            var stringChars = new char[16];

            for (int i = 0; i < stringChars.Length; i++)
            {
                stringChars[i] = chars[random.Next(1000000) % chars.Length];
            }

            return new String(stringChars);
        }

        public static int Unixtimestamp()
        {
            return (Int32)(DateTime.UtcNow.Subtract(new DateTime(1970, 1, 1))).TotalSeconds;
        }

        public static byte[] StringToByteArray(string str)
        {
            var uBytes = Encoding.Unicode.GetBytes(str);
            byte[] bytes = Encoding.Convert(Encoding.Unicode, Encoding.ASCII, uBytes);
            return bytes;
        }

        public static string ByteArrayToString(byte[] data)
        {
            return System.Text.Encoding.UTF8.GetString(data);
        }

        public static IEnumerable<TValue> RandomValueFromDictionary<TKey, TValue>(IDictionary<TKey, TValue> dict)
        {
            Random rand = new Random();
            List<TValue> values = Enumerable.ToList(dict.Values);
            int size = dict.Count;
            while (true)
            {
                yield return values[rand.Next(size)];
            }
        }
    }

    class InvokeResponse
    {
        public DateTime expireDate;
        public string sourceGuid;
    }

    class Method
    {
        public string identifier;
        public string methodGuid;
        public string proxyGuid;
    }


    [ProtoContract]
    public class RegistryEntry
    {
        public enum RegistryEntryType
        {
            INVOKE_METHOD = 1
        };
        [ProtoMember(1, IsRequired = true)]
        public RegistryEntryType type { get; set; }
        [ProtoMember(2, IsRequired = true)]
        public string identifier { get; set; }
        [ProtoMember(3, IsRequired = true)]
        public string method_guid { get; set; }
        [ProtoMember(4, IsRequired = true)]
        public string proxy_guid { get; set; }
    }
    [ProtoContract]
    public class Message
    {
        public enum Cmd
        {
            ERROR = 1,
            RESPONSE = 2,
            ERROR_RESPONSE = 3,
            NODE_HELLO = 4,
            PROXY_HELLO = 5,
            PING = 6,
            PONG = 7,
            INVOKE = 8,
            REGISTER_INVOKE = 9,
            REGISTER_INVOKE_OK = 10,
            REGISTRY_EXCHANGE_REQUEST = 11,
            REGISTRY_EXCHANGE_RESPONSE = 12,
            PUBLISH = 13,
            SUBSCRIBE = 14
        }

        [ProtoMember(1, IsRequired = true)]
        public Cmd cmd { get; set; }
        [ProtoMember(2, IsRequired = true)]
        public string source_proxy_guid { get; set; }
        [ProtoMember(3, IsRequired = true)]
        public byte[] payload { get; set; }
        [ProtoMember(4)]
        public string target_proxy_guid { get; set; }
        [ProtoMember(5)]
        public string identifier { get; set; }
        [ProtoMember(6)]
        public string guid_from { get; set; }
        [ProtoMember(7)]
        public string guid_to { get; set; }
        [ProtoMember(8)]
        public Int32 recursion { get; set; }
        [ProtoMember(9)]
        public UInt32 start_time { get; set; }
        [ProtoMember(10)]
        public Int32 timeout_ms { get; set; }
        [ProtoMember(11)]
        public List<RegistryEntry> reg_entry { get; set; }
    }
}
