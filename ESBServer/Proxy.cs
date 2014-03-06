using System;
using System.Collections.Generic;
using System.Linq;
using System.Net;
using System.Text;
using System.Threading;
using ProtoBuf;
using ServiceStack.Redis;
using log4net;
using System.ServiceProcess;

namespace ESBServer
{
    public class Proxy : ServiceBase
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        bool isWork = false;
        string guid;
        Publisher publisher;
        HandshakeServer handshakeServer;
        Dictionary<string, Subscriber> subscribers;
        Dictionary<string, Dictionary<string, Method>> localMethods; //key is identifier
        Dictionary<string, InvokeResponse> invokeResponses; //cmdGuid, source_proxy_guid
        Dictionary<string, int> subscribeChannels; //channel, refs
        Dictionary<string, List<string>> nodeSubscribeChannels; //nodeGuid, list of channels
        List<string> proxyGuids;
        DateTime lastRedisUpdate;
        RedisClient registryRedis = null;
        Random random;
        public Proxy()
        {
            random = new Random(BitConverter.ToInt32(Guid.NewGuid().ToByteArray(), 0));
            guid = GenGuid();
            log.InfoFormat("New ESBServer {0}", guid);

            subscribeChannels = new Dictionary<string, int>();
            nodeSubscribeChannels = new Dictionary<string, List<string>>();
            proxyGuids = new List<string>();
            int publisherPort = 7001;
            
            localMethods = new Dictionary<string, Dictionary<string, Method>>();
            invokeResponses = new Dictionary<string, InvokeResponse>();
            
            subscribers = new Dictionary<string, Subscriber>();
            registryRedis = new RedisClient("esb-redis", 6379);
            lastRedisUpdate = DateTime.Now.AddHours(-1);
            isWork = true;

            for (var i = 0; i < 10; i++)
            {
                try
                {
                    publisher = new Publisher(guid, GetFQDN(), publisherPort);
                    handshakeServer = new HandshakeServer(publisherPort+1, publisherPort, (ip, port, targetGuid) =>
                    {
                        log.InfoFormat("New client requests my port from IP {0}, his port is {1}, guid: {2}", ip, port, targetGuid);

                        var connectString = String.Format("tcp://{0}:{1}", ip, port);
                        var toKill = new List<string>();
                        foreach (var s in subscribers)
                        {
                            if (s.Value.connectionString == connectString)
                            {
                                log.InfoFormat("Kill subscriber {0} because he have a same connection string: {1}, probably old one is dead...", s.Key, connectString);
                                toKill.Add(s.Key);
                            }
                        }
                        foreach (var g in toKill)
                        {
                            int totalRemovedMethods = KillSubscriber(g);
                            log.InfoFormat("removed {0} methods from registry", totalRemovedMethods);
                        }

                        var sub = new Subscriber(guid, targetGuid, ip, port);
                        foreach (var s in subscribeChannels)
                        {
                            sub.Subscribe(s.Key);
                        }

                        subscribers.Add(targetGuid, sub);
                    });
                    break;
                }
                catch (Exception e)
                {
                    log.WarnFormat("Bind on port {0} failed: {1}", publisherPort, e.ToString());
                    publisherPort += 2;
                    if (i == 10)
                    {
                        throw new Exception("Can not bind on ports 7002-7022");
                    }
                }
            }

            (new Thread(new ThreadStart(MainLoop))).Start();
        }

        protected override void OnStop()
        {
            log.ErrorFormat("Got shutdown request!");
            isWork = false;
        }

        void MainLoop()
        {
            while (isWork)
            {
                try
                {
                    var nothingToDo = true;

                    handshakeServer.Poll();

                    int time = Unixtimestamp();
                    var listOfDeadSubscribers = new List<string>();
                    foreach (var k in subscribers)
                    {
                        var subscriber = k.Value;
                        for (int i = 0; i<10000; i++)
                        {
                            var msg = subscriber.Poll();
                            if (msg == null) break;
                            if (log.IsDebugEnabled) log.DebugFormat("Got message from subscriber `{0}`, type: `{1}`, identifier: `{2}`, payload: `{3}`", subscriber.targetGuid, msg.cmd, msg.identifier, ByteArrayToString(msg.payload));
                            switch (msg.cmd)
                            {
                                case Message.Cmd.PING:
                                    Pong(msg);
                                    break;
                                case Message.Cmd.PONG:
                                    break;
                                case Message.Cmd.REGISTER_INVOKE:
                                    RegisterInvoke(msg);
                                    break;
                                case Message.Cmd.INVOKE:
                                    Invoke(msg);
                                    break;
                                case Message.Cmd.PUBLISH:
                                    Publish(msg);
                                    break;
                                case Message.Cmd.SUBSCRIBE:
                                    Subscribe(msg);
                                    break;
                                case Message.Cmd.RESPONSE:
                                case Message.Cmd.ERROR_RESPONSE:
                                    Response(msg);
                                    break;
                                default:
                                    log.ErrorFormat("Unknown command received, cmd payload: {0}", ByteArrayToString(msg.payload));
                                    throw new Exception("Unknown command received");
                            }
                        }
                        if (time - subscriber.lastPingTime > 1)
                        {
                            //Send ping
                            if (log.IsDebugEnabled) log.DebugFormat("Send Ping to subscriber {0}", subscriber.targetGuid);
                            subscriber.lastPingTime = Unixtimestamp();
                            SendPing(subscriber.targetGuid);
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
                        log.InfoFormat("Removed {0} methods from registry of dead node {1}", totalRemovedMethods, guid);
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
                    log.ErrorFormat("Exception in MainLoop: {0}", e.ToString());
                    Thread.Sleep(10);
                }
            }
        }

        void Publish(Message msg)
        {
            if (log.IsDebugEnabled) log.DebugFormat("Publish()");
            if (msg.recursion > 0) return;
            msg.source_proxy_guid = guid;
            msg.recursion = 1;
            publisher.Publish(msg.identifier, msg);
        }

        void Subscribe(Message msg)
        {
            if (log.IsDebugEnabled) log.DebugFormat("Subscribe()");
            if (!subscribeChannels.ContainsKey(msg.identifier))
            {
                subscribeChannels[msg.identifier] = 1;
            }
            else
            {
                subscribeChannels[msg.identifier]++;
            }

            if (!nodeSubscribeChannels.ContainsKey(msg.source_proxy_guid))
            {
                nodeSubscribeChannels[msg.source_proxy_guid] = new List<string>();
            }

            if (!nodeSubscribeChannels[msg.source_proxy_guid].Contains(msg.identifier))
            {
                nodeSubscribeChannels[msg.source_proxy_guid].Add(msg.identifier);
            }

            log.InfoFormat("Subscribe... `{0}`, on this channel we have {1} subscribers", msg.identifier, subscribeChannels[msg.identifier]);
            foreach (var v in subscribers)
            {
                var sub = v.Value;
                sub.Subscribe(msg.identifier);
            }
        }

        int KillSubscriber(string targetGuid)
        {
            if (log.IsDebugEnabled) log.DebugFormat("KillSubscriber()");
            var sub = subscribers[targetGuid];

            if (proxyGuids.Contains(targetGuid))
            {
                registryRedis.ZRem("ZSET:PROXIES", StringToByteArray(String.Format("{0}#{1}:{2}", sub.targetGuid, sub.host, sub.port+1)));
                proxyGuids.Remove(targetGuid);
            }

            subscribers.Remove(targetGuid);
            sub.Dispose();

            int totalRemoved = 0;
            foreach (var d in localMethods)
            {
                var toRemove = new List<string>();
                foreach (var m in d.Value)
                {
                    if (m.Value.proxyGuid == targetGuid)
                    {
                        log.InfoFormat("Remove method `{0}` with guid {1}", m.Value.identifier, m.Key);
                        toRemove.Add(m.Key);
                    }
                }
                foreach (var k in toRemove)
                {
                    d.Value.Remove(k);
                    totalRemoved++;
                }
            }

            if (nodeSubscribeChannels.ContainsKey(targetGuid))
            {
                foreach (var channel in nodeSubscribeChannels[targetGuid])
                {
                    subscribeChannels[channel]--;
                    log.InfoFormat("On channel {0} we have {1} refs", channel, subscribeChannels[channel]);
                    if (subscribeChannels[channel] <= 0)
                    {
                        foreach (var s in subscribers)
                        {
                            var subs = s.Value;
                            subs.Unsubscribe(channel);
                        }
                    }
                }
            }
            nodeSubscribeChannels.Remove(targetGuid);

            return totalRemoved;
        }

        void RegisterInvoke(Message cmdReq)
        {
            if (log.IsDebugEnabled) log.DebugFormat("RegisterInvoke()");
            if (!localMethods.ContainsKey(cmdReq.identifier))
            {
                localMethods[cmdReq.identifier] = new Dictionary<string, Method>();
            }
            if (!localMethods[cmdReq.identifier].ContainsKey(ByteArrayToString(cmdReq.payload)))
            {
                log.InfoFormat("RegisterInvoke guid `{0}`, identifier `{1}`, node guid `{2}`", ByteArrayToString(cmdReq.payload), cmdReq.identifier, cmdReq.source_proxy_guid);
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
            if (log.IsDebugEnabled) log.DebugFormat("Response()");
            if (!invokeResponses.ContainsKey(msg.guid_to))
            {
                log.InfoFormat("Response {0} not found in queue", msg.guid_to);
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
            if (log.IsDebugEnabled) log.DebugFormat("Invoke()");
            //log.InfoFormat("Got Invoke from {0} - {1}", cmdReq.source_proxy_guid, cmdReq.identifier);
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

        void SendPing(string targetGuid)
        {
            if (log.IsDebugEnabled) log.DebugFormat("SendPing()");
            publisher.Publish(targetGuid, new Message
            {
                cmd = Message.Cmd.PING,
                source_proxy_guid = guid,
                guid_from = "",
                payload = StringToByteArray("ping!")
            });
        }

        void RedisPing()
        {
            if (log.IsDebugEnabled) log.DebugFormat("RedisPing()");
            var me = String.Format("{0}#{1}:{2}", guid, GetFQDN(), handshakeServer.port);
            if (log.IsDebugEnabled) log.DebugFormat("Add to redis: `{0}`", me);
            registryRedis.ZAdd("ZSET:PROXIES", Unixtimestamp(), StringToByteArray(me));
            var rez = registryRedis.ZRange("ZSET:PROXIES", 0, -1);
            if (log.IsDebugEnabled) log.DebugFormat("Entries in redis: {0}", rez.Length);
            if (rez.Length > 1) //1 this proxy
            {
                for (var i = 0; i < rez.Length; i++)
                {
                    var entry = ByteArrayToString(rez[i]);
                    var tmp = entry.Split('#');
                    var targetGuid = tmp[0];
                    if (targetGuid == guid)
                    {
                        //found me in list...
                        continue;
                    }
                    if (proxyGuids.Contains(targetGuid))
                    {
                        //connected already...
                        continue;
                    }
                    var conStr = tmp[1];
                    var tmp2 = conStr.Split(':');
                    var host = tmp2[0];
                    var port = Convert.ToInt32(tmp2[1]);

                    if (port == handshakeServer.port && GetFQDN() == host && guid != targetGuid)
                    {
                        log.WarnFormat("Found me in proxies, but with wrong guid, delete this entry");
                        registryRedis.ZRem("ZSET:PROXIES", rez[i]);
                        continue;
                    }

                    var found = false;
                    foreach (var s in subscribers)
                    {
                        if (s.Value.targetGuid == targetGuid)
                        {
                            found = true;
                            break;
                        }
                    }
                    if (!found)
                    {
                        log.InfoFormat("found new proxy at tcp://{0}:{1}", host, port);

                        if (!ConnectToProxy(host, port, targetGuid))
                        {
                            registryRedis.ZRem("ZSET:PROXIES", rez[i]);
                        }
                    }
                }
            }
        }

        bool ConnectToProxy(string host, int port, string targetGuid)
        {
            if (log.IsDebugEnabled) log.DebugFormat("ConnectToProxy({0}, {1}, {2})", host, port, targetGuid);
            try
            {
                log.InfoFormat("Connect to subscriber tcp://{0}:{1}", host, port-1);
                var sub = new Subscriber(guid, targetGuid, host, port-1);
                subscribers.Add(targetGuid, sub);
                proxyGuids.Add(targetGuid);
                foreach (var s in subscribeChannels)
                {
                    sub.Subscribe(s.Key);
                }
                return true;
            }
            catch (Exception e)
            {
                log.ErrorFormat("Connection to subscriber tcp://{0}:{1} failed", host, port - 1);
            }
            return false;
        }

        void Pong(Message cmdReq)
        {
            log.DebugFormat("Got ping from {0}", cmdReq.source_proxy_guid);
            var respMsg = new Message
            {
                cmd = Message.Cmd.PONG,
                payload = StringToByteArray("\"pong!\""), //this is JSON encoded string
                source_proxy_guid = guid,
                guid_to = cmdReq.guid_from
            };
            publisher.Publish(cmdReq.source_proxy_guid, respMsg);
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

            log.DebugFormat("GetFQDN returns - `{0}`", fqdn);

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
