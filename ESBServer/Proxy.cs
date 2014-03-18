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
using StatsdClient;
using System.IO;
using System.Configuration;

namespace ESBServer
{
    public class luaRedis
    {
        public string script;
        public string sha1;
    };

    public class Proxy : ServiceBase
    {
        private static readonly log4net.ILog log = log4net.LogManager.GetLogger(System.Reflection.MethodBase.GetCurrentMethod().DeclaringType);
        bool isWork = false;
        string guid;
        Publisher publisher;
        HandshakeServer handshakeServer;
        Dictionary<string, Subscriber> subscribers;
        Dictionary<string, Dictionary<string, Method>> localMethods; //identifier, methodGuid, struct
        Dictionary<string, Dictionary<string, Method>> remoteMethods; //identifier, methodGuid, struct
        Dictionary<string, InvokeResponse> invokeResponses; //cmdGuid, source_component_guid
        Dictionary<string, int> subscribeChannels; //channel, refs
        Dictionary<string, List<string>> nodeSubscribeChannels; //nodeGuid, list of channels
        List<string> proxyGuids;
        List<RedisClient> queueRedises;
        Dictionary<string, DateTime> proxyRegistryExchange;
        DateTime lastRedisUpdate;
        DateTime lastReport;
        RedisClient registryRedis = null;
        Random random;
        StatsdUDP statsdUdp;
        Statsd statsD;
        Dictionary<string, luaRedis> luaScripts;
        List<string> channelsWithQueues;

        public Proxy()
        {
            random = new Random(BitConverter.ToInt32(Guid.NewGuid().ToByteArray(), 0));
            guid = GenGuid();
            log.InfoFormat("New ESBServer {0}", guid);
            statsdUdp = new StatsdUDP("ams-node-ops01.frxfarm.local", 8125);
            statsD = new Statsd(statsdUdp);

            statsD.Send<Statsd.Counting>("esb.start-server", 1); //counter had one hit


            subscribeChannels = new Dictionary<string, int>();
            nodeSubscribeChannels = new Dictionary<string, List<string>>();
            proxyRegistryExchange = new Dictionary<string, DateTime>();
            proxyGuids = new List<string>();
            int publisherPort = 7001;

            localMethods = new Dictionary<string, Dictionary<string, Method>>();
            remoteMethods = new Dictionary<string, Dictionary<string, Method>>();
            invokeResponses = new Dictionary<string, InvokeResponse>();

            subscribers = new Dictionary<string, Subscriber>();

            var registryHost = ConfigurationManager.AppSettings["registryRedisHost"].ToString();
            var registryPort = Convert.ToInt32(ConfigurationManager.AppSettings["registryRedisPort"].ToString());
            log.InfoFormat("using registry: {0}:{1}", registryHost, registryPort);
            registryRedis = new RedisClient(registryHost, registryPort);
            queueRedises = new List<RedisClient>();
            channelsWithQueues = new List<string>();

            luaScripts = new Dictionary<string, luaRedis>();

            luaScripts["regQueue"] = new luaRedis
            {
                script = @"
local channelName = ARGV[1]
local queueName = ARGV[2]

local ret = redis.call('SADD', 'SET:QUEUES:' .. channelName, queueName)
if ret == 1 then
    redis.call('SADD', 'SET:QUEUES', channelName)
end
"
            };

            luaScripts["listChannelsWithQueues"] = new luaRedis
            {
                script = @"
return redis.call('SMEMBERS', 'SET:QUEUES')
"
            };

            luaScripts["enqueue"] = new luaRedis
            {
                script = @"
local channelName = ARGV[1]
local entry = ARGV[2]
local expire = 86400*7

local queues = redis.call('SMEMBERS', 'SET:QUEUES:' .. channelName)
for k,v in ipairs(queues) do
    local queueName = v
    local id = redis.call('INCR', 'INCR:QUEUE:' .. channelName .. ':' .. queueName .. ':ID')
    redis.call('SETEX', 'KV:QUEUE:' .. channelName .. ':' .. queueName .. ':' .. id, expire, entry)
    redis.call('ZADD', 'ZSET:QUEUE:' .. channelName .. ':' .. queueName, id, id);
end
"
            };

            luaScripts["unregQueue"] = new luaRedis
            {
                script = @"
local channelName = ARGV[1]
local queueName = ARGV[2]

redis.call('SREM', 'SET:QUEUES:' .. channelName, queueName)
if redis.call('SCARD', 'SET:QUEUES:' .. channelName) < 1 then
    redis.call('SREM', 'SET:QUEUES', channelName)
end

local ids = redis.call('ZRANGE', 'ZSET:QUEUE:' .. channelName .. ':' .. queueName, 0, -1)
for k,v in ipairs(ids) do
    redis.call('DEL', 'KV:QUEUE:' .. channelName .. ':' .. queueName .. ':' .. v)
end
redis.call('DEL', 'ZSET:QUEUE:' .. channelName .. ':' .. queueName)
"
            };

            luaScripts["peek"] = new luaRedis
            {
                script = @"
local channelName = ARGV[1]
local queueName = ARGV[2]
local timeout = ARGV[3]

local ids = redis.call('ZRANGE', 'ZSET:QUEUE:' .. channelName .. ':' .. queueName, 0, -1)
for k,v in ipairs(ids) do
    local lock = 'LOCK:QUEUE:' .. channelName .. ':' .. queueName .. ':' .. v
    local ret = redis.call('SETNX', lock, v)
    if ret == 1 then
        redis.call('PEXPIRE', lock, timeout)
        local entry = redis.call('GET', 'KV:QUEUE:' .. channelName .. ':' .. queueName .. ':' .. v)
        if entry == nil then
            redis.call('ZREM', 'ZSET:QUEUE:' .. channelName .. ':' .. queueName, v)
        else
            return cjson.encode({v, entry})
        end
    end
end

return nil
"
            };

            luaScripts["dequeue"] = new luaRedis
            {
                script = @"
local channelName = ARGV[1]
local queueName = ARGV[2]
local id = ARGV[3]

redis.call('ZREM', 'ZSET:QUEUE:' .. channelName .. ':' .. queueName, id)
redis.call('DEL', 'KV:QUEUE:' .. channelName .. ':' .. queueName .. ':' .. id)
"
            };

            var qArr = ConfigurationManager.AppSettings["queueRedises"].ToString().Split('|');
            foreach (var r in qArr)
            {
                var rHost = r.Split(':')[0];
                var rPort = Convert.ToInt32(r.Split(':')[1]);
                log.InfoFormat("Adding queue redis into list: {0}:{1}", registryHost, registryPort);
                var redis = new RedisClient(rHost, rPort);
                foreach (var d in luaScripts)
                {
                    log.InfoFormat("Compile lua script `{0}`", d.Key);
                    luaScripts[d.Key].sha1 = ByteArrayToString(redis.ScriptLoad(luaScripts[d.Key].script));
                    log.InfoFormat("SHA1 of lua script: {0}", luaScripts[d.Key].sha1);
                }
                queueRedises.Add(redis);
            }

            lastRedisUpdate = DateTime.Now.AddHours(-1);
            isWork = true;
            lastReport = DateTime.Now;

            for (var i = 0; i < 10; i++)
            {
                try
                {
                    publisher = new Publisher(guid, GetFQDN(), publisherPort);
                    handshakeServer = new HandshakeServer(publisherPort + 1, publisherPort, (ip, port, targetGuid) =>
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
                    var date = DateTime.Now;
                    var listOfDeadSubscribers = new List<string>();
                    foreach (var k in subscribers)
                    {
                        var subscriber = k.Value;
                        for (int i = 0; i < 500; i++)
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
                                case Message.Cmd.REGISTRY_EXCHANGE_REQUEST:
                                    SendMyRegistry(msg);
                                    break;
                                case Message.Cmd.REGISTRY_EXCHANGE_RESPONSE:
                                    GotRemoteRegisty(msg);
                                    break;
                                case Message.Cmd.REG_QUEUE:
                                    RegQueue(msg);
                                    break;
                                case Message.Cmd.UNREG_QUEUE:
                                    UnregQueue(msg);
                                    break;
                                case Message.Cmd.PEEK_QUEUE:
                                    PeekQueue(msg);
                                    break;
                                case Message.Cmd.DEQUEUE_QUEUE:
                                    DequeueQueue(msg);
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
                        else
                        {
                            if (proxyGuids.Contains(k.Key))
                            {
                                if (!proxyRegistryExchange.ContainsKey(k.Key))
                                {
                                    proxyRegistryExchange[k.Key] = DateTime.Now.AddMilliseconds(-3001);
                                }
                                if ((date - proxyRegistryExchange[k.Key]).TotalMilliseconds > 3000)
                                {
                                    RequestRegistryFromProxy(subscriber.targetGuid);
                                }
                            }
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
                        GetQueuesList();
                    }

                    var dt = (int)(DateTime.Now - date).TotalMilliseconds;
                    if (dt > 10)
                    {
                        log.WarnFormat("Main loop cycle taked {0}ms!", dt);
                    }
                    statsD.Send<Statsd.Timing>("main-loop-cycle-time", dt, 1.0 / 1000.0);

                    if ((DateTime.Now - lastReport).TotalSeconds > 30)
                    {
                        lastReport = DateTime.Now;
                        ReportStats();
                    }

                    if (nothingToDo)
                        Thread.Sleep(1);
                }
                catch (Exception e)
                {
                    log.ErrorFormat("Exception in MainLoop: {0}", e.ToString());
                    statsD.Send<Statsd.Counting>("esb.MainLoopErrors", 1);
                    Thread.Sleep(10);
                }
            }
        }

        void RegQueue(Message msg)
        {
            var queueName = ByteArrayToString(msg.payload);
            Encoding encoding = new UTF8Encoding();
            byte[] input = encoding.GetBytes(msg.identifier);
            using (MemoryStream stream = new MemoryStream(input))
            {
                int redisIndex = Math.Abs(MurMurHash3.Hash(stream)) % queueRedises.Count;
                log.InfoFormat("RegQueue for channel `{0}` queue name `{1}`, redisIndex is: {2}", msg.identifier, queueName, redisIndex);
                var script = luaScripts["regQueue"];
                var ret = queueRedises[redisIndex].EvalSha(script.sha1, 2, StringToByteArray("Channel"), StringToByteArray("QueueName"), StringToByteArray(msg.identifier), msg.payload);
                if (!channelsWithQueues.Contains(msg.identifier))
                    channelsWithQueues.Add(msg.identifier);
            }
        }

        void UnregQueue(Message msg)
        {
            var queueName = ByteArrayToString(msg.payload);
            Encoding encoding = new UTF8Encoding();
            byte[] input = encoding.GetBytes(msg.identifier);
            using (MemoryStream stream = new MemoryStream(input))
            {
                int redisIndex = Math.Abs(MurMurHash3.Hash(stream)) % queueRedises.Count;
                log.InfoFormat("UnregQueue for channel `{0}` queue name `{1}`, redisIndex is: {2}", msg.identifier, queueName, redisIndex);
                var script = luaScripts["unregQueue"];
                var ret = queueRedises[redisIndex].EvalSha(script.sha1, 2, StringToByteArray("Channel"), StringToByteArray("QueueName"), StringToByteArray(msg.identifier), msg.payload);
                channelsWithQueues.Remove(msg.identifier);
            }
        }

        void PeekQueue(Message msg)
        {
            var payload = ByteArrayToString(msg.payload).Split('\t');
            var queueName = payload[0];
            var timeout_ms = payload[1];
            Encoding encoding = new UTF8Encoding();
            byte[] input = encoding.GetBytes(msg.identifier);
            using (MemoryStream stream = new MemoryStream(input))
            {
                int redisIndex = Math.Abs(MurMurHash3.Hash(stream)) % queueRedises.Count;
                log.InfoFormat("PeekQueue for channel `{0}` queue name `{1}`, redisIndex is: {2}", msg.identifier, queueName, redisIndex);
                var script = luaScripts["peek"];
                var ret = queueRedises[redisIndex].EvalSha(
                    script.sha1, 
                    3, 
                    StringToByteArray("Channel"), 
                    StringToByteArray("QueueName"), 
                    StringToByteArray("Timeout"), 
                    StringToByteArray(msg.identifier),
                    StringToByteArray(queueName), 
                    StringToByteArray(timeout_ms)
                );
                if (ret == null || ret[1] == null)
                {
                    publisher.Publish(msg.source_component_guid, new Message
                    {
                        cmd = Message.Cmd.RESPONSE,
                        source_component_guid = guid,
                        target_operation_guid = msg.source_operation_guid,
                        payload = StringToByteArray("null")
                    });
                }
                else
                {
                    //var resp = ByteArrayToString(ret[1]);
                    publisher.Publish(msg.source_component_guid, new Message
                    {
                        cmd = Message.Cmd.RESPONSE,
                        source_component_guid = guid,
                        target_operation_guid = msg.source_operation_guid,
                        payload = ret[1]
                    });
                }
            }
        }

        void DequeueQueue(Message msg)
        {
            var payload = ByteArrayToString(msg.payload).Split('\t');
            var queueName = payload[0];
            var id = payload[1];
            Encoding encoding = new UTF8Encoding();
            byte[] input = encoding.GetBytes(msg.identifier);
            using (MemoryStream stream = new MemoryStream(input))
            {
                int redisIndex = Math.Abs(MurMurHash3.Hash(stream)) % queueRedises.Count;
                log.InfoFormat("PeekQueue for channel `{0}` queue name `{1}`, redisIndex is: {2}", msg.identifier, queueName, redisIndex);
                var script = luaScripts["dequeue"];
                queueRedises[redisIndex].EvalSha(
                    script.sha1,
                    3,
                    StringToByteArray("Channel"),
                    StringToByteArray("QueueName"),
                    StringToByteArray("Id"),
                    StringToByteArray(msg.identifier),
                    StringToByteArray(queueName),
                    StringToByteArray(id)
                );
            }
        }

        void GotRemoteRegisty(Message msg)
        {
            var remoteMethodsCount = 0;
            if (msg.reg_entry != null) remoteMethodsCount = msg.reg_entry.Count;
            if (log.IsDebugEnabled) log.DebugFormat("Got remote registry from proxy {0} - {1} methods", msg.source_component_guid, remoteMethodsCount);
            if (remoteMethodsCount < 1) return;

            foreach (var m in msg.reg_entry)
            {
                if (!remoteMethods.ContainsKey(m.identifier))
                    remoteMethods[m.identifier] = new Dictionary<string, Method>();
                if (!remoteMethods[m.identifier].ContainsKey(m.method_guid))
                {
                    log.InfoFormat("Add new remote method with identifier `{0}`, guid `{1}` on proxy `{2}`", m.identifier, m.method_guid, m.proxy_guid);
                    remoteMethods[m.identifier][m.method_guid] = new Method
                    {
                        isLocal = false,
                        lastUpdate = DateTime.Now,
                        identifier = m.identifier,
                        methodGuid = m.method_guid,
                        proxyGuid = m.proxy_guid,
                        type = m.type
                    };
                }
                else
                {
                    if (log.IsDebugEnabled) log.DebugFormat("Update lastUpdate for {0} {1} on proxy {2}", m.identifier, m.method_guid, m.proxy_guid);
                    remoteMethods[m.identifier][m.method_guid].lastUpdate = DateTime.Now;
                }
            }
        }

        void SendMyRegistry(Message msgReq)
        {
            if (log.IsDebugEnabled) log.DebugFormat("Send my registry to proxy {0}", msgReq.source_component_guid);
            var msg = new Message
            {
                cmd = Message.Cmd.REGISTRY_EXCHANGE_RESPONSE,
                source_component_guid = guid,
                payload = StringToByteArray("take it"),
                reg_entry = new List<RegistryEntry>()
            };
            var methodCount = 0;
            foreach (var d in localMethods)
            {
                foreach (var m in d.Value)
                {
                    if (log.IsDebugEnabled) log.DebugFormat("Add local invoke method {0} with guid {1}", d.Key, m.Key);
                    msg.reg_entry.Add(new RegistryEntry
                    {
                        identifier = d.Key,
                        method_guid = m.Key,
                        proxy_guid = guid,
                        type = RegistryEntry.RegistryEntryType.INVOKE_METHOD
                    });
                    methodCount++;
                }
            }
            if (log.IsDebugEnabled) log.DebugFormat("send {0} local methods", methodCount);
            publisher.Publish(msgReq.source_component_guid, msg);
        }

        void RequestRegistryFromProxy(string targetGuid)
        {
            if (log.IsDebugEnabled) log.DebugFormat("request registry exchange");
            proxyRegistryExchange[targetGuid] = DateTime.Now;
            var msg = new Message
            {
                cmd = Message.Cmd.REGISTRY_EXCHANGE_REQUEST,
                source_component_guid = guid,
                payload = StringToByteArray("please")
            };

            publisher.Publish(targetGuid, msg);
        }

        void Publish(Message msg)
        {
            if (log.IsDebugEnabled) log.DebugFormat("Publish()");
            if (msg.recursion > 1) return;
            if (msg.recursion == 0 && channelsWithQueues.Contains(msg.identifier))
            {
                Encoding encoding = new UTF8Encoding();
                byte[] input = encoding.GetBytes(msg.identifier);
                using (MemoryStream stream = new MemoryStream(input))
                {
                    int redisIndex = Math.Abs(MurMurHash3.Hash(stream)) % queueRedises.Count;
                    log.InfoFormat("Enqueue message on channel `{0}`", msg.identifier);
                    var script = luaScripts["enqueue"];
                    queueRedises[redisIndex].EvalSha(script.sha1, 2, StringToByteArray("Channel"), StringToByteArray("Entry"), StringToByteArray(msg.identifier), msg.payload);
                    statsD.Send<Statsd.Counting>(String.Format("esb.queue.{0}", msg.identifier), 1, 1.0 / 50.0);
                }
            }
            statsD.Send<Statsd.Counting>("esb.publishes", 1, 1.0 / 250.0);
            statsD.Send<Statsd.Counting>(String.Format("esb.publish.{0}", msg.identifier), 1, 1.0 / 50.0); //counter had one hit
            msg.source_component_guid = guid;
            msg.recursion = msg.recursion + 1;
            publisher.Publish(msg.identifier, msg);
        }

        void Subscribe(Message msg)
        {
            if (log.IsDebugEnabled) log.DebugFormat("Subscribe()");
            statsD.Send<Statsd.Counting>("esb.subscribes", 1, 1.0 / 10.0);

            if (!nodeSubscribeChannels.ContainsKey(msg.source_component_guid))
            {
                nodeSubscribeChannels[msg.source_component_guid] = new List<string>();
            }

            if (!subscribeChannels.ContainsKey(msg.identifier))
            {
                subscribeChannels[msg.identifier] = 1;
            }
            else
            {
                if (!nodeSubscribeChannels[msg.source_component_guid].Contains(msg.identifier))
                    subscribeChannels[msg.identifier]++;
            }

            if (!nodeSubscribeChannels[msg.source_component_guid].Contains(msg.identifier))
            {
                nodeSubscribeChannels[msg.source_component_guid].Add(msg.identifier);
                log.InfoFormat("Subscribe... `{0}`, on this channel we have {1} subscribers", msg.identifier, subscribeChannels[msg.identifier]);
                statsD.Send<Statsd.Counting>(String.Format("esb.subscribe.{0}", msg.identifier), 1);
            }

            foreach (var v in subscribers)
            {
                var sub = v.Value;
                sub.Subscribe(msg.identifier);
            }
        }

        int KillSubscriber(string targetGuid)
        {
            if (log.IsDebugEnabled) log.DebugFormat("KillSubscriber()");
            statsD.Send<Statsd.Counting>("esb.kill-subscriber", 1);
            var sub = subscribers[targetGuid];

            if (proxyGuids.Contains(targetGuid))
            {
                registryRedis.ZRem("ZSET:PROXIES", StringToByteArray(String.Format("{0}#{1}:{2}", sub.targetGuid, sub.host, sub.port + 1)));
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
                        log.InfoFormat("Remove local method `{0}` with guid {1}", m.Value.identifier, m.Key);
                        toRemove.Add(m.Key);
                    }
                }
                foreach (var k in toRemove)
                {
                    d.Value.Remove(k);
                    totalRemoved++;
                }
            }

            foreach (var d in remoteMethods)
            {
                var toRemove = new List<string>();
                foreach (var m in d.Value)
                {
                    if (m.Value.proxyGuid == targetGuid)
                    {
                        log.InfoFormat("Remove remote method `{0}` with guid {1}", m.Value.identifier, m.Key);
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
                    log.InfoFormat("On channel `{0}` we have {1} refs", channel, subscribeChannels[channel]);
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

        void ReportStats()
        {
            log.InfoFormat("ESB have a following:");
            foreach (var i in localMethods)
            {
                foreach (var m in i.Value)
                {
                    log.InfoFormat("Local Invoke method `{0}` - Method guid: `{1}`, Proxy guid: `{2}`", i.Key, m.Value.methodGuid, m.Value.proxyGuid);
                }
            }

            foreach (var i in remoteMethods)
            {
                foreach (var m in i.Value)
                {
                    log.InfoFormat("Remote Invoke method `{0}` - Method guid: `{1}`, Proxy guid: `{2}`", i.Key, m.Value.methodGuid, m.Value.proxyGuid);
                }
            }

            log.InfoFormat("ESB have subscribers for:");

            foreach (var c in subscribeChannels)
            {
                log.InfoFormat("On channel `{0}` ESB have {1} refs", c.Key, c.Value);
            }
        }

        void RegisterInvoke(Message cmdReq)
        {
            if (log.IsDebugEnabled) log.DebugFormat("RegisterInvoke()");
            statsD.Send<Statsd.Counting>("esb.register-invoke", 1);
            if (!localMethods.ContainsKey(cmdReq.identifier))
            {
                localMethods[cmdReq.identifier] = new Dictionary<string, Method>();
            }
            if (!localMethods[cmdReq.identifier].ContainsKey(ByteArrayToString(cmdReq.payload)))
            {
                log.InfoFormat("RegisterInvoke guid `{0}`, identifier `{1}`, node guid `{2}`", ByteArrayToString(cmdReq.payload), cmdReq.identifier, cmdReq.source_component_guid);
                localMethods[cmdReq.identifier][ByteArrayToString(cmdReq.payload)] = new Method
                {
                    identifier = cmdReq.identifier,
                    methodGuid = ByteArrayToString(cmdReq.payload),
                    proxyGuid = cmdReq.source_component_guid,
                    isLocal = true,
                    type = RegistryEntry.RegistryEntryType.INVOKE_METHOD,
                    lastUpdate = DateTime.Now
                };
            }
            else
            {
                localMethods[cmdReq.identifier][ByteArrayToString(cmdReq.payload)].lastUpdate = DateTime.Now;
            }

            var respMsg = new Message
            {
                cmd = Message.Cmd.REGISTER_INVOKE_OK,
                payload = StringToByteArray("\"good\""), //this is JSON encoded string
                source_component_guid = guid,
                target_operation_guid = cmdReq.source_operation_guid
            };
            publisher.Publish(cmdReq.source_component_guid, respMsg);
        }

        void Response(Message msg)
        {
            if (log.IsDebugEnabled) log.DebugFormat("Response()");
            if (!invokeResponses.ContainsKey(msg.target_operation_guid))
            {
                log.InfoFormat("Response {0} not found in queue", msg.target_operation_guid);
                return;
            }
            var resp = invokeResponses[msg.target_operation_guid];
            var respMsg = new Message
            {
                cmd = msg.cmd,
                payload = msg.payload, //this is JSON encoded string
                source_component_guid = guid,
                target_operation_guid = msg.target_operation_guid
            };
            publisher.Publish(resp.sourceGuid, respMsg);
            invokeResponses.Remove(msg.target_operation_guid);
        }

        void Invoke(Message cmdReq)
        {
            if (log.IsDebugEnabled) log.DebugFormat("Invoke()");
            statsD.Send<Statsd.Counting>("esb.invokes", 1, 1.0 / 100.0); //counter had one hit
            statsD.Send<Statsd.Counting>(String.Format("esb.invoke.{0}", cmdReq.identifier), 1, 1.0 / 10.0); //counter had one hit
            //log.InfoFormat("Got Invoke from {0} - {1}", cmdReq.source_component_guid, cmdReq.identifier);
            if (!localMethods.ContainsKey(cmdReq.identifier) || localMethods[cmdReq.identifier].Count < 1)
            {
                if (!remoteMethods.ContainsKey(cmdReq.identifier) || remoteMethods[cmdReq.identifier].Count < 1)
                {
                    var respMsg = new Message
                    {
                        cmd = Message.Cmd.ERROR,
                        payload = StringToByteArray(String.Format("Invoke method \"{0}\" not found", cmdReq.identifier)),
                        source_component_guid = guid,
                        target_operation_guid = cmdReq.source_operation_guid
                    };
                    publisher.Publish(cmdReq.source_component_guid, respMsg);
                    statsD.Send<Statsd.Counting>(String.Format("esb.invoke-notfounds", cmdReq.identifier), 1); //counter had one hit
                    statsD.Send<Statsd.Counting>(String.Format("esb.invoke-notfound.{0}", cmdReq.identifier), 1); //counter had one hit
                    return;
                }
                //remote method call

                var remoteMethod = RandomValueFromDictionary<string, Method>(remoteMethods[cmdReq.identifier]).First();
                invokeResponses[cmdReq.source_operation_guid] = new InvokeResponse
                {
                    expireDate = (DateTime.Now.AddSeconds(30)),
                    sourceGuid = cmdReq.source_component_guid
                };

                var remoteMsg = new Message
                {
                    cmd = Message.Cmd.INVOKE,
                    source_component_guid = guid,
                    target_operation_guid = remoteMethod.methodGuid,
                    source_operation_guid = cmdReq.source_operation_guid,
                    payload = cmdReq.payload,
                    identifier = cmdReq.identifier
                };
                publisher.Publish(remoteMethod.proxyGuid, remoteMsg);
                return;
            }
            var method = RandomValueFromDictionary<string, Method>(localMethods[cmdReq.identifier]).First();

            invokeResponses[cmdReq.source_operation_guid] = new InvokeResponse
            {
                expireDate = (DateTime.Now.AddSeconds(30)),
                sourceGuid = cmdReq.source_component_guid
            };

            var msg = new Message
            {
                cmd = Message.Cmd.INVOKE,
                source_component_guid = guid,
                target_operation_guid = method.methodGuid,
                source_operation_guid = cmdReq.source_operation_guid,
                payload = cmdReq.payload,
                identifier = cmdReq.identifier
            };
            publisher.Publish(method.proxyGuid, msg);
        }

        void SendPing(string targetGuid)
        {
            if (log.IsDebugEnabled) log.DebugFormat("SendPing()");
            publisher.Publish(targetGuid, new Message
            {
                cmd = Message.Cmd.PING,
                source_component_guid = guid,
                source_operation_guid = "",
                payload = StringToByteArray("ping!")
            });
        }

        void GetQueuesList()
        {
            if (log.IsDebugEnabled) log.DebugFormat("GetQueuesList()");
            //channelsWithQueues = new List<string>();
            var script = luaScripts["listChannelsWithQueues"];
            foreach (var r in queueRedises)
            {
                var ret = r.EvalSha(script.sha1, 0);
                foreach (var c in ret)
                {
                    var channel = ByteArrayToString(c);
                    if (!channelsWithQueues.Contains(channel))
                    {
                        log.InfoFormat("Register new channel for queue `{0}`", channel);
                        channelsWithQueues.Add(channel);
                    }
                }
            }
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
            statsD.Send<Statsd.Counting>("esb.connect-to-proxy", 1);
            try
            {
                log.InfoFormat("Connect to subscriber tcp://{0}:{1}", host, port - 1);
                var sub = new Subscriber(guid, targetGuid, host, port - 1);
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
            log.DebugFormat("Got ping from {0}", cmdReq.source_component_guid);
            var respMsg = new Message
            {
                cmd = Message.Cmd.PONG,
                payload = StringToByteArray("\"pong!\""), //this is JSON encoded string
                source_component_guid = guid,
                target_operation_guid = cmdReq.source_operation_guid
            };
            publisher.Publish(cmdReq.source_component_guid, respMsg);
            statsD.Send<Statsd.Counting>("esb.pong", 1, 1.0 / 30.0);
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
            if (statsD != null) statsD.Send<Statsd.Counting>("esb.gen-guid", 1, 1.0 / 100.0);
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
        public bool isLocal;
        public RegistryEntry.RegistryEntryType type;
        public DateTime lastUpdate;
    }


    [ProtoContract]
    public class RegistryEntry
    {
        public enum RegistryEntryType
        {
            INVOKE_METHOD = 1,
            QUEUE = 2
        };
        [ProtoMember(1, IsRequired = true)]
        public RegistryEntryType type { get; set; }
        [ProtoMember(2, IsRequired = true)]
        public string identifier { get; set; }
        [ProtoMember(3, IsRequired = true)]
        public string method_guid { get; set; }
        [ProtoMember(4, IsRequired = true)]
        public string proxy_guid { get; set; }
        [ProtoMember(5, IsRequired = false)]
        public string queue_name { get; set; }
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
            PING = 5,
            PONG = 6,
            INVOKE = 7,
            REGISTER_INVOKE = 8,
            REGISTER_INVOKE_OK = 9,
            REGISTRY_EXCHANGE_REQUEST = 10,
            REGISTRY_EXCHANGE_RESPONSE = 11,
            PUBLISH = 12,
            SUBSCRIBE = 13,
            REG_QUEUE = 14,
            UNREG_QUEUE = 15,
            PEEK_QUEUE = 16,
            DEQUEUE_QUEUE = 17,
            LEN_QUEUE = 18
        }

        [ProtoMember(1, IsRequired = true)]
        public Cmd cmd { get; set; }
        [ProtoMember(2, IsRequired = true)]
        public string source_component_guid { get; set; }
        [ProtoMember(3, IsRequired = true)]
        public byte[] payload { get; set; }
        [ProtoMember(4)]
        public string target_component_guid { get; set; }
        [ProtoMember(5)]
        public string identifier { get; set; }
        [ProtoMember(6)]
        public string source_operation_guid { get; set; }
        [ProtoMember(7)]
        public string target_operation_guid { get; set; }
        [ProtoMember(8)]
        public Int32 recursion { get; set; }
        [ProtoMember(9)]
        public List<RegistryEntry> reg_entry { get; set; }
    }

    /*
    This code is public domain.
 
    The MurmurHash3 algorithm was created by Austin Appleby and put into the public domain.  See http://code.google.com/p/smhasher/
 
    This C# variant was authored by
    Elliott B. Edwards and was placed into the public domain as a gist
    Status...Working on verification (Test Suite)
    Set up to run as a LinqPad (linqpad.net) script (thus the ".Dump()" call)
    */
    public static class MurMurHash3
    {
        //Change to suit your needs
        const uint seed = 144;

        public static int Hash(Stream stream)
        {
            const uint c1 = 0xcc9e2d51;
            const uint c2 = 0x1b873593;

            uint h1 = seed;
            uint k1 = 0;
            uint streamLength = 0;

            using (BinaryReader reader = new BinaryReader(stream))
            {
                byte[] chunk = reader.ReadBytes(4);
                while (chunk.Length > 0)
                {
                    streamLength += (uint)chunk.Length;
                    switch (chunk.Length)
                    {
                        case 4:
                            /* Get four bytes from the input into an uint */
                            k1 = (uint)
                               (chunk[0]
                              | chunk[1] << 8
                              | chunk[2] << 16
                              | chunk[3] << 24);

                            /* bitmagic hash */
                            k1 *= c1;
                            k1 = rotl32(k1, 15);
                            k1 *= c2;

                            h1 ^= k1;
                            h1 = rotl32(h1, 13);
                            h1 = h1 * 5 + 0xe6546b64;
                            break;
                        case 3:
                            k1 = (uint)
                               (chunk[0]
                              | chunk[1] << 8
                              | chunk[2] << 16);
                            k1 *= c1;
                            k1 = rotl32(k1, 15);
                            k1 *= c2;
                            h1 ^= k1;
                            break;
                        case 2:
                            k1 = (uint)
                               (chunk[0]
                              | chunk[1] << 8);
                            k1 *= c1;
                            k1 = rotl32(k1, 15);
                            k1 *= c2;
                            h1 ^= k1;
                            break;
                        case 1:
                            k1 = (uint)(chunk[0]);
                            k1 *= c1;
                            k1 = rotl32(k1, 15);
                            k1 *= c2;
                            h1 ^= k1;
                            break;

                    }
                    chunk = reader.ReadBytes(4);
                }
            }

            // finalization, magic chants to wrap it all up
            h1 ^= streamLength;
            h1 = fmix(h1);

            unchecked //ignore overflow
            {
                return (int)h1;
            }
        }

        private static uint rotl32(uint x, byte r)
        {
            return (x << r) | (x >> (32 - r));
        }

        private static uint fmix(uint h)
        {
            h ^= h >> 16;
            h *= 0x85ebca6b;
            h ^= h >> 13;
            h *= 0xc2b2ae35;
            h ^= h >> 16;
            return h;
        }
    }
}
