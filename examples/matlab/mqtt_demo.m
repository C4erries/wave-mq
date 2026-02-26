% Minimal MATLAB MQTT demo for wave-mq single-node mode.
% Requires MATLAB with mqttclient support.

brokerHost = "127.0.0.1";
brokerPort = 1883;
topic = "demo.mqtt";
clientID = "matlab-demo-" + string(randi([1000 9999]));

mq = mqttclient("tcp://" + brokerHost, Port=brokerPort, ClientID=clientID);
disp("Connected to MQTT broker");

sub = subscribe(mq, topic, QoS=0);
disp("Subscribed to " + topic);

pause(1);
payload = "hello-from-matlab-" + string(datetime("now","Format","yyyyMMddHHmmss"));

% Some MATLAB releases expose write(), others publish().
if any(strcmp(methods(mq), "write"))
    write(mq, topic, payload, QoS=0);
else
    publish(mq, topic, payload, QoS=0);
end
disp("Published payload: " + payload);

disp("Waiting up to 10 seconds for a message...");
started = tic;
while toc(started) < 10
    msg = read(mq, NumMessages=1);
    if ~isempty(msg)
        disp("Received:");
        disp(msg);
        break;
    end
    pause(0.2);
end

clear sub mq;
disp("Done");
