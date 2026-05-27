# robot_mqtt_bridge

Self-contained MQTT ↔ [robot_agent](../robot_agent) bridge. Receives
structured plans over MQTT, forwards them to robot_agent's `/ws/agent`
WebSocket in direct mode (no LLM), and publishes the final result
back to MQTT.

```
MQTT client (client.py) ──► broker ──► server.py ──► ws://localhost:8001/ws/agent
                                            │
                  ◄────── broker ◄────── result publish
```

## Layout

```
robot_mqtt_bridge/
├── server.py         # MqttComm (transport) + Server (bridge) + main()
├── client.py         # MqttClient — for tests / integration
├── requirements.txt
└── README.md
```

No dependency on pyconnect. Just `paho-mqtt`, `websockets`, `numpy`.

## Install

```bash
pip install -r requirements.txt
```

## Topics

| Topic | Dir | Schema |
|---|---|---|
| `cotap/keti/task/plan` | in | `{contents:{data:{plan:"find::apple\nmove::kitchen", request_id?:"..."}}}` |
| `cotap/common/plan_result` | out | `{contents:{data:{result:{action,isdone,error?,...}, request_id?:"..."}}}` |
| `cotap/keti/bridge/status` | out (retained + LWT) | `{contents:{data:{online:true\|false}}}` |

Envelope `{timestamp, contents:{data:...}}` produced/consumed by
`MqttComm.sendIt()`.

### `action` field

- Single-task step (`find::apple`) → `action = "find"`
- Parallel step (`move::a && grip::1`) → `action = "move&grip"`
- Bridge busy (refused) → `action = "busy"`
- Couldn't determine → `action = "unknown"`

`isdone` mirrors robot_agent's final `done.success` (false on transport
error or any step failure).

## Usage

### 1. Start robot_agent

```bash
cd ../kcare_robot && uvicorn kcare_robot.main:app --port 8001
```

### 2. Start the bridge server

```bash
cd robot_mqtt_bridge

# defaults: agent=ws://localhost:8001, broker=$MQTT_SERVER_IP or 192.168.1.200
python server.py

# explicit
python server.py --agent-url ws://localhost:8001 --mqtt-ip 192.168.1.200

# with broker auth (ip/user/pass — MqttComm convention)
python server.py --mqtt-ip "192.168.1.200/admin/secret"

# quiet
python server.py --quiet

# via env vars
AGENT_URL=ws://localhost:8001 MQTT_SERVER_IP=192.168.1.200 python server.py
```

### 3a. Send a plan from the CLI

```bash
# subscribe in one terminal
mosquitto_sub -h 192.168.1.200 -t 'cotap/common/plan_result' -v

# publish a structured plan
mosquitto_pub -h 192.168.1.200 -t 'cotap/keti/task/plan' -m '{
  "timestamp": "2026-05-27 14:30:00",
  "contents": {"data": {"plan": "find::apple\nmove::kitchen"}}
}'
```

Expected on `cotap/common/plan_result`:

```json
{
  "timestamp": "2026-05-27 14:30:30",
  "contents": {"data": {"result": {
    "action": "move",
    "isdone": true,
    "pose": [1.2, 3.4]
  }}}
}
```

With `request_id` for correlation:

```bash
mosquitto_pub -h 192.168.1.200 -t 'cotap/keti/task/plan' -m '{
  "timestamp": "2026-05-27 14:30:00",
  "contents": {"data": {
    "plan": "lift::200",
    "request_id": "req-abc-123"
  }}
}'
```

The server echoes `request_id` at `contents.data.request_id`.

### 3b. Send a plan from Python (using `MqttClient`)

```python
from client import MqttClient

mqttClient = MqttClient(bPrint=True)

# convenience: single-skill plans
mqttClient.actionWork("move", "table@living room")   # → plan = "move::table@living room"
mqttClient.actionWork("lift", "200")                  # → plan = "lift::200.0"
mqttClient.actionWork("grip", 1)                      # → plan = "grip::1000"
mqttClient.actionWork("init_arm", None)               # → plan = "init_arm::"

# arbitrary multi-step plan
ok = mqttClient.commWork("find::apple\nmove::kitchen\npick::", timeout=180)
print("success" if ok else "failed/timeout")

mqttClient.close()
```

Run the built-in demo:
```bash
python client.py
```

### 3c. Send a plan from Python (ad-hoc paho-mqtt)

If you don't want to import `MqttClient`:

```python
import json, time, uuid
import paho.mqtt.client as mqtt

BROKER = "192.168.1.200"
REQ_ID = str(uuid.uuid4())
result_holder = {}

def on_message(client, userdata, msg):
    data = json.loads(msg.payload)["contents"]["data"]
    if data.get("request_id") == REQ_ID:
        result_holder["res"] = data["result"]

c = mqtt.Client()
c.on_message = on_message
c.connect(BROKER, 1883)
c.subscribe("cotap/common/plan_result", qos=1)
c.loop_start()

payload = {
    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
    "contents": {"data": {
        "plan": "find::apple\nmove::kitchen",
        "request_id": REQ_ID,
    }},
}
c.publish("cotap/keti/task/plan", json.dumps(payload), qos=1)

deadline = time.time() + 180
while "res" not in result_holder and time.time() < deadline:
    time.sleep(0.1)

c.loop_stop()
c.disconnect()
print(result_holder.get("res") or "timeout")
```

## Module API

```python
from server import MqttComm, Server

# Low-level transport (you usually don't touch this directly)
bus = MqttComm("192.168.1.200", [("foo/#", 1)],
               on_msg_callback=lambda t, d: print(t, d))
bus.mqtt_init()
bus.sendIt("foo/bar", {"hello": "world"})
resp = bus.getIt_f("foo/bar", timeout=5)  # blocks; returns {timestamp,data} or None
bus.flush_and_close()

# High-level bridge (the actual server)
server = Server(agent_ws_url="ws://localhost:8001",
                mqtt_ip="192.168.1.200", bPrint=True)
# ... runs in background threads until shutdown() is called
server.shutdown()
```

## Limitations

- **One plan at a time.** robot_agent is unsafe under concurrent skill
  execution. Server holds an `asyncio.Lock` and rejects overlapping
  plans with `{action:"busy", isdone:false}`.
- **Single broker, multiple clients = cross-talk** on the result topic.
  Pass `request_id` to filter.
- **Final result only** — per-step events from the WS stream are not
  republished. (Add a topic + handler inside `Server.exec_plan` if you
  want progress events.)
- **WS connection per plan** — no pooling; failures surface as `error`
  field in the result.
