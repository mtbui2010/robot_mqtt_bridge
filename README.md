# robot_mqtt_bridge

A standalone process that bridges an MQTT broker with a running
[robot_agent](../robot_agent) instance. Receives structured plans over
MQTT, forwards them to robot_agent's `/ws/agent` WebSocket in direct
mode (no LLM), and publishes the final result back to MQTT.

```
MQTT client ──► broker ──► bridge.py ──► ws://localhost:8001/ws/agent
                                              │
              ◄────────── broker ◄──────────  │ (final result)
```

## Install

```bash
pip install -r requirements.txt
# pyconnect must already be importable (MqttComm comes from it)
pip install -e ../pyconnect
```

## Run

Make sure robot_agent is up first:

```bash
cd ../kcare_robot && uvicorn kcare_robot.main:app --port 8001
```

Then start the bridge:

```bash
cd robot_mqtt_bridge

# defaults: agent=ws://localhost:8001, broker=$MQTT_SERVER_IP or 192.168.1.200
python bridge.py

# explicit
python bridge.py --agent-url ws://localhost:8001 --mqtt-ip 192.168.1.200

# with broker auth (ip/user/pass — same convention as MqttComm)
python bridge.py --mqtt-ip "192.168.1.200/admin/secret"

# quiet mode
python bridge.py --quiet
```

Environment variable equivalents:

```bash
AGENT_URL=ws://localhost:8001 MQTT_SERVER_IP=192.168.1.200 python bridge.py
```

## Topics

| Topic | Dir | Schema |
|---|---|---|
| `cotap/keti/task/plan` | in | `{contents:{data:{plan:"find::apple\nmove::kitchen", request_id?:"..."}}}` |
| `cotap/common/plan_result` | out | `{contents:{data:{result:{action,isdone,error?,...}, request_id?:"..."}}}` |
| `cotap/keti/bridge/status` | out (retained + LWT) | `{contents:{data:{online:true|false}}}` |

The envelope `{timestamp, contents:{data:...}}` is produced by
`MqttComm.sendIt()`. The bridge consumes the SAME envelope on input.

### `action` field semantics

- Single-task step (`find::apple`): `action = "find"`
- Parallel step (`move::a && grip::1`): `action = "move&grip"`
- Bridge busy (refused): `action = "busy"`
- Unable to determine: `action = "unknown"`

`isdone` mirrors robot_agent's final `done.success` (false if any step
returned `isdone:false` or a transport error occurred).

## Usage

### From the CLI

Test with `mosquitto_pub` / `mosquitto_sub`:

```bash
# subscribe in one terminal
mosquitto_sub -h 192.168.1.200 -t 'cotap/common/plan_result' -v

# publish a structured plan
mosquitto_pub -h 192.168.1.200 -t 'cotap/keti/task/plan' -m '{
  "timestamp": "2026-05-27 14:30:00",
  "contents": {"data": {"plan": "find::apple\nmove::kitchen"}}
}'
```

Expected response on `cotap/common/plan_result`:

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

With an explicit `request_id` for correlation:

```bash
mosquitto_pub -h 192.168.1.200 -t 'cotap/keti/task/plan' -m '{
  "timestamp": "2026-05-27 14:30:00",
  "contents": {"data": {
    "plan": "lift::200",
    "request_id": "req-abc-123"
  }}
}'
```

→ The bridge echoes `request_id` under `contents.data.request_id`.

### From Python (using the existing `MqttClient`)

The `MqttClient` in your code already works against this bridge — no
changes needed. It publishes to `cotap/keti/task/plan` and blocks on
`cotap/common/plan_result`:

```python
from your_pkg.mqtt_client import MqttClient

client = MqttClient(bPrint=True)

# simple skill calls (one per request)
client.actionWork("move", "kitchen")     # → plan = "move::kitchen"
client.actionWork("lift", "200")         # → plan = "lift::200"
client.actionWork("grip", "1")           # → plan = "grip::1000"

# arbitrary multi-step plan
ok = client.commWork("find::apple\nmove::kitchen\npick::", timeout=180)
print("success" if ok else "failed/timeout")
```

Note: the existing `MqttClient.on_mqtt_message` references undefined
`self.COMMAND_MQTT_TOPIC` / `self.STT_MQTT_TOPIC`. Because the bridge
publishes the bridge-status message on `cotap/keti/bridge/status` (NOT
under `cotap/common/...` which the client subscribes to with a
wildcard), the client will not receive any messages that would trigger
the buggy branch. Only `cotap/common/plan_result` reaches the client
and that topic isn't checked in the broken `if`.

### From Python (lightweight ad-hoc client)

If you don't want the full `MqttClient`, here's a minimal request/wait
pattern using `paho-mqtt` directly:

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

## Limitations

- **One plan at a time.** robot_agent is unsafe under concurrent skill
  execution. The bridge holds an `asyncio.Lock` and rejects overlapping
  plans with `{action:"busy", isdone:false}`.
- **Single broker, multiple clients = cross-talk.** Without
  `request_id`, both clients receive each other's result on
  `cotap/common/plan_result`. Use `request_id` to filter.
- **Per-step events are not published.** Only the final aggregated
  result is sent. (The WebSocket stream includes `step_start`,
  `step_log`, `step_done` — add a topic + handler in `_exec_plan` if
  you need progress.)
- **No reconnection backoff.** Paho's `loop_start()` auto-reconnects,
  but the WS connection to robot_agent is opened per-plan; failures
  surface as `error` field in the result.
