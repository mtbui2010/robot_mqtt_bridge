# robot_mqtt_bridge

MQTT ↔ [robot_agent](../robot_agent) bridge as an installable Python
package. Sends structured plans (`find::apple\nmove::kitchen`) over MQTT
to a running robot_agent, executes them via `/ws/agent` in direct mode
(no LLM), and publishes the result back.

```
your script / mosquitto_pub ─► broker ─► robot-mqtt-server ─► ws://…/ws/agent
                                              │
                       ◄────── broker ◄───────┘ (final result)
```

---

## Quickstart

```bash
# 0. install
pip install -e .

# 1. start the robot backend (port 8001)
cd ../kcare_robot && uvicorn kcare_robot.main:app --port 8001

# 2. start the bridge
robot-mqtt-server --mqtt-ip <broker-ip>

# 3. send a plan
robot-mqtt-client --mqtt-ip <broker-ip> action move kitchen
# → "move complete"   (exit code 0)
```

That's it. Skip to [Common scenarios](#common-scenarios) for what to do
next.

---

## Install

```bash
pip install -e .       # editable (dev)
pip install .          # normal
```

Dependencies (auto-installed): `paho-mqtt`, `websockets`, `numpy`.

Provides two CLI commands:
- `robot-mqtt-server` — the bridge daemon
- `robot-mqtt-client` — convenience CLI for sending plans

Public Python API:
```python
from robot_mqtt_bridge import MqttComm, Server, MqttClient
```

---

## CLI cheatsheet

### Server

| Command | Purpose |
|---|---|
| `robot-mqtt-server` | start with defaults (`ws://0.0.0.0:8001`, `0.0.0.0` broker) |
| `robot-mqtt-server --agent-url ws://HOST:8001 --mqtt-ip BROKER` | explicit endpoints |
| `robot-mqtt-server --mqtt-ip "BROKER/user/pass"` | broker auth (slash-separated) |
| `robot-mqtt-server --quiet` | suppress logs |
| `AGENT_URL=… MQTT_SERVER_IP=… robot-mqtt-server` | via env vars |

Stops cleanly on `Ctrl+C` / `SIGTERM` (publishes `online:false`).

### Client

| Command | Purpose |
|---|---|
| `robot-mqtt-client action move kitchen` | one skill, target = "kitchen" |
| `robot-mqtt-client action lift 200` | lift to 200 |
| `robot-mqtt-client action grip 1` | grip close (`0` = open) |
| `robot-mqtt-client action init_arm` | reset arm |
| `robot-mqtt-client plan "find::apple\nmove::kitchen\npick::"` | raw multi-step plan |
| `robot-mqtt-client --mqtt-ip BROKER plan "..."` | non-default broker |
| `robot-mqtt-client --timeout 60 plan "..."` | custom timeout (default 180s) |

Exit code: `0` success, `1` failure/timeout.

Defaults: both CLIs use `--mqtt-ip 0.0.0.0` and the server uses
`--agent-url ws://0.0.0.0:8001`. Override with flags or
`MQTT_SERVER_IP` / `AGENT_URL` env vars.

---

## Common scenarios

### A. Same machine, default ports

```bash
# terminal 1
uvicorn kcare_robot.main:app --port 8001
# terminal 2
robot-mqtt-server
# terminal 3
robot-mqtt-client action move kitchen
```

### B. Bridge on robot, broker on a different host

```bash
robot-mqtt-server --mqtt-ip 192.168.1.200
```

Clients elsewhere on the network point at the same broker:
```bash
robot-mqtt-client --mqtt-ip 192.168.1.200 action lift 100
```

### C. Sending a multi-step plan

```bash
robot-mqtt-client plan "find::apple\nmove::kitchen\npick::"
```

Newlines are literal `\n` in the CLI (decoded internally). Or pipe from
a file:

```bash
robot-mqtt-client plan "$(cat my_plan.txt | tr '\n' '|' | sed 's/|/\\n/g')"
```

### D. Driving from Python (no broker, no fuss)

```python
from robot_mqtt_bridge import MqttClient

client = MqttClient(bPrint=True, mqtt_ip="192.168.1.200")
try:
    client.actionWork("move", "kitchen")
    client.actionWork("lift", 200)
    ok = client.commWork("find::apple\nmove::kitchen\npick::", timeout=180)
    print("done" if ok else "failed")
finally:
    client.close()
```

### E. Driving from a non-Python language (raw MQTT)

Anything that speaks MQTT can send a plan. JS / Go / shell — just match
the envelope:

```bash
# subscribe to results
mosquitto_sub -h 192.168.1.200 -t 'cotap/common/plan_result' -v

# publish a plan
mosquitto_pub -h 192.168.1.200 -t 'cotap/keti/task/plan' -m '{
  "timestamp": "2026-05-27 14:30:00",
  "contents": {"data": {"plan": "find::apple\nmove::kitchen"}}
}'
```

### F. Concurrent clients — use `request_id`

When several clients share a broker, results on
`cotap/common/plan_result` get cross-delivered. Add a `request_id` to
the plan and filter on it client-side:

```python
import uuid
req_id = str(uuid.uuid4())

# (via raw paho — MqttClient currently doesn't expose request_id)
payload = {
    "timestamp": time.strftime("%Y-%m-%d %H:%M:%S"),
    "contents": {"data": {"plan": "lift::200", "request_id": req_id}},
}
```

The server echoes `request_id` at `contents.data.request_id` in the
response.

### G. Embedding the bridge inside another Python service

```python
from robot_mqtt_bridge import Server

server = Server(agent_ws_url="ws://localhost:8001",
                mqtt_ip="192.168.1.200")
# server runs on background threads; do your own work...
server.shutdown()
```

### H. Just want pub/sub, no plans

```python
from robot_mqtt_bridge import MqttComm

bus = MqttComm("192.168.1.200", [("foo/#", 1)],
               on_msg_callback=lambda t, d: print(t, d))
bus.mqtt_init()
bus.sendIt("foo/bar", {"hello": "world"})         # envelope-wrapped
resp = bus.getIt_f("foo/bar", timeout=5)          # blocking get
bus.flush_and_close()
```

---

## Protocol reference

### Topics

| Topic | Dir | Schema (inside the envelope) |
|---|---|---|
| `cotap/keti/task/plan` | client → bridge | `{plan:"find::apple\nmove::kitchen", request_id?:"..."}` |
| `cotap/common/plan_result` | bridge → client | `{result:{action,isdone,error?,...}, request_id?:"..."}` |
| `cotap/keti/bridge/status` | bridge → all (retained, LWT) | `{online:true\|false}` |

All messages share the envelope produced by `MqttComm.sendIt()`:

```json
{
  "timestamp": "2026-05-27 14:30:00",
  "contents": {"data": { ... topic-specific schema ... }}
}
```

### Plan syntax (`direct=true` mode of robot_agent)

```
SKILL::ARGS                        # one task
SKILL1::ARGS && SKILL2::ARGS       # parallel within a step
SKILL_A::...                       # multiple lines = sequential steps
SKILL_B::...
```

Example: `find::apple\nmove::kitchen && grip::1\npick::`

### Result fields

- `action` — name of the LAST step:
  - single task → `"move"`
  - parallel → `"move&grip"`
  - bridge rejected (busy) → `"busy"`
  - undetermined → `"unknown"`
- `isdone` — `true` if robot_agent reported `done.success` AND no
  transport error.
- `error` — present only on failure (transport error or agent `error`
  event).
- Plus any fields returned by the last skill (`pose`, `data`, etc.).

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| Client hangs, eventually prints `No result` | Bridge not running, or `robot_agent` down | `robot-mqtt-server` running? `curl localhost:8001/skills` returns? |
| `action="busy"` returned immediately | Another plan still executing | Wait or queue plans client-side (bridge enforces serial execution) |
| `action="unknown"`, `error="ws transport error: ..."` | Bridge can't reach robot_agent | Check `--agent-url`; agent listening on port? |
| Cross-talk between clients | Multiple clients on same broker | Use `request_id` to filter (see scenario F) |
| Doesn't connect to broker on a fresh container | `0.0.0.0` default doesn't resolve | Set `--mqtt-ip` or `MQTT_SERVER_IP` to the actual broker host |
| Bridge died silently, retained `online:true` lingers | Last Will didn't trigger (clean shutdown) | The bridge publishes `online:false` on `SIGTERM`; LWT only fires on hard disconnect |

To inspect what's flowing on the broker:

```bash
mosquitto_sub -h <broker> -t 'cotap/#' -v
```

---

## Limitations

- **One plan at a time.** robot_agent is unsafe under concurrent skill
  execution. The bridge holds an `asyncio.Lock` and rejects overlapping
  plans with `action:"busy"`.
- **Final result only.** Per-step events from the WebSocket stream
  (`step_start`, `step_log`, `step_done`) are not republished. Add a
  topic + handler inside `Server.exec_plan` if you want progress.
- **WS connection per plan.** No pooling; transport failures appear in
  the result's `error` field.
- **No request queue.** Clients must serialize themselves (or accept
  `busy` rejections).
- **`0.0.0.0` default IP** isn't a valid "connect to" address on all
  platforms — works on Linux (resolves to loopback) but be explicit
  with `--mqtt-ip` in production.

---

## Layout

```
robot_mqtt_bridge/
├── pyproject.toml             # metadata + deps + 2 CLI entry points
├── README.md
└── robot_mqtt_bridge/         # the package
    ├── __init__.py            # exports MqttComm, Server, MqttClient
    ├── mqtt_comm.py           # MqttComm — paho-mqtt wrapper
    ├── server.py              # Server class + `robot-mqtt-server` CLI
    └── client.py              # MqttClient + `robot-mqtt-client` CLI
```
