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

# 2. start the bridge   (broker IP is REQUIRED — see "Why no default?" below)
robot-mqtt-server --mqtt-ip 192.168.1.200

# 3. send a plan
robot-mqtt-client --mqtt-ip 192.168.1.200 action move kitchen
# → prints full result JSON; exit 0 on isdone=true, 1 otherwise
```

Or use the env var once and skip the flag:

```bash
export MQTT_SERVER_IP=192.168.1.200
robot-mqtt-server &
robot-mqtt-client action move kitchen
```

---

## Why no default broker IP?

The broker is almost never on the same machine as the bridge. There is
no sensible default — old versions defaulted to `0.0.0.0`/`localhost`
which silently routed to the local machine and gave confusing
"connection refused" errors. The current version **requires you to be
explicit**: either `--mqtt-ip <ip>` or `MQTT_SERVER_IP=<ip>`.

If you forget:

```
$ robot-mqtt-server
robot-mqtt-server: error: --mqtt-ip is required (or set MQTT_SERVER_IP env var)
```

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
| `robot-mqtt-server --mqtt-ip BROKER` | start; agent URL defaults to `ws://localhost:8001` |
| `robot-mqtt-server --agent-url ws://HOST:8001 --mqtt-ip BROKER` | explicit endpoints |
| `robot-mqtt-server --mqtt-ip "BROKER/user/pass"` | broker auth (slash-separated) |
| `robot-mqtt-server --mqtt-ip BROKER --quiet` | suppress logs |
| `AGENT_URL=… MQTT_SERVER_IP=… robot-mqtt-server` | via env vars |

Stops cleanly on `Ctrl+C` / `SIGTERM` (publishes `online:false`).

### Client

| Command | Purpose |
|---|---|
| `robot-mqtt-client --mqtt-ip BROKER action move kitchen` | one skill, target = "kitchen" |
| `robot-mqtt-client --mqtt-ip BROKER action lift 200` | lift to 200 |
| `robot-mqtt-client --mqtt-ip BROKER action grip 1` | grip close (`0` = open) |
| `robot-mqtt-client --mqtt-ip BROKER action init_arm` | reset arm |
| `robot-mqtt-client --mqtt-ip BROKER plan "find::apple\nmove::kitchen\npick::"` | raw multi-step plan |
| `robot-mqtt-client --mqtt-ip BROKER --timeout 60 plan "..."` | custom timeout (default 180s) |
| `robot-mqtt-client --mqtt-ip BROKER --quiet plan "..."` | suppress MQTT/connect logs (still prints result JSON) |

`--mqtt-ip` is required (or set `MQTT_SERVER_IP`).

**Output**: full result as pretty-printed JSON on stdout. **Exit code**:
`0` if `isdone` true, `1` otherwise (failure / timeout). Pipe stdout
into `jq` to extract fields:

```bash
robot-mqtt-client --mqtt-ip 192.168.1.200 action lift 200 | jq -r .isdone
```

---

## Common scenarios

### A. Same machine

```bash
export MQTT_SERVER_IP=192.168.1.200    # adjust for your broker

# terminal 1
uvicorn kcare_robot.main:app --port 8001
# terminal 2
robot-mqtt-server
# terminal 3
robot-mqtt-client action move kitchen
```

### B. Multi-step plan

```bash
robot-mqtt-client --mqtt-ip 192.168.1.200 \
    plan "find::apple\nmove::kitchen\npick::"
```

Newlines are literal `\n` in the CLI (decoded internally). Or pipe from
a file:

```bash
robot-mqtt-client --mqtt-ip 192.168.1.200 \
    plan "$(cat my_plan.txt | tr '\n' '|' | sed 's/|/\\n/g')"
```

### C. Driving from Python

```python
from robot_mqtt_bridge import MqttClient

client = MqttClient(bPrint=True, mqtt_ip="192.168.1.200")
try:
    result = client.actionWork("move", "kitchen")
    # result is a dict like:
    # {"action": "move", "isdone": True, "pose": [...], ...}
    print(result["isdone"], result.get("pose"))

    result = client.commWork("find::apple\nmove::kitchen\npick::", timeout=180)
    if result is None:
        print("timeout")
    elif not result["isdone"]:
        print("failed:", result.get("error"))
    else:
        print("done:", result)
finally:
    client.close()
```

### D. Driving from non-Python (raw MQTT)

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

### E. Concurrent clients — use `request_id`

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

### F. Embedding the bridge inside another Python service

```python
from robot_mqtt_bridge import Server

server = Server(agent_ws_url="ws://localhost:8001",
                mqtt_ip="192.168.1.200")
# server runs on background threads; do your own work...
server.shutdown()
```

### G. Just want pub/sub, no plans

```python
from robot_mqtt_bridge import MqttComm

bus = MqttComm("192.168.1.200", [("foo/#", 1)],
               on_msg_callback=lambda t, d: print(t, d))
bus.mqtt_init()
if not bus.wait_connected(5.0):
    raise RuntimeError("broker unreachable")
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

The bridge publishes back the **full** result of the last step the agent
executed, plus a few synthesised fields. Example:

```json
{
  "action": "move",
  "isdone": true,
  "pose": [1.2, 3.4],
  "msg": "moved to kitchen"
}
```

- `action` — name of the LAST step:
  - single task → `"move"`
  - parallel → `"move&grip"`
  - bridge rejected (busy) → `"busy"`
  - undetermined → `"unknown"`
- `isdone` — `true` if robot_agent reported `done.success` AND no
  transport error.
- `error` — present only on failure (transport error or agent `error`
  event).
- Plus any fields the skill itself returned (`pose`, `data`, ...).

### Python return values

| Call | Returns | When |
|---|---|---|
| `MqttClient.commWork(plan, timeout)` | `dict` | bridge replied (whether `isdone` true or false) |
| `MqttClient.commWork(plan, timeout)` | `None` | timeout or client was stopped |
| `MqttClient.commWork(plan, timeout)` | raises `RuntimeError` | malformed bridge response |
| `MqttClient.actionWork(action, target)` | same as `commWork` | wraps a single-skill plan |

Always treat the result as a dict — check `result is None` first, then
`result["isdone"]`.

---

## Troubleshooting

| Symptom | Likely cause | Fix |
|---|---|---|
| `--mqtt-ip is required` on startup | No flag and no env var | Set `--mqtt-ip` or export `MQTT_SERVER_IP` |
| `[MQTT] Connect failed: [Errno 111] Connection refused` | No broker at the IP you gave | `nc -zv <broker> 1883` to verify; check `192.168.1.X` != your own machine's IP unless broker is local |
| `Connection refused` when using `--mqtt-ip 0.0.0.0` or your own LAN IP | IP routes back to YOUR machine; no broker there | Use the IP of the machine actually running the broker |
| Returns `None` (timeout) | Bridge not running, or `robot_agent` down | `robot-mqtt-server` running? `curl localhost:8001/skills` returns? |
| `action="busy"`, `isdone:false` immediately | Another plan still executing | Wait or queue plans client-side (bridge enforces serial execution) |
| `action="unknown"`, `error="ws transport error: ..."` | Bridge can't reach robot_agent | Check `--agent-url`; agent listening on port? |
| Cross-talk between clients | Multiple clients on same broker | Use `request_id` to filter (see scenario E) |
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
- **Broker IP must be reachable AND have a broker.** Connecting to your
  own machine's LAN IP routes back to localhost and fails if you don't
  also run a broker locally.

---

## Migration from 0.1.x

- **`MqttClient.commWork()` now returns `dict | None`** instead of
  `bool`. Existing code like `if client.commWork(...)` still works
  (dict is truthy) but the meaning changed:
  - old: `True` = success, `False` = failure, `None` = stopped
  - new: `dict` = response (success or failure — check `result["isdone"]`),
    `None` = timeout / stopped, raises on malformed response
  - The internal `print("complete")` / `print("failed")` lines were
    removed; the caller is responsible for displaying results now.
- **`MqttClient.actionWork()` same change** — returns a dict.
- **`--mqtt-ip` / `MQTT_SERVER_IP` is now required.** The old
  `0.0.0.0` default is gone.
- **CLI output changed** from a single status line to a pretty-printed
  JSON dump of the result. Exit code semantics unchanged.

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
