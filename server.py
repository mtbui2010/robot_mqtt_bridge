"""MQTT bridge server for robot_agent.

Subscribes to `cotap/keti/task/plan`, forwards the structured plan to
robot_agent's `/ws/agent` WebSocket in direct mode (no LLM), then publishes
the final aggregated result to `cotap/common/plan_result` in the schema
the existing MqttClient expects.

Run:
    python server.py
    python server.py --agent-url ws://localhost:8001 --mqtt-ip 192.168.1.200

Contains:
    - MqttComm  : low-level MQTT pub/sub wrapper (transport)
    - Server    : application-level bridge (MQTT plan in → WS out → MQTT result back)
    - main()    : entry point
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta

import numpy as np
import paho.mqtt.client as mqtt
import websockets


KETI_TASK_TOPIC = "cotap/keti/task/plan"
PLAN_RESULT_TOPIC = "cotap/common/plan_result"
BRIDGE_STATUS_TOPIC = "cotap/keti/bridge/status"


# ---------------------------------------------------------------------------
# MqttComm — transport
# ---------------------------------------------------------------------------

class MqttComm:

    def __init__(self, connectIP, subscribeTopics,
                 on_msg_callback=None, loop_mode="start", ttl_sec=30,
                 bPrint=False, external_stop_event=None,
                 will_topic=None, will_payload=None):

        self.bPrint = bPrint
        self.ttl_sec = ttl_sec

        self.connectIP, self.username, self.password = self._parse_connect_ip(connectIP)

        self.loop_mode = loop_mode
        self.subscribeTopics = subscribeTopics
        self.external_msg_handler = on_msg_callback
        self._publish_infos = []
        self._stop_event = external_stop_event or threading.Event()
        self._resp_lock = threading.Lock()
        self._will_topic = will_topic
        self._will_payload = will_payload

    def _parse_connect_ip(self, s):
        if s is None:
            return None, None, None

        s = str(s).strip()
        parts = s.split('/')

        if len(parts) == 1:
            return parts[0].strip(), None, None

        ip = (parts[0] if len(parts) > 0 else "").strip()
        user = (parts[1] if len(parts) > 1 else "").strip() or None
        pw = (parts[2] if len(parts) > 2 else "").strip() or None
        return ip, user, pw

    def request_stop(self):
        try:
            self._stop_event.set()
        except Exception:
            pass

    def is_stopped(self):
        return self._stop_event.is_set()

    def convert_all_types(self, obj):
        if isinstance(obj, dict):
            return {k: self.convert_all_types(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self.convert_all_types(i) for i in obj]
        elif isinstance(obj, (np.integer, np.int32, np.int64)):
            return int(obj)
        elif isinstance(obj, (np.floating, np.float32, np.float64)):
            return float(obj)
        elif isinstance(obj, np.bool_):
            return bool(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif hasattr(obj, 'data'):
            return obj.data
        elif isinstance(obj, bytes):
            return obj.decode('utf-8')
        else:
            return obj

    def mqtt_init(self):
        self.debug = True
        self.spin_time = 0.05
        self.mqtt_connect_flag = False
        self.mqtt_response_dict = defaultdict(deque)

        self.cleanup_thread = threading.Thread(target=self.cleanup_loop, daemon=True)
        self.cleanup_thread.start()

        self.con_mqtt = mqtt.Client()
        self.con_mqtt.on_message = self.mqtt_on_message
        self.con_mqtt.on_connect = self.mqtt_on_connect

        try:
            if self.username and self.password:
                self.con_mqtt.username_pw_set(self.username, self.password)
        except Exception as e:
            if self.bPrint:
                print(f"[MQTT] username/password set error: {e}")

        if self._will_topic:
            try:
                payload = self._will_payload
                if not isinstance(payload, (str, bytes)):
                    payload = json.dumps(payload)
                self.con_mqtt.will_set(self._will_topic, payload, qos=1, retain=True)
            except Exception as e:
                if self.bPrint:
                    print(f"[MQTT] will_set failed: {e}")

        try:
            self.con_mqtt.connect(self.connectIP, 1883)
        except Exception as e:
            self.mqtt_connect_flag = False
            if self.bPrint:
                print(f"[MQTT] Connect failed: {e}")
            return

        if self.loop_mode == "start":
            self.con_mqtt.loop_start()
        elif self.loop_mode == "forever":
            self.con_mqtt.loop_forever()
        elif self.loop_mode == "eventlet":
            import eventlet
            eventlet.spawn(self.mqtt_start)

    def mqtt_start(self):
        try:
            if self.bPrint:
                print("[MQTT] Connecting to broker...")
            self.con_mqtt.connect(self.connectIP, 1883)
            self.con_mqtt.loop_forever()
        except Exception as e:
            self.mqtt_connect_flag = False
            if self.bPrint:
                print(f"[MQTT Error] {e}")

    def mqtt_stop(self):
        if self.mqtt_connect_flag:
            self.con_mqtt.loop_stop()

    def mqtt_on_message(self, client, userData, msg):
        try:
            tmpMsg = msg.payload.decode('utf-8')
            tmpDict = json.loads(tmpMsg)
        except (UnicodeDecodeError, json.JSONDecodeError) as e:
            if self.bPrint:
                print(f"[MQTT] dropping malformed message on {msg.topic}: {e}")
            return

        topic = msg.topic
        if tmpDict is None or len(tmpDict) == 0:
            return

        if self.external_msg_handler:
            try:
                self.external_msg_handler(topic, tmpDict)
            except Exception as e:
                if self.bPrint:
                    print(f"[MQTT] handler raised on {topic}: {e}")

    def setMqttResponseData(self, topic, responseData):
        with self._resp_lock:
            self.mqtt_response_dict[topic].append({
                "timestamp": datetime.now(),
                "data": responseData,
            })

    def getIt_f(self, topic, timeout):
        """Block until a response appears on topic; return {timestamp,data} or None."""
        deadline = time.time() + timeout
        while time.time() < deadline:
            with self._resp_lock:
                queue = self.mqtt_response_dict.get(topic)
                if queue:
                    return queue.popleft()
            if self.is_stopped():
                return None
            time.sleep(self.spin_time)
        return None

    def cleanup_expired_responses(self):
        now = datetime.now()
        with self._resp_lock:
            for topic in list(self.mqtt_response_dict.keys()):
                queue = self.mqtt_response_dict[topic]
                self.mqtt_response_dict[topic] = deque(
                    [item for item in queue if now - item["timestamp"] < timedelta(seconds=self.ttl_sec)]
                )

    def cleanup_loop(self):
        while not self._stop_event.is_set():
            self.cleanup_expired_responses()
            time.sleep(5)

    def mqtt_on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            if self.bPrint:
                print("[MQTT] Connected.")
            self.con_mqtt.subscribe(self.subscribeTopics)
            self.mqtt_connect_flag = True
        else:
            if self.bPrint:
                print(f"[MQTT] Connect failed with result code {rc}")

    def publish_mqtt(self, topic, result):
        if result is None or len(result) == 0:
            return

        converted = self.convert_all_types(result)
        jsonString = json.dumps(converted)

        if self.mqtt_connect_flag:
            info = self.con_mqtt.publish(topic, jsonString, 1)
            try:
                self._publish_infos.append(info)
            except Exception:
                pass

    def flush_and_close(self, timeout=2.0):
        try:
            self._stop_event.set()
        except Exception:
            pass

        for info in list(self._publish_infos):
            try:
                info.wait_for_publish(timeout=timeout)
            except Exception:
                pass
        self._publish_infos.clear()

        try:
            self.con_mqtt.disconnect()
        except Exception:
            pass
        try:
            self.con_mqtt.loop_stop()
        except Exception:
            pass

    def sendIt(self, topic, content):
        contents = {'data': content}
        strNow = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        command = {'timestamp': strNow, 'contents': contents}
        self.publish_mqtt(topic, command)


# ---------------------------------------------------------------------------
# Server — application bridge
# ---------------------------------------------------------------------------

def _task_action_name(task_str: str) -> str:
    """Extract action name(s) from a task line like 'find::apple' or 'a::1 && b::2'."""
    actions = []
    for part in task_str.split("&&"):
        part = part.strip()
        if "::" in part:
            actions.append(part.split("::", 1)[0].strip())
    return "&".join(actions) if actions else task_str.strip()


class Server:

    def __init__(self, agent_ws_url: str, mqtt_ip: str, bPrint: bool = True):
        self.bPrint = bPrint
        self.agent_ws_url = agent_ws_url.rstrip("/") + "/ws/agent"

        self.KETI_TASK_MQTT_TOPIC = KETI_TASK_TOPIC
        self.PLAN_RESULT_MQTT_TOPIC = PLAN_RESULT_TOPIC

        self.loop = asyncio.new_event_loop()
        self.loop_thread = threading.Thread(
            target=self._run_loop, daemon=True, name="server-asyncio"
        )
        self.loop_thread.start()

        self._exec_lock = asyncio.Lock()

        self.mqttComm = MqttComm(
            mqtt_ip,
            [(self.KETI_TASK_MQTT_TOPIC, 1)],
            on_msg_callback=self.on_mqtt_message,
            loop_mode="start",
            bPrint=bPrint,
            will_topic=BRIDGE_STATUS_TOPIC,
            will_payload={"timestamp": datetime.now().isoformat(),
                          "contents": {"data": {"online": False}}},
        )
        self.mqttComm.mqtt_init()
        self.mqttComm.sendIt(BRIDGE_STATUS_TOPIC, {"online": True})

    def _run_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def on_mqtt_message(self, topic, tmpDict):
        contents = tmpDict.get('contents', {})
        dataDict = contents.get('data') if isinstance(contents, dict) else None
        if not isinstance(dataDict, dict):
            if self.bPrint:
                print(f"[server] malformed envelope on {topic}: {tmpDict}")
            return

        self.mqttComm.setMqttResponseData(topic, dataDict)

        if topic == self.KETI_TASK_MQTT_TOPIC:
            plan = dataDict.get('plan')
            if not plan:
                if self.bPrint:
                    print(f"[server] plan message missing 'plan' field: {dataDict}")
                return
            request_id = dataDict.get('request_id')
            if self.bPrint:
                print(f"[server] plan : {plan!r} request_id={request_id}")
            asyncio.run_coroutine_threadsafe(
                self.exec_plan(plan, request_id), self.loop
            )

    async def exec_plan(self, plan: str, request_id=None):
        if self._exec_lock.locked():
            if self.bPrint:
                print(f"[server] busy, rejecting plan: {plan!r}")
            self.sendWork(
                action="busy",
                kwargs={"isdone": False, "error": "bridge busy with another plan"},
                request_id=request_id,
            )
            return

        async with self._exec_lock:
            last_action = ""
            last_step_result: dict = {}
            success = False
            err_msg = None

            try:
                async with websockets.connect(self.agent_ws_url) as ws:
                    await ws.send(json.dumps({
                        "prompt": plan,
                        "lang": "en",
                        "direct": True,
                    }))
                    async for raw in ws:
                        ev = json.loads(raw)
                        kind = ev.get("event")

                        if kind == "step_start":
                            last_action = _task_action_name(ev.get("task", ""))
                        elif kind == "step_done":
                            res = ev.get("result")
                            if isinstance(res, dict):
                                last_step_result = res
                        elif kind == "done":
                            success = bool(ev.get("success"))
                            break
                        elif kind == "error":
                            err_msg = ev.get("msg") or "agent error"
                            break
            except Exception as e:
                err_msg = f"ws transport error: {e}"
                if self.bPrint:
                    print(f"[server] {err_msg}")

            kwargs = dict(last_step_result)
            kwargs["isdone"] = success and err_msg is None
            if err_msg:
                kwargs["error"] = err_msg

            self.sendWork(action=last_action or "unknown", kwargs=kwargs, request_id=request_id)

    def sendWork(self, action, kwargs, request_id=None):
        """Publish a result message in the schema MqttClient expects."""
        newKwargs = dict(kwargs)
        newKwargs['action'] = action

        if self.bPrint:
            print(f"[server] sendWork action={action} kwargs={newKwargs}")

        payload = {'result': newKwargs}
        if request_id is not None:
            payload['request_id'] = request_id
        self.mqttComm.sendIt(self.PLAN_RESULT_MQTT_TOPIC, payload)

    def shutdown(self):
        if self.bPrint:
            print("[server] shutting down...")
        try:
            self.mqttComm.sendIt(BRIDGE_STATUS_TOPIC, {"online": False})
        except Exception:
            pass
        self.mqttComm.flush_and_close(timeout=2.0)
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.loop_thread.join(timeout=2.0)


# ---------------------------------------------------------------------------
# entry point
# ---------------------------------------------------------------------------

def main():
    p = argparse.ArgumentParser(description="MQTT ↔ robot_agent bridge server")
    p.add_argument("--agent-url",
                   default=os.environ.get("AGENT_URL", "ws://localhost:8001"),
                   help="robot_agent base URL (default ws://localhost:8001)")
    p.add_argument("--mqtt-ip",
                   default=os.environ.get("MQTT_SERVER_IP", "192.168.1.200"),
                   help="MQTT broker IP (or ip/user/pass)")
    p.add_argument("--quiet", action="store_true", help="suppress log output")
    args = p.parse_args()

    server = Server(
        agent_ws_url=args.agent_url,
        mqtt_ip=args.mqtt_ip,
        bPrint=not args.quiet,
    )

    stop_event = threading.Event()

    def _handle_sig(signum, frame):
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_sig)
    signal.signal(signal.SIGTERM, _handle_sig)

    print(f"[server] running. agent={server.agent_ws_url} broker={args.mqtt_ip}")
    print(f"[server] listening on {KETI_TASK_TOPIC}, publishing to {PLAN_RESULT_TOPIC}")
    try:
        stop_event.wait()
    finally:
        server.shutdown()


if __name__ == "__main__":
    sys.exit(main())
