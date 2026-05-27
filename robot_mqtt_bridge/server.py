"""MQTT bridge server for robot_agent.

Subscribes to `cotap/keti/task/plan`, forwards the structured plan to
robot_agent's `/ws/agent` WebSocket in direct mode (no LLM), then publishes
the final aggregated result to `cotap/common/plan_result` in the schema
the existing MqttClient expects.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import signal
import sys
import threading
from datetime import datetime

import websockets

from .mqtt_comm import MqttComm


KETI_TASK_TOPIC = "cotap/keti/task/plan"
PLAN_RESULT_TOPIC = "cotap/common/plan_result"
BRIDGE_STATUS_TOPIC = "cotap/keti/bridge/status"


def _task_action_name(task_str: str) -> str:
    """Extract action name(s) from a task line like 'find::apple' or 'a::1 && b::2'."""
    actions = []
    for part in task_str.split("&&"):
        part = part.strip()
        if "::" in part:
            actions.append(part.split("::", 1)[0].strip())
    return "&".join(actions) if actions else task_str.strip()


class Server:

    def __init__(self, agent_ws_url: str = "ws://localhost:8001",
                 mqtt_ip: str | None = None, bPrint: bool = True):
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

        mqtt_ip = mqtt_ip or os.environ.get("MQTT_SERVER_IP")
        if not mqtt_ip:
            raise ValueError(
                "[Server] MQTT broker IP is required. "
                "Pass mqtt_ip=... or set the MQTT_SERVER_IP env var."
            )
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
        if not self.mqttComm.wait_connected(timeout=5.0):
            raise RuntimeError(
                f"[Server] could not connect to MQTT broker at "
                f"{mqtt_ip} within 5s"
            )
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


def main():
    p = argparse.ArgumentParser(
        prog="robot-mqtt-server",
        description="MQTT ↔ robot_agent bridge server",
    )
    p.add_argument("--agent-url",
                   default=os.environ.get("AGENT_URL", "ws://localhost:8001"),
                   help="robot_agent base URL (default ws://localhost:8001)")
    p.add_argument("--mqtt-ip",
                   default=os.environ.get("MQTT_SERVER_IP"),
                   help="MQTT broker IP (or ip/user/pass). "
                        "Required: set this flag or MQTT_SERVER_IP env var.")
    p.add_argument("--quiet", action="store_true", help="suppress log output")
    args = p.parse_args()

    if not args.mqtt_ip:
        p.error("--mqtt-ip is required (or set MQTT_SERVER_IP env var)")

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
