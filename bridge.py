"""MQTT ↔ robot_agent bridge.

Subscribes to `cotap/keti/task/plan`, forwards the plan to robot_agent's
WebSocket `/ws/agent` in direct mode, then publishes the final result to
`cotap/common/plan_result` in the schema expected by the existing MqttClient.

Run:
    python bridge.py
    python bridge.py --agent-url ws://localhost:8001 --mqtt-ip 192.168.1.200
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
from pyconnect.ros.comm import MqttComm


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


class AgentBridge:

    def __init__(self, agent_ws_url: str, mqtt_ip: str, bPrint: bool = True):
        self.agent_ws_url = agent_ws_url.rstrip("/") + "/ws/agent"
        self.bPrint = bPrint

        self.loop = asyncio.new_event_loop()
        self.loop_thread = threading.Thread(
            target=self._run_loop, daemon=True, name="bridge-asyncio"
        )
        self.loop_thread.start()

        self._exec_lock = asyncio.Lock()

        self.mqttComm = MqttComm(
            mqtt_ip,
            [(KETI_TASK_TOPIC, 1)],
            on_msg_callback=self._on_mqtt_message,
            loop_mode="start",
            bPrint=bPrint,
        )
        self.mqttComm.mqtt_init()

        self._publish_status(online=True)

    def _run_loop(self):
        asyncio.set_event_loop(self.loop)
        self.loop.run_forever()

    def _publish_status(self, online: bool):
        self.mqttComm.sendIt(BRIDGE_STATUS_TOPIC, {"online": online})

    def _on_mqtt_message(self, topic: str, tmpDict: dict):
        if topic != KETI_TASK_TOPIC:
            return

        try:
            data = tmpDict["contents"]["data"]
            plan = data["plan"]
        except (KeyError, TypeError) as e:
            if self.bPrint:
                print(f"[bridge] malformed plan message: {e} payload={tmpDict}")
            return

        request_id = data.get("request_id")

        asyncio.run_coroutine_threadsafe(
            self._exec_plan(plan, request_id), self.loop
        )

    async def _exec_plan(self, plan: str, request_id: str | None):
        if self._exec_lock.locked():
            if self.bPrint:
                print(f"[bridge] busy, rejecting plan: {plan!r}")
            self._publish_result(
                action="busy", isdone=False,
                error="bridge busy with another plan",
                request_id=request_id,
            )
            return

        async with self._exec_lock:
            if self.bPrint:
                print(f"[bridge] exec_plan: {plan!r} request_id={request_id}")

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
                    print(f"[bridge] {err_msg}")

            payload = dict(last_step_result)
            payload["action"] = last_action or "unknown"
            payload["isdone"] = success and err_msg is None
            if err_msg:
                payload["error"] = err_msg

            self._publish_result(request_id=request_id, **payload)

    def _publish_result(self, request_id: str | None = None, **fields):
        result = {"result": fields}
        if request_id is not None:
            result["request_id"] = request_id
        self.mqttComm.sendIt(PLAN_RESULT_TOPIC, result)

    def shutdown(self):
        if self.bPrint:
            print("[bridge] shutting down...")
        self._publish_status(online=False)
        self.mqttComm.flush_and_close(timeout=2.0)
        self.loop.call_soon_threadsafe(self.loop.stop)
        self.loop_thread.join(timeout=2.0)


def main():
    p = argparse.ArgumentParser(description="MQTT ↔ robot_agent bridge")
    p.add_argument("--agent-url", default=os.environ.get("AGENT_URL", "ws://localhost:8001"),
                   help="robot_agent base URL (default ws://localhost:8001)")
    p.add_argument("--mqtt-ip", default=os.environ.get("MQTT_SERVER_IP", "192.168.1.200"),
                   help="MQTT broker IP (or ip/user/pass)")
    p.add_argument("--quiet", action="store_true", help="suppress log output")
    args = p.parse_args()

    bridge = AgentBridge(
        agent_ws_url=args.agent_url,
        mqtt_ip=args.mqtt_ip,
        bPrint=not args.quiet,
    )

    stop_event = threading.Event()

    def _handle_sig(signum, frame):
        stop_event.set()

    signal.signal(signal.SIGINT, _handle_sig)
    signal.signal(signal.SIGTERM, _handle_sig)

    print(f"[bridge] running. agent={bridge.agent_ws_url} broker={args.mqtt_ip}")
    print(f"[bridge] listening on {KETI_TASK_TOPIC}, publishing to {PLAN_RESULT_TOPIC}")
    try:
        stop_event.wait()
    finally:
        bridge.shutdown()


if __name__ == "__main__":
    sys.exit(main())
