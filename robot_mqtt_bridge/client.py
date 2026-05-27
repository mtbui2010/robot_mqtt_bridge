"""MQTT client for sending structured plans to the bridge server.

Publishes to `cotap/keti/task/plan` and blocks on
`cotap/common/plan_result` for the response.
"""

from __future__ import annotations

import argparse
import json
import os
import sys

from .mqtt_comm import MqttComm


KETI_TASK_TOPIC = "cotap/keti/task/plan"
PLAN_RESULT_TOPIC = "cotap/common/plan_result"


class MqttClient:

    def __init__(self, bPrint: bool = False, max_timeout: int = 180,
                 mqtt_ip: str | None = None):
        self.bPrint = bPrint
        self.max_timeout = max_timeout

        self.KETI_TASK_MQTT_TOPIC = KETI_TASK_TOPIC
        self.PLAN_RESULT_MQTT_TOPIC = PLAN_RESULT_TOPIC

        mqttConnectIP = mqtt_ip or os.environ.get('MQTT_SERVER_IP')
        if not mqttConnectIP:
            raise ValueError(
                "[MqttClient] MQTT broker IP is required. "
                "Pass mqtt_ip=... or set the MQTT_SERVER_IP env var."
            )
        mqttSubscribeTopics = [(self.PLAN_RESULT_MQTT_TOPIC, 1)]

        self.mqttComm = MqttComm(
            mqttConnectIP,
            mqttSubscribeTopics,
            on_msg_callback=self.on_mqtt_message,
            loop_mode="start",
            bPrint=bPrint,
        )
        self.mqttComm.mqtt_init()
        if not self.mqttComm.wait_connected(timeout=5.0):
            raise RuntimeError(
                f"[MqttClient] could not connect to MQTT broker at "
                f"{mqttConnectIP} within 5s"
            )

    def commWork(self, plan: str, timeout: float) -> dict | None:
        """Send a structured plan and block for the result.

        Returns:
            dict: full result payload, at least ``{action, isdone, ...}``,
                  plus any fields the executing skill returned. On agent
                  failure the dict contains ``isdone=False`` and usually
                  an ``error`` field.
            None: timeout (no response within ``timeout``) or client
                  was stopped.

        Raises:
            RuntimeError: malformed response from the bridge.
        """
        self.mqttComm.sendIt(self.KETI_TASK_MQTT_TOPIC, {'plan': plan})
        plan_result = self.mqttComm.getIt_f(self.PLAN_RESULT_MQTT_TOPIC, timeout)

        if self.mqttComm.is_stopped():
            return None
        if not plan_result:
            return None

        data = plan_result.get('data') or {}
        result = data.get('result')
        if not isinstance(result, dict):
            raise RuntimeError(f"malformed bridge response: {plan_result!r}")

        return result

    def actionWork(self, action: str, target) -> dict | None:
        """Convenience: build a single-skill plan and execute it.

        Returns the same shape as :meth:`commWork`.
        """
        if action == 'init_arm':
            return self.commWork('init_arm::', self.max_timeout)

        elif action == 'move':
            return self.commWork(f'move::{target}', self.max_timeout * 2)

        elif action == 'lift':
            return self.commWork(f'lift::{float(target)}', self.max_timeout)

        elif action == 'grip':
            plan = 'grip::1000' if int(target) else 'grip::0'
            return self.commWork(plan, self.max_timeout)

        raise ValueError(f'unknown action: {action!r}')

    def on_mqtt_message(self, topic: str, tmpDict: dict):
        """Cache incoming responses so commWork's getIt_f can pick them up."""
        contents = tmpDict.get('contents', {})
        dataDict = contents.get('data') if isinstance(contents, dict) else None
        if not isinstance(dataDict, dict):
            return
        self.mqttComm.setMqttResponseData(topic, dataDict)

    def close(self):
        self.mqttComm.flush_and_close(timeout=2.0)


def main():
    p = argparse.ArgumentParser(
        prog="robot-mqtt-client",
        description="MQTT client for the robot_agent bridge",
    )
    p.add_argument("--mqtt-ip",
                   default=os.environ.get("MQTT_SERVER_IP"),
                   help="MQTT broker IP (or ip/user/pass). "
                        "Required: set this flag or MQTT_SERVER_IP env var.")
    p.add_argument("--timeout", type=float, default=180,
                   help="result timeout in seconds (default 180)")
    p.add_argument("--quiet", action="store_true",
                   help="suppress MQTT connect/handler log output")

    sub = p.add_subparsers(dest="cmd", required=True, metavar="CMD")

    sp_plan = sub.add_parser("plan", help="send a raw plan string (multi-line OK)")
    sp_plan.add_argument("plan", help='e.g. "find::apple\\nmove::kitchen"')

    sp_action = sub.add_parser("action", help="send a single-skill action")
    sp_action.add_argument("action", choices=["move", "lift", "grip", "init_arm"])
    sp_action.add_argument("target", nargs="?", default="",
                           help="action target (e.g. 'kitchen', '200', '1')")

    args = p.parse_args()

    if not args.mqtt_ip:
        p.error("--mqtt-ip is required (or set MQTT_SERVER_IP env var)")

    client = MqttClient(
        bPrint=not args.quiet,
        max_timeout=args.timeout,
        mqtt_ip=args.mqtt_ip,
    )
    try:
        if args.cmd == "plan":
            # Decode literal \n into real newlines for CLI convenience.
            plan = args.plan.encode().decode("unicode_escape")
            result = client.commWork(plan, args.timeout)
        else:
            result = client.actionWork(args.action, args.target)

        if result is None:
            print(json.dumps({"isdone": False, "error": "timeout or stopped"}, indent=2))
            return 1

        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result.get("isdone") else 1
    finally:
        client.close()


if __name__ == "__main__":
    sys.exit(main())
