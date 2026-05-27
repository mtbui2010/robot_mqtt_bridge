"""MQTT client for sending structured plans to the bridge server.

Publishes to `cotap/keti/task/plan` and blocks on
`cotap/common/plan_result` for the response.
"""

from __future__ import annotations

import argparse
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

        mqttConnectIP = mqtt_ip or os.environ.get('MQTT_SERVER_IP', '0.0.0.0')
        mqttSubscribeTopics = [(self.PLAN_RESULT_MQTT_TOPIC, 1)]

        self.mqttComm = MqttComm(
            mqttConnectIP,
            mqttSubscribeTopics,
            on_msg_callback=self.on_mqtt_message,
            loop_mode="start",
            bPrint=bPrint,
        )
        self.mqttComm.mqtt_init()

    def commWork(self, plan: str, timeout: float):
        """Send a structured plan and block for the result.

        Returns True if isdone, False on failure, None if stopped.
        """
        self.mqttComm.sendIt(self.KETI_TASK_MQTT_TOPIC, {'plan': plan})
        plan_result = self.mqttComm.getIt_f(self.PLAN_RESULT_MQTT_TOPIC, timeout)

        if self.mqttComm.is_stopped():
            return None

        if not plan_result:
            print(f'{plan} : No result')
            return False

        data = plan_result.get('data') or {}
        result = data.get('result')
        if not isinstance(result, dict):
            print(f'{plan} : malformed response')
            return False

        action = result.get('action', '?')
        if result.get('isdone'):
            print(f"{action} complete")
            return True

        err = result.get('error', '')
        print(f"{action} failed{f': {err}' if err else ''}")
        return False

    def actionWork(self, action: str, target):
        """Convenience: build a single-skill plan and execute it."""
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
                   default=os.environ.get("MQTT_SERVER_IP", "0.0.0.0"),
                   help="MQTT broker IP (or ip/user/pass)")
    p.add_argument("--timeout", type=float, default=180,
                   help="result timeout in seconds (default 180)")
    p.add_argument("--quiet", action="store_true", help="suppress log output")

    sub = p.add_subparsers(dest="cmd", required=True, metavar="CMD")

    sp_plan = sub.add_parser("plan", help="send a raw plan string (multi-line OK)")
    sp_plan.add_argument("plan", help='e.g. "find::apple\\nmove::kitchen"')

    sp_action = sub.add_parser("action", help="send a single-skill action")
    sp_action.add_argument("action", choices=["move", "lift", "grip", "init_arm"])
    sp_action.add_argument("target", nargs="?", default="",
                           help="action target (e.g. 'kitchen', '200', '1')")

    args = p.parse_args()

    client = MqttClient(
        bPrint=not args.quiet,
        max_timeout=args.timeout,
        mqtt_ip=args.mqtt_ip,
    )
    try:
        if args.cmd == "plan":
            # Decode literal \n into real newlines for CLI convenience.
            plan = args.plan.encode().decode("unicode_escape")
            ok = client.commWork(plan, args.timeout)
        else:
            ok = client.actionWork(args.action, args.target)
        return 0 if ok else 1
    finally:
        client.close()


if __name__ == "__main__":
    sys.exit(main())
