"""MQTT client for sending structured plans to the robot bridge server.

Publishes plans to `cotap/keti/task/plan` and blocks on
`cotap/common/plan_result` for the response.

Run:
    python client.py                     # demo: move to "table@living room"
"""

from __future__ import annotations

import os
import sys

from server import MqttComm


KETI_TASK_TOPIC = "cotap/keti/task/plan"
PLAN_RESULT_TOPIC = "cotap/common/plan_result"


class MqttClient:

    def __init__(self, bPrint: bool = False, max_timeout: int = 180,
                 mqtt_ip: str | None = None):
        self.bPrint = bPrint
        self.max_timeout = max_timeout

        self.KETI_TASK_MQTT_TOPIC = KETI_TASK_TOPIC
        self.PLAN_RESULT_MQTT_TOPIC = PLAN_RESULT_TOPIC

        mqttConnectIP = mqtt_ip or os.environ.get('MQTT_SERVER_IP', 'localhost')
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
        """Send a structured plan and block for the result."""
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
            plan = 'init_arm::'
            return self.commWork(plan, self.max_timeout)

        elif action == 'move':
            location = target
            plan = f'move::{location}'
            return self.commWork(plan, self.max_timeout * 2)

        elif action == 'lift':
            lift_move_pos = float(target)
            plan = f'lift::{lift_move_pos}'
            return self.commWork(plan, self.max_timeout)

        elif action == 'grip':
            grip_type = int(target)
            plan = 'grip::1000' if grip_type else 'grip::0'
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
    mqttClient = MqttClient(bPrint=True)
    try:
        mqttClient.actionWork("move", "table@living room")
    finally:
        mqttClient.close()


if __name__ == "__main__":
    sys.exit(main())
