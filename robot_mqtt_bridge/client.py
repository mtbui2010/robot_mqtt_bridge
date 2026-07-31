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

    def __init__(self, bPrint: bool = False, max_timeout: int = 600,
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


def _run_loop(run_once, ntimes: int, keep_on_error: bool) -> int:
    """Call ``run_once()`` up to ``ntimes`` times.

    ``run_once`` takes no args and returns a result dict (or ``None`` on
    timeout/stop), matching ``MqttClient.commWork``/``actionWork``.

    With ``ntimes == 1`` this prints a single plain result JSON, unchanged
    from the pre-loop behavior (keeps `| jq` pipelines working). For
    ``ntimes > 1`` it streams a "--- run i/N ---" header + result per
    iteration and a summary line at the end, stopping after the first
    failure unless ``keep_on_error`` is set. Returns 0 only if every
    attempted run had ``isdone=True``.
    """
    if ntimes == 1:
        result = run_once()
        if result is None:
            result = {"isdone": False, "error": "timeout or stopped"}
        print(json.dumps(result, indent=2, ensure_ascii=False))
        return 0 if result.get("isdone") else 1

    n_success = 0
    n_fail = 0
    for i in range(1, ntimes + 1):
        print(f"--- run {i}/{ntimes} ---")
        result = run_once()
        if result is None:
            result = {"isdone": False, "error": "timeout or stopped"}
        print(json.dumps(result, indent=2, ensure_ascii=False))

        if result.get("isdone"):
            n_success += 1
        else:
            n_fail += 1
            if not keep_on_error:
                break

    print(f"--- summary: {n_success} succeeded, {n_fail} failed, "
          f"{n_success + n_fail}/{ntimes} run ---")
    return 0 if n_fail == 0 else 1


def main():
    p = argparse.ArgumentParser(
        prog="robot-mqtt-client",
        description="MQTT client for the robot_agent bridge",
    )
    p.add_argument("--mqtt-ip",
                   default=os.environ.get("MQTT_SERVER_IP"),
                   help="MQTT broker IP (or ip/user/pass). "
                        "Required: set this flag or MQTT_SERVER_IP env var.")
    p.add_argument("--timeout", type=float, default=600,
                   help="result timeout in seconds (default 600); for multi-step "
                        "plans (plan/plan-file), this bounds the whole sequence, "
                        "not each individual step")
    p.add_argument("--quiet", action="store_true",
                   help="suppress MQTT connect/handler log output")
    p.add_argument("--ntimes", type=int, default=1,
                   help="number of times to run the command (default 1); "
                        "applies to plan, plan-file, and action")
    p.add_argument("--keep-on-error", action="store_true",
                   help="with --ntimes > 1, keep looping after a failed run "
                        "instead of stopping (default: stop on first error)")

    sub = p.add_subparsers(dest="cmd", required=True, metavar="CMD")

    sp_plan = sub.add_parser("plan", help="send a raw plan string (multi-line OK)")
    sp_plan.add_argument("plan", help='e.g. "find::apple\\nmove::kitchen"')

    sp_plan_file = sub.add_parser("plan-file", help="send a plan read from a file")
    sp_plan_file.add_argument("file_path", help="path to a file containing the plan text")

    sp_action = sub.add_parser("action", help="send a single-skill action")
    sp_action.add_argument("action", choices=["move", "lift", "grip", "init_arm"])
    sp_action.add_argument("target", nargs="?", default="",
                           help="action target (e.g. 'kitchen', '200', '1')")

    args = p.parse_args()

    if not args.mqtt_ip:
        p.error("--mqtt-ip is required (or set MQTT_SERVER_IP env var)")
    if args.ntimes < 1:
        p.error("--ntimes must be >= 1")

    client = MqttClient(
        bPrint=not args.quiet,
        max_timeout=args.timeout,
        mqtt_ip=args.mqtt_ip,
    )
    try:
        if args.cmd == "plan-file":
            with open(args.file_path, "r", encoding="utf-8") as f:
                plan = f.read()
            run_once = lambda: client.commWork(plan, args.timeout)
        elif args.cmd == "plan":
            # Decode literal \n into real newlines for CLI convenience.
            plan = args.plan.encode().decode("unicode_escape")
            run_once = lambda: client.commWork(plan, args.timeout)
        else:
            run_once = lambda: client.actionWork(args.action, args.target)

        return _run_loop(run_once, args.ntimes, args.keep_on_error)
    finally:
        client.close()


if __name__ == "__main__":
    sys.exit(main())
