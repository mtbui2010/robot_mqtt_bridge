"""Low-level MQTT pub/sub wrapper.

Thin layer over paho-mqtt that adds:
- ip/user/pass connection-string parsing
- numpy-aware JSON serialization
- envelope wrapping (`{timestamp, contents:{data:...}}`)
- TTL-bounded response cache for request/response patterns (`getIt_f`)
- Last Will support
- Thread-safe response dict
"""

from __future__ import annotations

import json
import threading
import time
from collections import defaultdict, deque
from datetime import datetime, timedelta

import numpy as np
import paho.mqtt.client as mqtt


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
