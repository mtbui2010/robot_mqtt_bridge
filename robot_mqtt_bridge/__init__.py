"""MQTT ↔ robot_agent bridge.

Public API::

    from robot_mqtt_bridge import MqttComm, Server, MqttClient

CLI entry points::

    robot-mqtt-server                       # start the bridge
    robot-mqtt-client action move kitchen   # send a one-skill plan
    robot-mqtt-client plan "find::apple\\nmove::kitchen"
"""

from .mqtt_comm import MqttComm
from .server import Server
from .client import MqttClient

__all__ = ["MqttComm", "Server", "MqttClient"]
__version__ = "0.1.0"
