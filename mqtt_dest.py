"""
This is mqtt_dest.py, a sample syslog-ng Python destination saving
logs to MQTT
Use "pip install paho-mqtt" or install the relevant package from your distro
host, port and topic are mandatory parameters
"""

import paho.mqtt.client as mqtt


class MqttDestination(object):
    """
    the MqttDestination class, reference this from syslog-ng.conf
    """

    def printdebug(self, msg):
        """prints debug message if debug is enabled"""
        if self.debug > 0:
            print(msg)

    def init(self, options):
        """
        initializing MQTT parameters, fails if mandatory parameters
        are not available, or not in the right format
        """
        self.host = None
        self.port = None
        self.topic = None
        self._is_opened = False
        self.debug = 0
        self.qos = 0
        self.mqttc = mqtt.Client("sng_mqtt")
        try:
            print('MQTT destination options: ' + str(options))
            self.host = options["host"]
            self.port = int(options["port"])
            self.topic = options["topic"]
            if "debug" in options:
                self.debug = int(options["debug"])
            if "qos" in options:
                self.qos = int(options["qos"])
        except:
            print('MQTT destination: exiting in init()...')
            return False
        return True

    def is_opened(self):
        return self._is_opened

    def open(self):
        """
        open connection to the MQTT server and start the loop
        """
        try:
            self.mqttc.connect(self.host, self.port)
            self.mqttc.loop_start()
            self._is_opened = True
            self.printdebug('MQTT destination: opened...')
        except:
            self.printdebug('MQTT destination: opening ' + self.host + ' at ' + str(self.port) + 'failed...')
            self._is_opened = False
            return False
        return True

    def close(self):
        """
        close the connection
        """
        self.mqttc.disconnect()
        self.printdebug('MQTT destination: closing connection to ' + self.host + ' at ' + str(self.port))
        self._is_opened = False

    def deinit(self):
        """do nothing :)"""
        pass

    def send(self, msg):
        """
        send the message
        """
        decoded_msg = msg['MESSAGE'].decode('utf-8')
        try:
            self.printdebug('MQTT destination: before sending')
            self.mqttc.publish(self.topic, decoded_msg, qos=self.qos)
            self.printdebug('MQTT destination: after sending')
        except:
            self.printdebug('MQTT destination: sending to topic ' + self.topic + ' failed...')
            self._is_opened = False            
            return False
        return True

"""
# code to test the MqttDestination outside of syslog-ng
print('starting up...')
bla = MqttDestination()
bla.init(options=dict(host="172.16.167.132",port="1883",topic="syslog/warn",debug="1"))
bla.open()
bla.send(msg=dict(MESSAGE=b"It's working..."))
bla.send(msg=dict(MESSAGE=b"Still working..."))
bla.close()
bla.deinit()
"""
