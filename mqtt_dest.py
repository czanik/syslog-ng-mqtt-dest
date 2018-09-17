"""
This is mqtt_dest.py, a sample syslog-ng Python destination saving
logs to MQTT
Use "pip install paho-mqtt" or install the relevant package from your distro
host, port and topic are mandatory parameters
"""

import paho.mqtt.client as mqtt

class LogDestination(object):
    """
    boilerplate code
    """
    def open(self):
        """Open a connection to the target service

        Should return False if opening fails"""
        return True

    def close(self):
        """Close the connection to the target service"""
        pass

    def is_opened(self):
        """Check if the connection to the target is able to receive messages"""
        return True

    def init(self, options):
        """This method is called at initialization time

        Should return false if initialization fails"""
        return True

    def deinit(self):
        """This method is called at deinitialization time"""
        pass

    def send(self, msg):
        """Send a message to the target service

        It should return True to indicate success, False will suspend the
        destination for a period specified by the time-reopen() option."""
        return True

class MqttDestination(LogDestination):
    """
    the MqttDestination class, reference this from syslog-ng.conf
    """
    def __init__(self):
        """
        basic initialization
        """
        self.host = None
        self.port = None
        self.topic = None
        self._is_opened = False
        self.debug = 0
        self.qos = 0
        self.mqttc = mqtt.Client("sng_mqtt")

    def init(self, options):
        """
        initializing MQTT parameters, fails if mandatory parameters
        are not available, or not in the right format
        """
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
            if self.debug > 0:
                print('MQTT destination: opened...')
        except:
            if self.debug > 0:
                print('MQTT destination: opening ' + self.host + ' at ' + str(self.port) + 'failed...')
            return False
        return True

    def close(self):
        """
        close the connection
        """
        self.mqttc.disconnect()
        if self.debug > 0:
            print('MQTT destination: closing connection to ' + self.host + ' at ' + str(self.port))
        self._is_opened = False

    def deinit(self):
        """do nothing :)"""
        pass

    def send(self, msg):
        """
        send the message
        """
        decoded = msg['MESSAGE'].decode('utf-8')
        try:
            if self.debug > 0:
                print('MQTT destination: before sending')
            self.mqttc.publish(self.topic, decoded, qos=self.qos)
            if self.debug > 0:
                print('MQTT destination: after sending')
        except:
            if self.debug > 0:
                print('MQTT destination: sending to topic ' + self.topic + ' failed...')
            return False
        return True

"""
# code to test the MqttDestination outside of syslog-ng
print('starting up...')
bla = MqttDestination()
bla.init(options=dict(host="127.0.0.1",port="1883",topic="syslog/warn"))
bla.open()
bla.send(msg=dict(MESSAGE=b"It's working..."))
bla.send(msg=dict(MESSAGE=b"Still working..."))
bla.close()
bla.deinit()
"""
