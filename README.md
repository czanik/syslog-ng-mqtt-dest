# syslog-ng-mqtt-dest
a sample python destination for syslog-ng

Read https://www.syslog-ng.com/community/b/blog/posts/python-destination-getting-into-details to learn more about the Python destination of syslog-ng.

## sample config

```
destination d_python_to_file {
    python(
        class("mqtt_dest.MqttDestination")
        on-error("fallback-to-string")
        options(
          host 127.0.0.1
          port 1883
          topic "syslog/warn"
          debug 0
          qos 2
        )
    );
};

log {
    source(s_sys);
    destination(d_python_to_file);
};
```
