## How to use cassandra as ouput for logstash

This is an example configuration for the plugin:

    output {
        cassandra {
        connection_options => {
          "hosts" => ['10.0.1.2']
        }
        keyspace => "logstash"
        table => "logs"
        event_schema => [
          "message", "message",
          "created_at", "@timestamp"
        ]
        index_tables => [
          "search_by_host", "host"
        ]
      }
    }
