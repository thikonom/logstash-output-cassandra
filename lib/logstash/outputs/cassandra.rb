# encoding: utf-8
require "logstash/outputs/base"
require "logstash/namespace"

class LogStash::Outputs::Cassandra < LogStash::Outputs::Base
  config_name "cassandra"

  # The keyspace to write to
  config :keyspace, :validate => :string, :required => true

  # The column family to write to
  config :table, :validate => :string, :required => true

  # Cluster connection options
  # If no connection optiosn provided, connects to localhost by default
  # (See http://datastax.github.io/ruby-driver/api/#cluster-class_method
  # for the list of options)
  config :connection_options, :validate => :hash

  # A mapping from event fields to column names
  config :event_schema, :validate => :hash

  # Index tables for efficient searching of the data
  # The index table should include only two fields the event_id (by default)
  # and an additional field and they should form a composite key
  # e.g CREATE TABLE search_by_host (id uuid, host text) PRIMARY KEY (id, host)
  config :index_tables, :validate => :hash

  public
  def register
    require "cassandra"

    @index_tables ||= {}
    @connection_options ||= {}

    cluster = Cassandra.cluster(@connection_options)
    @session = cluster.connect(@keyspace)
    @columns = @event_schema.keys
    @columns_cql_clause = @columns.join(',')
    @column_mappings = @columns.map { |c| @event_schema[c] }.map { |c| "%{#{c}}" }
    @column_mappings_placeholder = (["?"]*@columns.size).join(',')

  end # def register

  public
  def receive(event)
    return unless output?(event)

    begin
      event_id = Cassandra::Uuid::Generator.new.uuid
      column_values = @column_mappings.map do |i|
        if i.eql?("%{@timestamp}")
          LogStash::Timestamp.coerce(event.sprintf(i)).time
        else
          event.sprintf(i)
        end
      end
      insert = @session.prepare("INSERT INTO #{@table} (id,#{@columns_cql_clause}) VALUES (?,#{@column_mappings_placeholder})")
      @session.execute(insert, arguments: [event_id, *column_values])
      @index_tables.each do |tbl, id_key|
        timeline_query = "INSERT INTO #{tbl} (id, #{id_key}) VALUES (?, ?)"
        @session.execute(timeline_query, arguments: [event_id, event.sprintf("%{#{id_key}}")])
      end
    end

  end # def event
end # class LogStash::Outputs::Example
