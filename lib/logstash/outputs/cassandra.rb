require "logstash/outputs/base"
require "logstash/namespace"

# Cassandra is a distributed, fault-tolerant key-value store originally created by Facebook, now open source via the Apache group.
# Cassandra is based on Dynamo, but incorporates some BigTable-like ideas, including wide columns and supercolumns.
# Cassandra supports secondary indexes on column values, but the "traditional" way to store timeseries data is by having a
# column-family with wide columns supporting the index, and a separate table holding the events proper.
class LogStash::Outputs::Cassandra < LogStash::Outputs::Base
  config_name "cassandra"
  plugin_status "experimental"

  config :cluster, :validate => :string, :default => "logstash"

  # A list of nodes and ports in the cluster for our driver to connect to. 
  config :nodes, :validate => :hash, :default => {"localhost" => "9160"}

  # The keyspace where our event data will live.
  config :keyspace, :validate => :string, :default => "logstash"

  # The columnfamily name to write full event records to. The keys will be UUIDs, so if you wish to set key validators or comparators, use UUID.
  config :table, :validate => :string, :default => "logstash"

  # Cassandra rows can have named schema and column types, so you can specify a custom mapping from event fields to Cassandra columns if you want.
  # If not, the @message field goes into a "message" column.
  config :event_schema, :validate => :hash, :default => ["@message", "message"]

  # The columnfamily name(s) to maintain the time-series index in, and the template to use for row keys. 
  # You can specify multiple tables if you want to "index" your events under multiple keys. 
  # The row keys will be run through event.sprintf, so you can bucket the rows by an event attribute.
  #
  # The colum families you list should have a string key, and a UUID comparator for the wide columns:
  #   CREATE COLUMNFAMILY logstash_timeline (id text primary key) WITH comparator=uuid;
  config :index_tables, :validate => :hash

  config :astyanax, :validate => :string

  public
  def register
    require 'java'
    require @astyanax
    require 'logstash/util/cassandra'

    cluster_nodes = @nodes.map { |node, port| "#{node}:#{port}" }

    @logger.info("Cluster nodes", :nodes => cluster_nodes)
    @column_families = {
      @table.to_sym => Cassandra.column_family(@table, Cassandra::TimeUUIDSerializer.get, Cassandra::StringSerializer.get),
    }

    @index_table_keys = {}

    @index_tables ||= {}
    @index_tables.each do |table_name, key_template|
      cf = Cassandra.column_family(table_name, Cassandra::StringSerializer.get, Cassandra::TimeUUIDSerializer.get)
      @column_families[table_name.to_sym] = cf
      @index_table_keys[cf] = key_template
    end

    @schema = {}
    @event_schema.each do |target_col, source_col|
      key, type = target_col.split(':')
      @schema[key.to_sym] = type
    end

    @column_mappings = {}
    @event_schema.each do |target_col, source_col|
      key, type = target_col.split(':')
      @column_mappings[key.to_sym] = source_col
    end

    @client = Cassandra.new(@column_families, @schema, Cassandra.connect(@cluster_name, @keyspace, cluster_nodes))
  end # def register

  public
  def receive(event)
    return unless output?(event)

    index_keys = {}
    @index_table_keys.each do |cf, tmpl|
      index_keys[cf] = event.sprintf(tmpl)
    end

    row = {}
    @column_mappings.each do |target, src|
      if target.eql?(:ts)
        row[target] = event.parsed_timestamp
      else
        row[target] = event[src]
      end
    end

    @client.write_logs_row(row, index_keys)
  end # def receive
end # class LogStash::Outputs::Cassandra
