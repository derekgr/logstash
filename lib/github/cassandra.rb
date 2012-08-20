require 'java'

class Cassandra
  attr_reader :client, :column_families
  attr_writer :schema

  include_package "com.netflix.astyanax"
  include_package "com.netflix.astyanax.impl"
  include_package "com.netflix.astyanax.model"
  include_package "com.netflix.astyanax.serializers"
  java_import Java::com.netflix.astyanax.util.TimeUUIDUtils

  module ConnectionPool
    include_package "com.netflix.astyanax.connectionpool"
    include_package "com.netflix.astyanax.connectionpool.impl"
  end

  def initialize(column_families, schema, client = Cassandra.connect)
    @client = client

    @column_families = { 
      :logs => Cassandra.column_family("logs", TimeUUIDSerializer.get, StringSerializer.get),
      :logs_timeline => Cassandra.column_family("logs_timeline", StringSerializer.get, TimeUUIDSerializer.get),
      :user_timeline => Cassandra.column_family("user_timeline", StringSerializer.get, TimeUUIDSerializer.get),
    }

    @keyspace = @client.getEntity
    @schema = schema
  end

  def write_logs_row(row, index_tables)
    raise "Can't write logs row without schema!" if @schema.nil?

    # TODO: Genericize this. It's dependent on Apache loglines.
    row_key = row[:ts]
    raise "No timestamp for log event! Can't generate a unique UUID." if row_key.nil?

    uuid = TimeUUIDUtils.getUniqueTimeUUIDinMillis

    batch = @keyspace.prepareMutationBatch

    mutation = batch.withRow(@column_families[:logs], uuid)
    @schema.each do |k,v|
      raise "Ill-formed log insertion: no row #{k.to_s} in event (#{row.inspect})!" unless row.include?(k)
      if k.eql?(:ts)
        mutation.putColumn(k.to_s, row_key.getMillis, nil)
      elsif v.eql?('int')
        mutation.putColumn(k.to_s, row[k].first.to_i, nil)
      else
        mutation.putColumn(k.to_s, row[k].first, nil)
      end
    end

    # Is this the right thing to do? I need to generate unique values for timestamp-based UUIDs, but apache logs
    # only have second resolution, and many log entries can come by in a given second. So my initial idea is to
    # add a random millisecond component to the timestamp. Hm.
    millis = row_key.getMillis + rand(1000)
    timestamp_uuid = TimeUUIDUtils.getTimeUUID(millis)

    # Write "reverse indexes".
    index_tables.each do |column_family, key|
      mutation = batch.withRow(column_family, key)
      mutation.putColumn(timestamp_uuid, uuid)
    end

    batch.execute

    uuid
  end

  def get_logs_row(key)
    raise "Can't parse logs row without schema!" if @schema.nil?

    if key.class != java.util.UUID
      key = java.util.UUID.fromString(key.to_s)
    end

    query = @keyspace.prepareQuery(@column_families[:logs]).getKey(key).execute
    result = query.getResult

    record = {}

    @schema.each do |k, v|
      col = result.getColumnByName(k.to_s)
      val = nil

      case v
      when 'int'
        val = col.getIntegerValue
      when 'long'
        val = col.getLongValue
      when 'string'
        val = col.getStringValue
      when 'uuid'
        val = col.getUUIDValue
      when 'timestamp'
        val = col.getDateValue
      end

      record[k] = val
    end

    record
  end

  def self.column_family(name, key_serializer = StringSerializer.get, col_serializer = StringSerializer.get)
    ColumnFamily.new(name, key_serializer, col_serializer)
  end

  def self.connect(cluster = 'Test Cluster', keyspace = 'logtest', hosts = [ '127.0.0.1:9160' ], opts = nil) 
    opts ||= {}

    opts[:config] ||= AstyanaxConfigurationImpl.new.setDiscoveryType(ConnectionPool::NodeDiscoveryType::NONE)
    opts[:connection_pool_config] ||= ConnectionPool::ConnectionPoolConfigurationImpl.new("ConnectionPool").setPort(9160).setMaxConnsPerHost(1)
    opts[:connection_pool_monitor] ||= ConnectionPool::CountingConnectionPoolMonitor.new

    pool = opts[:connection_pool_config]
    pool.setSeeds(hosts.join(','))

    builder = AstyanaxContext::Builder.new.forCluster(cluster).forKeyspace(keyspace).withAstyanaxConfiguration(opts[:config])
    builder.withConnectionPoolConfiguration(opts[:connection_pool_config])
    builder.withConnectionPoolMonitor(opts[:connection_pool_monitor])

    client = builder.buildKeyspace(Java::com.netflix.astyanax.thrift.ThriftFamilyFactory.getInstance)
    client.start

    client
  end
end
