require "json"
require "logstash/namespace"
require "uri"
require "time"
require "logstash/time"

# General event type. 
# Basically a light wrapper on top of a hash.
#
# TODO(sissel): properly handle lazy properties like parsed time formats, urls,
# etc, as necessary.
class LogStash::Event
  public
  def initialize(data=Hash.new)
    if RUBY_ENGINE == "jruby"
      @@date_parser ||= Java::org.joda.time.format.ISODateTimeFormat.dateTimeParser.withOffsetParsed
    else
      # TODO(sissel): LOGSTASH-217
      @@date_parser ||= nil
    end

    @cancelled = false
    @data = {
      "@source" => "unknown",
      "@type" => nil,
      "@tags" => [],
      "@fields" => {},
    }.merge(data)

    if !@data.include?("@timestamp")
      @data["@timestamp"] = LogStash::Time.now.utc.to_iso8601
    end
  end # def initialize

  public
  def self.from_json(json)
    return LogStash::Event.new(JSON.parse(json))
  end # def self.from_json

  public
  def cancel
    @cancelled = true
  end # def cancel

  public
  def cancelled?
    return @cancelled
  end # def cancelled?

  # Create a deep-ish copy of this event.
  public
  def clone
    newdata = @data.clone
    newdata["@fields"] = {}
    fields.each do |k,v|
      newdata["@fields"][k] = v.clone
    end
    return LogStash::Event.new(newdata)
  end # def clone

  public
  def to_s
    return self.sprintf("%{@timestamp} %{@source}: %{@message}")
  end # def to_s

  public
  def timestamp; @data["@timestamp"]; end # def timestamp
  def timestamp=(val); @data["@timestamp"] = val; end # def timestamp=

  public
  def unix_timestamp
    if RUBY_ENGINE != "jruby"
      # This is really slow. See LOGSTASH-217
      return self.ruby_timestamp.to_f
    else
      return self.ruby_timestamp.getMillis.to_f / 1000
    end
  end

  public
  def ruby_timestamp
    if @ruby_timestamp.nil?
      if RUBY_ENGINE != "jruby"
        # This is really slow. See LOGSTASH-217
        @ruby_timestamp = Time.parse(timestamp)
      else
        @ruby_timestamp = @@date_parser.parseDateTime(timestamp)
      end
    end

    @ruby_timestamp
  end

  public
  def source; @data["@source"]; end # def source
  def source=(val) 
    uri = URI.parse(val) rescue nil
    val = uri if uri
    if val.is_a?(URI)
      @data["@source"] = val.to_s
      @data["@source_host"] = val.host
      @data["@source_path"] = val.path
    else
      @data["@source"] = val
      @data["@source_host"] = val
    end
  end # def source=

  public
  def message; @data["@message"]; end # def message
  def message=(val); @data["@message"] = val; end # def message=

  public
  def type; @data["@type"]; end # def type
  def type=(val); @data["@type"] = val; end # def type=

  public
  def tags; @data["@tags"]; end # def tags
  def tags=(val); @data["@tags"] = val; end # def tags=

  # field-related access
  public
  def [](key)
    # If the key isn't in fields and it starts with an "@" sign, get it out of data instead of fields
    if ! @data["@fields"].has_key?(key) and key.slice(0,1) == "@"
      return @data[key]
    # Exists in @fields (returns value) or doesn't start with "@" (return null)
    else
      return @data["@fields"][key]
    end
  end # def []
  
  public
  def []=(key, value)
    if @data.has_key?(key)
      @data[key] = value
    else
      @data["@fields"][key] = value
    end
  end # def []=

  def fields; return @data["@fields"] end # def fields
  
  public
  def to_json(*args); return @data.to_json(*args) end # def to_json
  def to_hash; return @data end # def to_hash

  public
  def overwrite(event)
    @data = event.to_hash
  end

  public
  def include?(key)
    return (@data.include?(key) or @data["@fields"].include?(key))
  end # def include?

  # Append an event to this one.
  public
  def append(event)
    self.message += "\n" + event.message 
    self.tags |= event.tags

    # Append all fields
    event.fields.each do |name, value|
      if self.fields.include?(name)
        if !self.fields[name].is_a?(Array)
          self.fields[name] = [self.fields[name]]
        end
        if value.is_a?(Array)
          self.fields[name] |= value
        else
          self.fields[name] << value
        end
      else
        self.fields[name] = value
      end
    end # event.fields.each
  end # def append

  # Remove a field
  public
  def remove(field)
    if @data.has_key?(field)
      @data.delete(field)
    else
      @data["@fields"].delete(field)
    end
  end # def remove

  # sprintf. This could use a better method name.
  # The idea is to take an event and convert it to a string based on 
  # any format values, delimited by %{foo} where 'foo' is a field or
  # metadata member.
  #
  # For example, if the event has @type == "foo" and @source == "bar"
  # then this string:
  #   "type is %{@type} and source is %{@source}"
  # will return
  #   "type is foo and source is bar"
  #
  # If a %{name} value is an array, then we will join by ','
  # If a %{name} value does not exist, then no substitution occurs.
  #
  # TODO(sissel): It is not clear what the value of a field that 
  # is an array (or hash?) should be. Join by comma? Something else?
  public
  def sprintf(format)
    if format.index("%").nil?
      return format
    end

    return format.gsub(/%\{[^}]+\}/) do |tok|
      # Take the inside of the %{ ... }
      key = tok[2 ... -1]

      if key == "+%s"
        # Got %{+%s}, support for unix epoch time
        if RUBY_ENGINE != "jruby"
          # TODO(sissel): LOGSTASH-217
          raise Exception.new("LogStash::Event#sprintf('+%s') is not " \
                              "supported yet in this version of ruby")
        end
        datetime = @@date_parser.parseDateTime(self.timestamp)
        (datetime.getMillis / 1000).to_i
      elsif key[0,1] == "+"
        # We got a %{+TIMEFORMAT} so use joda to format it.
        if RUBY_ENGINE != "jruby"
          # TODO(sissel): LOGSTASH-217
          raise Exception.new("LogStash::Event#sprintf('+dateformat') is not " \
                              "supported yet in this version of ruby")
        end
        datetime = @@date_parser.parseDateTime(self.timestamp)
        format = key[1 .. -1]
        datetime.toString(format) # return requested time format
      else
        # Use an event field.
        value = nil
        obj = self

        # If the top-level value exists, use that and don't try
        # to "look" into data structures.
        if self[key]
          value = self[key]
        else
          # "." is what ES uses to access structured data, so adopt that
          # idea here, too.  "foo.bar" will access key "bar" under hash "foo".
          key.split('.').each do |segment|
            if obj
              value = obj[segment] rescue nil
              obj = obj[segment] rescue nil
            else
              value = nil
              break
            end
          end # key.split.each
        end # if self[key]

        case value
        when nil
          tok # leave the %{foo} if this field does not exist in this event.
        when Array
          value.join(",") # Join by ',' if value is an array
        when Hash
          value.to_json # Convert hashes to json
        else
          value # otherwise return the value
        end
      end
    end
  end # def sprintf

  public
  def ==(other)
    #puts "#{self.class.name}#==(#{other.inspect})"
    if !other.is_a?(self.class)
      return false
    end

    return other.to_hash == self.to_hash
  end # def ==
end # class LogStash::Event
