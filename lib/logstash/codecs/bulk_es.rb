# encoding: utf-8
require "logstash/codecs/base"
require "logstash/codecs/line"
require "logstash/json"
require "logstash/namespace"


# Todo add description
class LogStash::Codecs::BulkEs < LogStash::Codecs::Base

  # The codec name
  config_name "bulk_es"

  def register
    @lines = LogStash::Codecs::Line.new
    @lines.charset = "UTF-8"
    @state = :start
    @metadata = Hash.new
  end # def register

  def decode(data)
    @lines.decode(data) do |bulk_message|
      begin
        line = LogStash::Json.load(bulk_message.get("message"))
        case @state
        when :metadata
          if @metadata["action"] == 'update'
             if line.has_key?("doc")
               event = LogStash::Event.new(line["doc"])
             elsif
               event = LogStash::Event.new(line)
             end
          elsif
            event = LogStash::Event.new(line)
          end
          event.set("@metadata", @metadata)
          yield event
          @state = :start
        when :start
          @metadata = line[line.keys[0]]
          @metadata["action"] = line.keys[0].to_s
          @state = :metadata
          if line.keys[0] == 'delete'
            event = LogStash::Event.new()
            event.set("@metadata", @metadata)
            yield event
            @state = :start
          end
        end
      rescue LogStash::Json::ParserError => e
        @logger.error("JSON parse failure. Bulk ES messages must in be UTF-8 JSON", :error => e, :data => data)
      end
    end
  end # def decode

  def encode(data)
    raise "Not implemented"
  end # def encode

end # class LogStash::Codecs::BulkEs
