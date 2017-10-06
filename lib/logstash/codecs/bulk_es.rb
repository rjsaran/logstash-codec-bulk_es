# encoding: utf-8
require "logstash/codecs/base"
require "logstash/codecs/line"
require "logstash/json"
require "logstash/namespace"


# This codec decodes the incoming meesage into http://www.elasticsearch.org/guide/en/elasticsearch/reference/current/docs-bulk.html[Elasticsearch bulk format]
# into individual events, and metadata into the `@metadata` field.
#
# Encoding is not supported.
class LogStash::Codecs::BulkEs < LogStash::Codecs::Base

  # The codec name
  config_name "bulk_es"

  private
  def log_failure(message, opts)
    @logger.error("[BulkES Json Parse Failure] #{message}", opts)
  end

  public
  def register
    @lines = LogStash::Codecs::Line.new
    @lines.charset = "UTF-8"
    @state = :init
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
          @state = :init
        when :init
          @metadata = line[line.keys[0]]
          @metadata["action"] = line.keys[0].to_s
          @state = :metadata
          if @metadata["action"] == 'delete'
            event = LogStash::Event.new()
            event.set("@metadata", @metadata)
            yield event
            @state = :init
          end
        end

      rescue LogStash::Json::ParserError => e
        log_failure(
          "messages must in be UTF-8 JSON format",
          :error => e,
          :data => data
        )
      end
    end
  end # def decode

  def encode(data)
    raise "Not implemented"
  end # def encode

end # class LogStash::Codecs::BulkEs
