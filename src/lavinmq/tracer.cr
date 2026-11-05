require "json"
require "./segment_position"

module LavinMQ
  # Records where each message goes inside a vhost, and why, for the MQSimulator
  # (https://github.com/84codes/MQSimulator) and for debugging.
  #
  # Modes:
  # * `Off`: nothing is recorded. Every hook is one boolean check.
  # * `Flow`: messages flow as usual and every step is recorded as an event.
  # * `Step`: as `Flow`, but a message stops before each step (publish, enqueue,
  #   deliver, ack/reject) until it is told to continue (`#continue`).
  #
  # Messages get a trace id when they enter the vhost. A message in a queue is
  # known by its segment position, so later events (deliver, ack, drop...) can
  # be tied to the id it was published with. A dead-lettered message gets a new
  # id, and the `dead-letter` event links the two.
  class Tracer
    enum Mode
      Off
      Flow
      Step
    end

    # How many events are kept for clients that ask for the log afterwards
    LOG_SIZE = 10_000

    record Event, seq : UInt64, json : String

    # A message that waits to be let through
    class Step
      getter id : UInt64
      getter stage : String
      getter message_id : UInt64
      getter json : String
      getter done = ::Channel(Nil).new(1)

      def initialize(@id, @stage, @message_id, @json)
      end
    end

    getter mode = Mode::Off
    @seq = 0_u64
    @next_message_id = 0_u64
    @next_step_id = 0_u64
    @log = Deque(Event).new
    @subscribers = Array(::Channel(Event)).new
    @steps = Hash(UInt64, Step).new
    @ids = Hash({String, SegmentPosition}, UInt64).new
    @lock = Mutex.new(:reentrant)

    def initialize(@vhost : String)
    end

    def enabled? : Bool
      !@mode.off?
    end

    def stepping? : Bool
      @mode.step?
    end

    def mode=(mode : Mode)
      @lock.synchronize do
        return if mode == @mode
        @mode = mode
        emit("mode", 0_u64, "Tracing mode is now #{mode.to_s.downcase}") do |json|
          json.field "mode", mode.to_s.downcase
        end
        @ids.clear if mode.off?
      end
      continue_all unless mode.step?
    end

    # ---- The log

    def events(since : UInt64 = 0_u64) : Array(Event)
      @lock.synchronize { @log.select { |e| e.seq > since } }
    end

    def clear
      @lock.synchronize { @log.clear }
    end

    def last_seq : UInt64
      @lock.synchronize { @seq }
    end

    def subscribe : ::Channel(Event)
      channel = ::Channel(Event).new(4096)
      @lock.synchronize { @subscribers << channel }
      channel
    end

    def unsubscribe(channel : ::Channel(Event))
      @lock.synchronize { @subscribers.delete(channel) }
      channel.close
    end

    def steps : Array(Step)
      @lock.synchronize { @steps.values }
    end

    # Write an event: its common fields, then what the block adds
    def emit(type : String, message_id : UInt64, text : String, & : JSON::Builder ->) : Nil
      @lock.synchronize do
        seq = @seq += 1
        json = JSON.build do |j|
          j.object do
            j.field "seq", seq
            j.field "t", RoughTime.unix_ms
            j.field "vhost", @vhost
            j.field "type", type
            j.field "message_id", message_id unless message_id.zero?
            yield j
            j.field "text", text
          end
        end
        event = Event.new(seq, json)
        @log.shift if @log.size >= LOG_SIZE
        @log.push event
        @subscribers.each do |s|
          s.try_send?(event) # a subscriber that doesn't keep up loses events, rather than stopping the broker
        end
      end
    end

    # ---- Message ids

    def next_message_id : UInt64
      @lock.synchronize { @next_message_id += 1 }
    end

    # The id of a message in a queue. A message that was there before tracing started gets one now.
    def id_for(queue : String, sp : SegmentPosition) : UInt64
      @lock.synchronize do
        @ids.put_if_absent({queue, sp}) { @next_message_id += 1 }
      end
    end

    def remember(queue : String, sp : SegmentPosition, id : UInt64) : Nil
      @lock.synchronize { @ids[{queue, sp}] = id }
    end

    def forget(queue : String, sp : SegmentPosition) : UInt64
      @lock.synchronize { @ids.delete({queue, sp}) || (@next_message_id += 1) }
    end

    # ---- Events of a message's journey

    def publish(exchange : String, routing_key : String, body : IO, bodysize : UInt64,
                publisher : String?, connection : String?) : UInt64
      id = next_message_id
      x = exchange.empty? ? "the default exchange" : "exchange '#{exchange}'"
      from = publisher ? " by #{publisher}" : ""
      emit("publish", id, "Published#{from} to #{x} with routing key '#{routing_key}'") do |json|
        json.field "exchange", exchange
        json.field "routing_key", routing_key
        json.field "publisher", publisher if publisher
        json.field "connection", connection if connection
        json.field "payload", payload_preview(body, bodysize)
      end
      id
    end

    def route(id : UInt64, from : String, to : String, to_kind : String, via : String) : Nil
      x = from.empty? ? "The default exchange" : "Exchange '#{from}'"
      why = case via
            when "default"   then "routes to the queue named by the routing key"
            when "alternate" then "matched nothing, so it goes to its alternate exchange"
            when "cc"        then "routes by the CC/BCC header"
            else                  "has a binding that matches"
            end
      emit("route", id, "#{x} #{why}: #{to_kind} '#{to}'") do |json|
        json.field "from", from
        json.field "to", to
        json.field "to_kind", to_kind
        json.field "via", via
      end
    end

    def unrouted(id : UInt64, exchange : String) : Nil
      x = exchange.empty? ? "The default exchange" : "Exchange '#{exchange}'"
      emit("unrouted", id, "#{x} has no binding that matches, and no alternate exchange that takes it: the message is not routed") do |json|
        json.field "exchange", exchange
      end
    end

    def enqueue(id : UInt64, queue : String, sp : SegmentPosition) : Nil
      remember(queue, sp, id)
      emit("enqueue", id, "Queue '#{queue}' stores the message") do |json|
        json.field "queue", queue
      end
    end

    def reject_publish(id : UInt64, queue : String) : Nil
      emit("reject-publish", id, "Queue '#{queue}' is full and its overflow is reject-publish: it refuses the message") do |json|
        json.field "queue", queue
      end
    end

    def deliver(queue : String, sp : SegmentPosition, consumer : String, channel : String,
                delivery_tag : UInt64, redelivered : Bool, no_ack : Bool) : Nil
      id = no_ack ? forget(queue, sp) : id_for(queue, sp)
      again = redelivered ? " again" : ""
      acks = no_ack ? " (automatic ack: the queue forgets it now)" : ""
      emit("deliver", id, "Queue '#{queue}' delivers the message#{again} to consumer '#{consumer}'#{acks}") do |json|
        json.field "queue", queue
        json.field "consumer", consumer
        json.field "channel", channel
        json.field "delivery_tag", delivery_tag
        json.field "redelivered", redelivered
        json.field "no_ack", no_ack
      end
    end

    def ack(queue : String, sp : SegmentPosition, consumer : String, delivery_tag : UInt64) : Nil
      id = forget(queue, sp)
      emit("ack", id, "Consumer '#{consumer}' acknowledges the message: queue '#{queue}' deletes it") do |json|
        json.field "queue", queue
        json.field "consumer", consumer
        json.field "delivery_tag", delivery_tag
      end
    end

    def reject(queue : String, sp : SegmentPosition, consumer : String, delivery_tag : UInt64,
               requeue : Bool, nack : Bool) : Nil
      id = id_for(queue, sp)
      verb = nack ? "nacks" : "rejects"
      what = requeue ? "back to the queue" : "out of the queue"
      emit("reject", id, "Consumer '#{consumer}' #{verb} the message, requeue=#{requeue}: it goes #{what}") do |json|
        json.field "queue", queue
        json.field "consumer", consumer
        json.field "delivery_tag", delivery_tag
        json.field "requeue", requeue
        json.field "nack", nack
      end
    end

    def requeue(queue : String, sp : SegmentPosition) : Nil
      id = id_for(queue, sp)
      emit("requeue", id, "Queue '#{queue}' puts the message back, to be delivered again") do |json|
        json.field "queue", queue
      end
    end

    DROP_REASONS = {
      "maxlen"         => "the queue is over its max-length, so the oldest message goes (overflow drop-head)",
      "maxlenbytes"    => "the queue is over its max-length-bytes, so the oldest message goes (overflow drop-head)",
      "expired"        => "its time to live ran out",
      "rejected"       => "a consumer rejected it without requeue",
      "delivery_limit" => "it went over the queue's delivery limit",
    }

    # A message leaves a queue without being acknowledged. Returns its id, which dead-lettering links to.
    def drop(queue : String, sp : SegmentPosition, reason : String, dead_letter_exchange : String?) : UInt64
      id = forget(queue, sp)
      why = DROP_REASONS[reason]? || reason
      dlx = dead_letter_exchange ? ", and is dead-lettered" : ", and is gone (no dead-letter exchange)"
      emit("drop", id, "Queue '#{queue}' drops the message: #{why}#{dlx}") do |json|
        json.field "queue", queue
        json.field "reason", reason
      end
      id
    end

    def dead_letter(id : UInt64, queue : String, exchange : String, routing_key : String, reason : String) : UInt64
      copy = next_message_id
      x = exchange.empty? ? "the default exchange" : "exchange '#{exchange}'"
      emit("dead-letter", id, "Queue '#{queue}' republishes the message to its dead-letter exchange, #{x}, with routing key '#{routing_key}' (x-first-death-reason #{reason}); the copy is message #{copy}") do |json|
        json.field "queue", queue
        json.field "exchange", exchange
        json.field "routing_key", routing_key
        json.field "copy_id", copy
        json.field "reason", reason
      end
      copy
    end

    def confirm(id : UInt64, publisher : String, ack : Bool) : Nil
      text = ack ? "The broker confirms the message to #{publisher} (basic.ack)" : "The broker refuses the message: #{publisher} gets a basic.nack"
      emit("confirm", id, text) do |json|
        json.field "publisher", publisher
        json.field "kind", ack ? "ack" : "nack"
      end
    end

    def returned(id : UInt64, publisher : String, reason : String) : Nil
      emit("return", id, "The message is returned to #{publisher} (#{reason}), because it was published as mandatory and was not routed") do |json|
        json.field "publisher", publisher
        json.field "reason", reason
      end
    end

    # ---- Step mode

    STAGE_TEXT = {
      "publish" => "is about to be routed",
      "enqueue" => "is about to be stored in",
      "deliver" => "is about to be delivered from",
      "ack"     => "is about to be acknowledged in",
      "reject"  => "is about to be rejected in",
    }

    # In step mode, wait until the message is let through. `where` names the
    # exchange or queue, and `cancel` (a closed channel) ends the wait early,
    # by raising `::Channel::ClosedError`.
    def gate(stage : String, id : UInt64, where : String, cancel : ::Channel(Nil)? = nil, &extra : JSON::Builder ->) : Nil
      return unless stepping?
      step = @lock.synchronize do
        step_id = @next_step_id += 1
        json = JSON.build do |j|
          j.object do
            j.field "step_id", step_id
            j.field "stage", stage
            j.field "message_id", id
            j.field "at", where
            extra.call(j)
          end
        end
        s = Step.new(step_id, stage, id, json)
        @steps[step_id] = s
        emit("step", id, "Message #{id} #{STAGE_TEXT[stage]? || stage} '#{where}': waiting for continue") do |j|
          j.field "step_id", step_id
          j.field "stage", stage
          j.field "at", where
          extra.call(j)
        end
        s
      end
      begin
        if c = cancel
          select
          when step.done.receive
          when c.receive
          end
        else
          step.done.receive
        end
      ensure
        @lock.synchronize { @steps.delete(step.id) }
      end
    end

    def gate(stage : String, id : UInt64, where : String, cancel : ::Channel(Nil)? = nil) : Nil
      gate(stage, id, where, cancel) { }
    end

    # Let one waiting message through. Returns false if no step has that id.
    def continue(step_id : UInt64) : Bool
      step = @lock.synchronize { @steps.delete(step_id) } || return false
      release(step)
      true
    end

    # Let every waiting message through. Returns how many there were.
    def continue_all : Int32
      waiting = @lock.synchronize do
        all = @steps.values
        @steps.clear
        all
      end
      waiting.each { |s| release(s) }
      waiting.size
    end

    private def release(step : Step)
      emit("continue", step.message_id, "Message #{step.message_id} continues (#{step.stage})") do |j|
        j.field "step_id", step.id
        j.field "stage", step.stage
      end
      step.done.try_send?(nil)
    end

    PAYLOAD_PREVIEW = 256

    # The first bytes of a body, when it is in memory, without moving the IO
    private def payload_preview(body : IO, bodysize : UInt64) : String?
      return unless mem = body.as?(IO::Memory)
      slice = mem.to_slice + mem.pos
      slice = slice[0, Math.min(slice.size, bodysize.clamp(0, PAYLOAD_PREVIEW).to_i)]
      String.new(slice).scrub
    end
  end
end
