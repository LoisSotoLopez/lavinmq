require "./spec_helper"

# The Tracer records where each message goes, for the MQSimulator, and in step
# mode it holds every message before each step until it is told to continue.
module SimulatorSpec
  def self.events(vhost : LavinMQ::VHost) : Array(JSON::Any)
    vhost.tracer.events.map { |e| JSON.parse(e.json) }
  end

  # The events of one kind, in order
  def self.of(events : Array(JSON::Any), type : String) : Array(JSON::Any)
    events.select { |e| e["type"] == type }
  end

  def self.wait_for_event(vhost, type, count = 1)
    wait_for { of(events(vhost), type).size >= count }
  end

  def self.wait_for_steps(vhost, count = 1) : Array(LavinMQ::Tracer::Step)
    wait_for { vhost.tracer.steps.size >= count }
    vhost.tracer.steps
  end
end

describe LavinMQ::Tracer do
  describe "in flow mode" do
    it "records a message from publish to ack, with one id" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.tracer.mode = LavinMQ::Tracer::Mode::Flow
        with_channel(s) do |ch|
          x = ch.exchange("rides", "direct")
          q = ch.queue("north")
          q.bind(x.name, "ride.north")
          q.subscribe(tag: "taxi", no_ack: false) { |msg| msg.ack }
          x.publish_confirm("a ride", "ride.north")
          SimulatorSpec.wait_for_event(vhost, "ack")
        end
        events = SimulatorSpec.events(vhost)
        types = events.map(&.["type"].as_s).reject("mode")
        # the confirm and the delivery happen in different fibers, so either may come first
        types.reject("confirm").should eq ["publish", "route", "enqueue", "deliver", "ack"]
        types.should contain "confirm"
        events.reject(&.["type"].== "mode").map(&.["message_id"]).uniq!.size.should eq 1

        publish = SimulatorSpec.of(events, "publish").first
        publish["exchange"].should eq "rides"
        publish["routing_key"].should eq "ride.north"
        publish["payload"].should eq "a ride"
        publish["publisher"].as_s.should_not be_empty # the connection name

        route = SimulatorSpec.of(events, "route").first
        route["from"].should eq "rides"
        route["to"].should eq "north"
        route["via"].should eq "binding"

        deliver = SimulatorSpec.of(events, "deliver").first
        deliver["queue"].should eq "north"
        deliver["consumer"].should eq "taxi"
        deliver["text"].as_s.should contain "delivers the message to consumer 'taxi'"
        SimulatorSpec.of(events, "confirm").first["kind"].should eq "ack"
      end
    end

    it "records the default exchange, and an unroutable message that is returned" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.tracer.mode = LavinMQ::Tracer::Mode::Flow
        with_channel(s) do |ch|
          q = ch.queue("tasks")
          q.publish_confirm("by name")
          ch.basic_publish_confirm("lost", "amq.direct", "nowhere", mandatory: true)
        end
        events = SimulatorSpec.events(vhost)
        route = SimulatorSpec.of(events, "route").first
        route["from"].should eq ""
        route["via"].should eq "default"
        SimulatorSpec.of(events, "unrouted").first["exchange"].should eq "amq.direct"
        SimulatorSpec.of(events, "return").first["reason"].should eq "NO_ROUTE"
      end
    end

    it "follows the alternate exchange" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.tracer.mode = LavinMQ::Tracer::Mode::Flow
        with_channel(s) do |ch|
          ch.exchange("unrouted", "fanout")
          ch.queue("leftovers").bind("unrouted", "")
          x = ch.exchange("main", "direct", args: AMQP::Client::Arguments.new({"alternate-exchange" => "unrouted"}))
          x.publish_confirm("x", "no-binding")
        end
        routes = SimulatorSpec.of(SimulatorSpec.events(vhost), "route")
        routes.map { |r| {r["from"].as_s, r["to"].as_s, r["via"].as_s} }.should eq [
          {"main", "unrouted", "alternate"},
          {"unrouted", "leftovers", "binding"},
        ]
      end
    end

    it "links a rejected message to its dead-lettered copy" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.tracer.mode = LavinMQ::Tracer::Mode::Flow
        with_channel(s) do |ch|
          ch.queue("dlq")
          q = ch.queue("work", args: AMQP::Client::Arguments.new({
            "x-dead-letter-exchange" => "", "x-dead-letter-routing-key" => "dlq",
          }))
          q.subscribe(tag: "picky", no_ack: false) { |msg| msg.reject(requeue: false) }
          q.publish_confirm("broken")
          SimulatorSpec.wait_for_event(vhost, "enqueue", 2)
        end
        events = SimulatorSpec.events(vhost)
        reject = SimulatorSpec.of(events, "reject").first
        reject["requeue"].should be_false
        drop = SimulatorSpec.of(events, "drop").first
        drop["reason"].should eq "rejected"
        dl = SimulatorSpec.of(events, "dead-letter").first
        dl["message_id"].should eq drop["message_id"]
        dl["exchange"].should eq ""
        dl["routing_key"].should eq "dlq"
        enqueues = SimulatorSpec.of(events, "enqueue")
        enqueues.last["queue"].should eq "dlq"
        enqueues.last["message_id"].should eq dl["copy_id"]
      end
    end

    it "records requeues and the drops of a full queue" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.tracer.mode = LavinMQ::Tracer::Mode::Flow
        with_channel(s) do |ch|
          q = ch.queue("small", args: AMQP::Client::Arguments.new({"x-max-length" => 1}))
          q.publish_confirm("first")
          q.publish_confirm("second")
          refuse = ch.queue("strict", args: AMQP::Client::Arguments.new({
            "x-max-length" => 1, "x-overflow" => "reject-publish",
          }))
          refuse.publish_confirm("first")
          refuse.publish_confirm("second").should be_false
          msg = q.get(no_ack: false).not_nil!
          msg.reject(requeue: true)
          SimulatorSpec.wait_for_event(vhost, "requeue")
        end
        events = SimulatorSpec.events(vhost)
        SimulatorSpec.of(events, "drop").first["reason"].should eq "maxlen"
        SimulatorSpec.of(events, "reject-publish").first["queue"].should eq "strict"
        SimulatorSpec.of(events, "confirm").last["kind"].should eq "nack"
        SimulatorSpec.of(events, "deliver").first["consumer"].should eq "basic.get"
        SimulatorSpec.of(events, "requeue").first["queue"].should eq "small"
      end
    end
  end

  it "records nothing when it is off" do
    with_amqp_server do |s|
      vhost = s.vhosts["/"]
      with_channel(s) do |ch|
        ch.queue("q").publish_confirm("m")
      end
      vhost.tracer.events.should be_empty
    end
  end

  describe "in step mode" do
    it "holds a message before each step until it is told to continue" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        tracer = vhost.tracer
        tracer.mode = LavinMQ::Tracer::Mode::Step
        with_channel(s) do |ch|
          q = ch.queue("steps")
          acked = ::Channel(Nil).new(1)
          q.subscribe(tag: "slow", no_ack: false) do |msg|
            msg.ack
            acked.send nil
          end
          spawn { ch.basic_publish("m", "", "steps") }

          {"publish", "enqueue", "deliver", "ack"}.each do |stage|
            steps = SimulatorSpec.wait_for_steps(vhost)
            steps.size.should eq 1
            steps.first.stage.should eq stage
            tracer.continue(steps.first.id).should be_true
          end
          acked.receive
          SimulatorSpec.wait_for_event(vhost, "ack")
          q.message_count.should eq 0
        end
        events = SimulatorSpec.events(vhost)
        SimulatorSpec.of(events, "step").map(&.["stage"].as_s).should eq ["publish", "enqueue", "deliver", "ack"]
        SimulatorSpec.of(events, "continue").size.should eq 4
      end
    end

    it "lets every message through when the mode changes" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        vhost.tracer.mode = LavinMQ::Tracer::Mode::Step
        with_channel(s) do |ch|
          q = ch.queue("q")
          spawn { ch.basic_publish("m", "", "q") }
          SimulatorSpec.wait_for_steps(vhost)
          vhost.tracer.mode = LavinMQ::Tracer::Mode::Flow
          should_eventually(eq 1) { q.message_count }
          vhost.tracer.steps.should be_empty
        end
      end
    end

    it "puts a message back when its consumer goes away while it waits" do
      with_amqp_server do |s|
        vhost = s.vhosts["/"]
        with_channel(s) do |ch|
          q = ch.queue("q")
          q.publish_confirm("m")
          vhost.tracer.mode = LavinMQ::Tracer::Mode::Step
          tag = q.subscribe(tag: "leaving", no_ack: false) { |_| }
          SimulatorSpec.wait_for_steps(vhost).first.stage.should eq "deliver"
          q.unsubscribe(tag)
          should_eventually(be_true) { vhost.tracer.steps.empty? }
          vhost.tracer.mode = LavinMQ::Tracer::Mode::Off
          should_eventually(eq 1) { q.message_count }
        end
      end
    end
  end
end

describe LavinMQ::HTTP::SimulatorController do
  it "changes the mode and gives the events" do
    with_http_server do |http, s|
      response = http.put("/api/simulator/%2f", body: %({"mode":"flow"}))
      response.status_code.should eq 204
      response = http.put("/api/simulator/%2f", body: %({"mode":"sideways"}))
      response.status_code.should eq 400
      with_channel(s) do |ch|
        ch.queue("q").publish_confirm("m")
      end
      response = http.get("/api/simulator/%2f")
      response.status_code.should eq 200
      JSON.parse(response.body)["mode"].should eq "flow"
      response = http.get("/api/simulator/%2f/events")
      events = JSON.parse(response.body).as_a
      events.map(&.["type"].as_s).should eq ["mode", "publish", "route", "enqueue", "confirm"]
      since = events[1]["seq"].as_i
      response = http.get("/api/simulator/%2f/events?since=#{since}")
      JSON.parse(response.body).as_a.size.should eq 3
      http.delete("/api/simulator/%2f/events").status_code.should eq 204
      JSON.parse(http.get("/api/simulator/%2f/events").body).as_a.should be_empty
    end
  end

  it "lists the waiting steps and continues them" do
    with_http_server do |http, s|
      http.put("/api/simulator/%2f", body: %({"mode":"step"})).status_code.should eq 204
      with_channel(s) do |ch|
        q = ch.queue("q")
        spawn { ch.basic_publish("m", "", "q") }
        SimulatorSpec.wait_for_steps(s.vhosts["/"])
        steps = JSON.parse(http.get("/api/simulator/%2f").body)["steps"].as_a
        steps.size.should eq 1
        steps.first["stage"].should eq "publish"
        id = steps.first["step_id"]
        http.post("/api/simulator/%2f/steps/#{id}/continue").status_code.should eq 204
        http.post("/api/simulator/%2f/steps/#{id}/continue").status_code.should eq 404
        SimulatorSpec.wait_for_steps(s.vhosts["/"]).first.stage.should eq "enqueue"
        response = http.post("/api/simulator/%2f/continue")
        JSON.parse(response.body)["continued"].should eq 1
        should_eventually(eq 1) { q.message_count }
      end
      http.put("/api/simulator/%2f", body: %({"mode":"off"}))
    end
  end

  it "streams the events" do
    with_http_server do |http, s|
      http.put("/api/simulator/%2f", body: %({"mode":"flow"}))
      client = HTTP::Client.new(URI.parse(http.test_uri("/")))
      client.get("/api/simulator/%2f/stream", headers: http.test_headers) do |response|
        response.status_code.should eq 200
        response.headers["Content-Type"].should eq "text/event-stream"
        io = response.body_io
        io.gets.should eq "id: 1"
        io.gets.not_nil!.should contain %("type":"mode")
        io.gets.should eq ""
        io.gets.should eq ": connected"
        io.gets.should eq ""
        with_channel(s) do |ch|
          ch.queue("q").publish_confirm("m")
        end
        io.gets.should eq "id: 2"
        io.gets.not_nil!.should contain %("type":"publish")
      end
    end
  end

  it "refuses to change the mode for a user who isn't an administrator" do
    with_http_server do |http, s|
      s.users.create("arnold", "pw", [LavinMQ::Tag::Management])
      s.users.add_permission("arnold", "/", /.*/, /.*/, /.*/)
      hdrs = ::HTTP::Headers{"Authorization" => "Basic YXJub2xkOnB3"}
      http.get("/api/simulator/%2f", headers: hdrs).status_code.should eq 200
      http.put("/api/simulator/%2f", headers: hdrs, body: %({"mode":"step"})).status_code.should eq 403
    end
  end
end
