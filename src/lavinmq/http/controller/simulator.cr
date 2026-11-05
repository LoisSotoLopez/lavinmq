require "../controller"
require "../../tracer"

module LavinMQ
  module HTTP
    # The vhost's Tracer, for the MQSimulator: the event log of where each
    # message went and why, and step mode, where messages wait for a continue.
    #
    # GET    /api/simulator/:vhost                         mode, last event seq, waiting steps
    # PUT    /api/simulator/:vhost                         {"mode": "off" | "flow" | "step"}
    # GET    /api/simulator/:vhost/events?since=N          the events after seq N, as a JSON array
    # DELETE /api/simulator/:vhost/events                  forget the events so far
    # GET    /api/simulator/:vhost/stream                  the events as Server-Sent Events (Last-Event-Id resumes)
    # POST   /api/simulator/:vhost/steps/:id/continue      let one waiting message through
    # POST   /api/simulator/:vhost/continue                let every waiting message through
    class SimulatorController < Controller
      private def register_routes
        get "/api/simulator/:vhost" do |context, params|
          with_tracer(context, params) do |tracer|
            context.response.print %({"mode":"#{tracer.mode.to_s.downcase}","last_seq":#{tracer.last_seq},"steps":[)
            tracer.steps.each_with_index do |step, i|
              context.response.print "," if i > 0
              context.response.print step.json
            end
            context.response.print "]}"
          end
        end

        put "/api/simulator/:vhost" do |context, params|
          with_tracer(context, params, write: true) do |tracer|
            body = parse_body(context)
            mode = body["mode"]?.try(&.as_s?).try { |m| Tracer::Mode.parse?(m) }
            bad_request(context, "Field 'mode' must be off, flow or step") unless mode
            tracer.mode = mode
            context.response.status_code = 204
          end
        end

        get "/api/simulator/:vhost/events" do |context, params|
          with_tracer(context, params) do |tracer|
            since = context.request.query_params["since"]?.try(&.to_u64?) || 0_u64
            context.response.print "["
            tracer.events(since).each_with_index do |event, i|
              context.response.print "," if i > 0
              context.response.print event.json
            end
            context.response.print "]"
          end
        end

        delete "/api/simulator/:vhost/events" do |context, params|
          with_tracer(context, params, write: true) do |tracer|
            tracer.clear
            context.response.status_code = 204
          end
        end

        get "/api/simulator/:vhost/stream" do |context, params|
          with_tracer(context, params) do |tracer|
            since = context.request.headers["Last-Event-Id"]?.try(&.to_u64?) ||
                    context.request.query_params["since"]?.try(&.to_u64?)
            channel = tracer.subscribe
            context.response.content_type = "text/event-stream"
            context.response.headers["Cache-Control"] = "no-store"
            context.response.headers["X-Accel-Buffering"] = "no"
            context.response.upgrade do |io|
              # The log so far, then what happens next. An event may come both ways
              # while subscribing: the seq tells them apart.
              last = 0_u64
              tracer.events(since || 0_u64).each do |event|
                print_event(event, io)
                last = event.seq
              end
              io.print ": connected\n\n"
              io.flush
              while event = channel.receive?
                next if event.seq <= last
                print_event(event, io)
                io.flush
              end
            rescue IO::Error
              # the client went away
            ensure
              tracer.unsubscribe(channel)
            end
          end
        end

        post "/api/simulator/:vhost/steps/:id/continue" do |context, params|
          with_tracer(context, params, write: true) do |tracer|
            id = params["id"].to_u64? || bad_request(context, "Bad step id")
            if tracer.continue(id)
              context.response.status_code = 204
            else
              not_found(context, "No step #{id} is waiting")
            end
          end
        end

        post "/api/simulator/:vhost/continue" do |context, params|
          with_tracer(context, params, write: true) do |tracer|
            {continued: tracer.continue_all}.to_json(context.response)
          end
        end
      end

      private def with_tracer(context, params, write = false, &)
        with_vhost(context, params) do |vhost|
          user = user(context)
          refuse_unless_management(context, user, vhost)
          refuse_unless_vhost_access(context, user, vhost)
          # Step mode holds every message of the vhost: only an administrator may change it
          refuse_unless_administrator(context, user) if write
          yield @amqp_server.vhosts[vhost].tracer
        end
      end

      private def print_event(event : Tracer::Event, io : IO)
        io << "id: " << event.seq << "\n"
        io << "data: " << event.json << "\n\n"
      end
    end
  end
end
