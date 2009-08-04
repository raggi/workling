require 'eventmachine'
require 'workling/remote/invokers/base'

#
#  Subscribes the workers to the correct queues. 
# 
module Workling
  module Remote
    module Invokers
      class EventmachineSubscriber < Workling::Remote::Invokers::Base

        def initialize(routing, client_class)
          super
        end

        #
        #  Starts EM loop and sets up subscription callbacks for workers. 
        #
        def listen
          setup = lambda {
            connect do
              routes.each do |route|
                @client.subscribe(route) do |args|
                  run(route, args)
                end
              end
            end
          }
          if EM.reactor_running?
            setup.call
            if EM.respond_to?(:reactor_thread)
              # EM 0.12.9+
              EM.reactor_thread.join
            else
              # EM 0.12.8
              EM.instance_variable_get(:@reactor_thread).join
            end
          else
            EM.run &setup
          end
        end

        def stop
          EM.stop if EM.reactor_running?
        end
      end
    end
  end
end