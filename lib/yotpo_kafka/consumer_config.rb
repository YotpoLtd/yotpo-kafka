module YotpoKafka
  class ConsumerConfig
    def self.configure(params)
      Phobos.configure(
          consumer: { offset_commit_threshold: 10,
                      enable_auto_commit: true,
                      consumer_auto_commit_interval: params[:consumer_auto_commit_interval] || 10000},
          backoff: { min_ms: 1000, max_ms: 60000 },
          logger: { ruby_kafka: { level: :info }},
          kafka: { client_id: get_unique_client_id(params[:handler].to_s, params[:group_id]),
                   seed_brokers: params[:kafka_broker_url].split(',') },
          listeners: get_listeners(params),
          producer: {}
          )
    end

    def self.get_listeners(params)
      listeners = []
      listeners_arr = get_listeners_for_retires(params[:topic], params[:group_id])
      listeners_arr.each do |listener|
        listeners <<
            {
                handler: params[:handler].to_s,
                topic: listener[:topic],
                group_id: listener[:group_id],
                start_from_beginning: true,
                max_wait_time: 5,
                delivery: :message,
            }
      end
      return listeners
    end

    def self.get_listeners_for_retires(topic, group_id)
      listeners = [{topic: topic, group_id: group_id},
                   {topic: "#{topic}_#{group_id}_failures", group_id: "#{group_id}_fail"}]
      return listeners
    end

    def self.get_unique_client_id(handler, group_id)
      return "#{handler}_#{group_id}"
    end
  end
end