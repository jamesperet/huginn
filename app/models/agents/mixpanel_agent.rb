require 'mixpanel_client'
require 'date'

module Agents
  class MixpanelAgent < Agent
    include WebRequestConcern

    cannot_receive_events!
    can_dry_run!
    default_schedule "every_1d"

    DEFAULT_EVENTS_ORDER = [['{{date_published}}', 'time'], ['{{last_updated}}', 'time']]

    description do
      <<-MD
        The Mixpanel Agent checks for analytics data and returns an event.

        # Ordering Events

        #{description_events_order}

        In this Agent, the default value for `events_order` is `#{DEFAULT_EVENTS_ORDER.to_json}`.
      MD
    end

    def default_options
      {
        'event_name' => "Page Visit",
        'property' => "Page",
        'value' => "home",
        'time' => 24,
        'interval' => 'hour'
      }
    end

    event_description <<-MD
      Events look like:

          {
            "count": "45",
            'event_name': "Page Visit",
            'property': "Page",
            'value': "home",
            'time': 24,
            'interval': 'hour'
          }

    MD

    def working?

    end

    def validate_options
      errors.add(:base, "event_name is required") unless options['event_name'].present?

      unless options['property'].present? && options['value'].present?
        errors.add(:base, "Please provide 'property' and 'value'")
      end

      validate_web_request_options!
      validate_events_order
    end

    def events_order
      super.presence || DEFAULT_EVENTS_ORDER
    end

    def check
      create_event :payload => {
        count: mixpanel_event_number(options),
        event_name: options['event_name'],
        property: options['property'],
        value: options['value'],
        time: options['time'],
        interval: options['interval']
      }
    end

    protected

    def mixpanel_config
        {
          api_key: ENV['MIXPANEL_API_KEY'],
          api_secret: ENV['MIXPANEL_SECRET_KEY']
        }
      end

    def mixpanel_client
      @mixpanel_client ||= Mixpanel::Client.new(mixpanel_config())
    end

    def mixpanel_event_number(options)
      property, value = options[:property], options[:value]

      unless (property && value) || (!property && !value)
        raise "Must specify both 'property' and 'value' or none"
      end

      if [TrueClass, FalseClass].include?(value.class)
        raise "As of Aug 7, 2013, MixPanel has a bug with querying boolean values\nPlease use number_for_event_using_export until that's fixed"
      end

      event_name = options[:event_name]

      unless event_name
        raise "Event name must be provided"
      end

      type = options[:type] || "general" #MixPanel API uses the term 'general' to mean 'total'

      unless ["unique", "general", "average"].include? type
        raise "Invalid type #{type}"
      end

      num_days = options[:time] || 24
      interval = options[:interval] || "hour"

      mixpanel_options = {
        type: type,
        unit: interval,
        interval: num_days,
        limit: 5,
      }

      if property && value
        mixpanel_endpoint = "events/properties/"
        mixpanel_options.merge!({
          event: event_name,
          values: [value],
          name: property
        })
      else
        mixpanel_endpoint = "events/"
        mixpanel_options.merge!({
          event: [event_name]
        })
      end

      data = mixpanel_client.request(mixpanel_endpoint, mixpanel_options)

      total_for_events(data)
    end

    def total_for_events(data)
      counts_per_property = data["data"]["values"].collect do |c, values|
        values.collect { |k, v| v }.inject(:+)
      end

      #now, calculate grand total
      counts_per_property.inject(:+)
    end

    ###########################

    def number_for_event_using_export(event_name, property, value, num_days = 30)

      # TODO:
      # MixPanel doesn't understand boolean values for properties
      # There is an open ticket, but for now, there is a work around to use export API
      # https://mixpanel.com/docs/api-documentation/exporting-raw-data-you-inserted-into-mixpanel
      to_date = Date.today
      from_date = to_date - num_days

      data = mixpanel_client.request('export', {
        event: [event_name],
        from_date: from_date.to_s,
        to_date: to_date.to_s,
        where: "boolean(properties[\"#{property}\"]) == #{value} ",
      })

      data.count
    end

  end
end
