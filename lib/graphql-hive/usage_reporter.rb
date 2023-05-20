# frozen_string_literal: true

require 'digest'
require 'graphql-hive/analyzer'
require 'graphql-hive/printer'

module GraphQL
  class Hive < GraphQL::Tracing::PlatformTracing
    # Report usage to Hive API without impacting application performances
    class UsageReporter
      @@instance = nil

      @queue = nil
      @thread = nil
      @operations_buffer = nil
      @client = nil

      def self.instance
        @@instance
      end

      def initialize(options, client)
        logger.debug('UsageReporter.initialize')
        @@instance = self

        @options = options
        @client = client

        @options_mutex = Mutex.new
        @queue = Queue.new

        start_thread
        logger.debug('UsageReporter.initialize done')
      end

      def add_operation(operation)
        logger.debug("UsageReporter.add_operation: #{operation}")
        @queue.push(operation)
        logger.debug('UsageReporter.add_operation done')
      end

      def on_exit
        logger.debug('UsageReporter.on_exit')
        @queue.close
        @thread.join
        logger.debug('UsageReporter.on_exit done')
      end

      def on_start
        logger.debug('UsageReporter.on_start')
        start_thread
        logger.debug('UsageReporter.on_start done')
      end

      private

      def logger
        @options[:logger]
      end

      def start_thread
        logger.debug('UsageReporter.start_thread')
        if @thread&.alive?
          @options[:logger].warn('Tried to start operations flushing thread but it was already alive')
          return
        end

        @thread = Thread.new do
          buffer = []
          logger.debug('UsageReporter.start_thread thread started')
          while (operation = @queue.pop(false))
            @options[:logger].debug("add operation to buffer: #{operation}")
            buffer << operation
            @options_mutex.synchronize do
              logger.debug("buffer size: #{buffer.size}")
              if buffer.size >= @options[:buffer_size]
                @options[:logger].debug('buffer is full, sending!')
                process_operations(buffer)
                buffer = []
              end
            end
          end
          unless buffer.size.zero?
            @options[:logger].debug('shuting down with buffer, sending!')
            process_operations(buffer)
          end
        rescue StandardError => e
          # ensure configured logger receives exception as well in setups where STDERR might not be
          # monitored.
          @options[:logger].error('GraphQL Hive usage collection thread terminating')
          @options[:logger].error(e)

          raise e
        end
        logger.debug('UsageReporter.start_thread done')
      end

      def process_operations(operations)
        logger.debug('UsageReporter.process_operations')
        report = {
          size: 0,
          map: {},
          operations: []
        }

        operations.each do |operation|
          add_operation_to_report(report, operation)
        end

        @options[:logger].debug("sending report: #{report}")

        @client.send('/usage', report, :usage)
      end

      def add_operation_to_report(report, operation)
        timestamp, queries, results, duration = operation

        errors = errors_from_results(results)

        operation_name = queries.map(&:operations).map(&:keys).flatten.compact.join(', ')
        operation = ''
        fields = Set.new

        queries.each do |query|
          analyzer = GraphQL::Hive::Analyzer.new(query)
          visitor = GraphQL::Analysis::AST::Visitor.new(
            query: query,
            analyzers: [analyzer]
          )

          visitor.visit

          fields.merge(analyzer.result)

          operation += "\n" unless operation.empty?
          operation += GraphQL::Hive::Printer.new.print(visitor.result)
        end

        md5 = Digest::MD5.new
        md5.update operation
        operation_map_key = md5.hexdigest

        operation_record = {
          operationMapKey: operation_map_key,
          timestamp: timestamp.to_i,
          execution: {
            ok: errors[:errorsTotal].zero?,
            duration: duration,
            errorsTotal: errors[:errorsTotal],
            errors: errors[:errors]
          }
        }

        if results[0]
          context = results[0].query.context
          operation_record[:metadata] = { client: @options[:client_info].call(context) } if @options[:client_info]
        end

        report[:map][operation_map_key] = {
          fields: fields.to_a,
          operationName: operation_name,
          operation: operation
        }
        report[:operations] << operation_record
        report[:size] += 1
      end

      def errors_from_results(results)
        acc = { errorsTotal: 0, errors: [] }
        results.each do |result|
          errors = result.to_h.fetch('errors', [])
          errors.each do |error|
            acc[:errorsTotal] += 1
            acc[:errors] << { message: error['message'], path: error['path']&.join('.') }
          end
        end
        acc
      end
    end
  end
end
