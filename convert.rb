require 'pg'
require 'yaml'

exit_value = 0

puts 'PG Data Converter - PostgreSQL data migration tool (C) 2018 Phitherek_'

puts 'Reading config...'

@config = YAML.load_file('configuration.yml')

puts 'Connecting to source...'

source_connection = PG::Connection.new(host: @config['source']['hostname'], port: @config['source']['port'], dbname: @config['source']['database'], user: @config['source']['username'], password: @config['source']['password'])

puts 'Connecting to destination...'

dest_connection = PG::Connection.new(host: @config['destination']['hostname'], port: @config['destination']['port'], dbname: @config['destination']['database'], user: @config['destination']['username'], password: @config['destination']['password'])

puts 'Migrating data...'

begin
  @config['map'].each do |tablemap|
    begin
      src_table = tablemap['source_table']
      dest_table = tablemap['destination_table']
      puts "SELECT * FROM #{src_table}"
      results = source_connection.exec("SELECT * FROM #{src_table}")
      results.each do |result|
        values = []
        query = "INSERT INTO #{dest_table}("
        field_keys = tablemap['fields'].keys
        field_keys.each do |dest_field|
          values << eval(tablemap['fields'][dest_field])
          if field_keys.last != dest_field
            query += "#{dest_field}, "
          else
            query += "#{dest_field})"
          end
        end
        query += ' VALUES('
        1.upto(values.count).each do |i|
          if i != values.count
            query += "$#{i}, "
          else
            query += "$#{i})"
          end
        end
        puts "Query: (#{query}), values: (#{values})"
        dest_connection.exec_params(query, values)
        puts "Migrated from #{src_table} to #{dest_table}!"
      end
    rescue PG::Error => e
      puts "Encountered PG::Error: #{e.to_s}. Trying next definition..."
      next
    end
  end
rescue => e
  puts "Encountered exception: #{e.class} - #{e}"
  puts e.backtrace
  exit_value = 1
end

puts 'Closing destination connection...'

dest_connection.finish

puts 'Closing source connection...'

source_connection.finish

exit exit_value

