require 'csv'
require 'json'
require 'base64'

def call(input)
  input_data = input['json_string']

  # Guard: missing/blank input
  return { 'csv_string' => '', 'csv_binary' => '' } if input_data.nil? || input_data.to_s.strip.empty?

  # 1) Parse JSON into Ruby objects
  begin
    data = JSON.parse(input_data)
  rescue JSON::ParserError => e
    raise "Invalid JSON in `json_string`: #{e.message}"
  end

  # Normalize to array of records
  data = [data] if data.is_a?(Hash)
  return { 'csv_string' => '', 'csv_binary' => '' } if !data.is_a?(Array) || data.empty?

  # 2) Flatten each record
  flat_data = data.map do |record|
    record = {} unless record.is_a?(Hash)
    flatten_value(record)
  end

  # 3) Extract headers (stable order: first-seen wins)
  headers = flat_data.reduce([]) { |acc, row| acc | row.keys }

  # 4) Generate CSV
  csv_output = CSV.generate do |csv|
    csv << headers
    flat_data.each do |row|
      csv << headers.map { |h| normalize_cell(row[h]) }
    end
  end

  # 5) Base64 encode for Workato "binary"
  binary_content = Base64.strict_encode64(csv_output)

  {
    'csv_string' => csv_output,
    'csv_binary' => binary_content,

    # Optional: Workato-friendly file object (often easier to use downstream)
    'csv_file' => {
      'content' => binary_content,
      'content_type' => 'text/csv',
      'original_filename' => 'export.csv'
    }
  }
end

# Flattens hashes + arrays into a single-level hash with dotted keys.
# Examples:
#  {a:{b:1}} => {"a.b"=>1}
#  {items:[{id:1},{id:2}]} => {"items[0].id"=>1, "items[1].id"=>2}
def flatten_value(value, parent_key = nil, out = {})
  case value
  when Hash
    value.each do |k, v|
      key = parent_key ? "#{parent_key}.#{k}" : k.to_s
      flatten_value(v, key, out)
    end
  when Array
    value.each_with_index do |v, i|
      key = parent_key ? "#{parent_key}[#{i}]" : "[#{i}]"
      flatten_value(v, key, out)
    end
  else
    out[parent_key.to_s] = value
  end
  out
end

def normalize_cell(v)
  case v
  when NilClass
    nil
  when Hash, Array
    JSON.generate(v) # keep structure but CSV-safe
  else
    v
  end
end
