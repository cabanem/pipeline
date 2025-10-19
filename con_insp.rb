#!/usr/bin/env ruby
# frozen_string_literal: true

# === Workato Connector Inspector ============================================
#
# Parses a Workato-style Ruby connector, builds an IR + call graph, flags issues,
# and emits JSON/DOT/Markdown/NDJSON outputs — without executing connector code.
#
# Usage example:
#   ruby workato_connector_inspect.rb path/to/connector.rb \
#     --emit json,md,dot,ndjson \
#     --outdir ./out \
#     --pretty \
#     --max-warnings 1000 \
#     --graph-name MyConnector
#
# ============================================================================

# ----------------------------- Dependencies ---------------------------------

require 'optparse'
require 'json'
require 'set'
require 'ripper'
require 'strscan'
require 'parser'
require 'digest'

begin
  require 'parser/current'
rescue LoadError
  $stderr.puts "Missing dependency 'parser'. Install with: gem install parser"
  exit 1
end

# ----------------------------- Utilities -------------------------------------

module Util
  module_function

  def deep_freeze(obj)
    case obj
    when Hash
      obj.each_value { |v| deep_freeze(v) }
      obj.freeze
    when Array
      obj.each { |v| deep_freeze(v) }
      obj.freeze
    else
      obj.freeze
    end
  end

  def sym_or_str(node)
    return unless node.is_a?(Parser::AST::Node)
    case node.type
    when :sym then node.children[0].to_s
    when :str then node.children[0].to_s
    else
      nil
    end
  end

  def const_name(node)
    return unless node.is_a?(Parser::AST::Node)
    case node.type
    when :const
      parent = const_name(node.children[0])
      name = node.children[1].to_s
      parent ? "#{parent}::#{name}" : name
    else
      nil
    end
  end

  def node_loc(node)
    return {} unless node && node.location && node.location.expression
    loc = node.location.expression
    {
      line:   loc.line,
      column: loc.column,
      length: loc.size,
      begin:  loc.begin_pos,
      end:    loc.end_pos
    }
  end

  def read_source(path)
    content = File.read(path, mode: 'r:UTF-8')
    [content, content.each_line.count]
  end

  def slice_source(source, loc, max_bytes: 16_384) # non-destructive; safe cap
    return nil unless source && loc && loc[:begin] && loc[:end]
    start = loc[:begin]
    stop  = [loc[:end], start + max_bytes].min
    source.byteslice(start...stop)
  end

  def safe_filename(base, ext)
    base = base.gsub(/[^\w\-.]+/, '_')
    "#{base}#{ext}"
  end

  def label(sym)
    sym.to_s.gsub(/[^\w\-\.\:]+/, '_')
  end
end

# ---------------------------- Domain constants -------------------------------

module Workato
  ROOT_KEYS = %w[
    title version connection test actions triggers object_definitions pick_lists methods
    secure_tunnel webhook_keys streams custom_action custom_action_help
  ].freeze

  HTTP_VERBS = %i[get post put patch delete options head].freeze

  # Required keys (per docs) for action/trigger blocks
  ACTION_REQUIRED = %w[input_fields execute output_fields].freeze
  TRIGGER_REQUIRED = %w[input_fields output_fields dedup].freeze
end

# ------------------------------ IR types -------------------------------------

module IR
  # All IR nodes expose: kind, name, loc, meta (Hash), children (Array)
  class Node
    attr_reader :id, :kind, :name, :loc, :meta, :children

    def initialize(kind:, name:, loc:, meta: {}, children: [], id: nil)
      @kind, @name, @loc = kind, name, loc || {}
      @meta, @children   = meta.freeze, children.freeze
      @id                = id || IR::Id.make(kind: @kind, name: @name, loc: @loc)
      freeze
    end

    def with_children(new_children)
      Node.new(kind: @kind, name: @name, loc: @loc, meta: @meta, children: new_children, id: @id)
    end

    def to_h
      { id: id, kind: kind, name: name, loc: loc, meta: meta, children: children.map(&:to_h) }
    end
  end

  class Issue
    attr_reader :severity, :code, :message, :loc, :context
    def initialize(severity:, code:, message:, loc: {}, context: {})
      @severity, @code, @message, @loc, @context = severity, code, message, loc || {}, context || {}
      freeze
    end

    def to_h
      { severity: severity, code: code, message: message, loc: loc, context: context }
    end
  end

  class Graph
    attr_reader :directed, :nodes, :edges
    def initialize(directed: true)
      @directed = directed
      @nodes = {} # id => {label:, kind:}
      @edges = Set.new # [from, to, meta]
    end

    def add_node(id, label:, kind:)
      @nodes[id] ||= { label: label, kind: kind }
    end

    def add_edge(from, to, meta = {})
      @edges << [from, to, meta]
    end

    def to_dot(name: 'Connector')
      buf = +"digraph #{Util.label(name)} {\n"
      buf << "  rankdir=LR;\n"
      @nodes.each do |id, info|
        buf << format("  %s [label=%s, shape=%s];\n",
                      Util.label(id.to_s.inspect),
                      (info[:label] || id).to_s.inspect,
                      shape_for(info[:kind]))
      end
      @edges.each do |from, to, meta|
        lbl = (meta[:label] || '').to_s
        attrs = lbl.empty? ? "" : " [label=#{lbl.inspect}]"
        buf << format("  %s -> %s%s;\n", Util.label(from.to_s.inspect), Util.label(to.to_s.inspect), attrs)
      end
      buf << "}\n"
      buf
    end

    private

    def shape_for(kind)
      case kind
      when 'action' then 'box'
      when 'trigger' then 'diamond'
      when 'method' then 'oval'
      when 'lambda' then 'ellipse'
      when 'http' then 'parallelogram'
      else 'plaintext'
      end
    end
  end

  class Bundle
    attr_reader :root, :issues, :graph, :stats, :salvaged, :lambdas
    def initialize(root:, issues:, graph:, stats:, salvaged:, lambdas: [])
      @root, @issues, @graph, @stats, @salvaged, @lambdas = root, issues, graph, stats, salvaged, lambdas
      freeze
    end

    def to_h
      {
        root: root&.to_h,
        issues: issues.map(&:to_h),
        graph: {
          nodes: graph.nodes,
          edges: graph.edges.to_a
        },
        stats: stats,
        salvaged: salvaged,
        lambdas: lambdas
      }
    end
  end

  module Id
    module_function
    def make(kind:, name:, loc:)
      data = [kind, name, loc&.dig(:begin), loc&.dig(:end), loc&.dig(:line), loc&.dig(:column)].join("\0")
      "n_" + Digest::SHA1.hexdigest(data)[0, 16]
    end
  end
end

# ------------------------------ Parsing --------------------------------------

class AstParser
  def initialize(filename:, source:)
    @filename = filename
    @source = source
    @diagnostics = []
  end

  def parse
    buffer = Parser::Source::Buffer.new(@filename, 1)
    buffer.source = @source

    # Pick a grammar that matches your Ruby to avoid warnings/method gaps.
    ruby_minor = RUBY_VERSION.split('.').first(2).join('.')
    parser_klass =
      case ruby_minor
      when '3.0' then defined?(Parser::Ruby30) ? Parser::Ruby30 : Parser::CurrentRuby
      when '3.1' then defined?(Parser::Ruby31) ? Parser::Ruby31 : Parser::CurrentRuby
      when '3.2' then defined?(Parser::Ruby32) ? Parser::Ruby32 : Parser::CurrentRuby
      when '3.3' then defined?(Parser::Ruby33) ? Parser::Ruby33 : Parser::CurrentRuby
      else             Parser::CurrentRuby
      end

    builder = Parser::Builders::Default.new
    # Guard feature toggles — present in some parser versions only.
    builder.emit_lambda = true                  if builder.respond_to?(:emit_lambda=)
    builder.emit_procarg0 = true                if builder.respond_to?(:emit_procarg0=)
    builder.emit_arg_inside_procarg0 = true     if builder.respond_to?(:emit_arg_inside_procarg0=)
    builder.emit_forward_arg = true             if builder.respond_to?(:emit_forward_arg=)
    builder.emit_file_line_as_literals = true   if builder.respond_to?(:emit_file_line_as_literals=)

    parser = parser_klass.new(builder)

    # Optional: suppress deprecation noise if available
    if parser.respond_to?(:diagnostics) && parser.diagnostics.respond_to?(:all_errors_are_fatal=)
      parser.diagnostics.all_errors_are_fatal = false
    end
    parser.diagnostics.consumer = ->(diag) do
      @diagnostics << (diag.respond_to?(:render) ? diag.render : diag.to_s)
    end

    ast, comments = parser.parse_with_comments(buffer)
    associations = (Parser::Source::Comment.associate(ast, comments) rescue {})
    { ast: ast, comments: associations, diagnostics: @diagnostics }

  rescue Parser::SyntaxError => e
    { ast: nil, comments: [], diagnostics: @diagnostics + [e.message] }
  end
end

# --------- Ripper salvage (when AST parsing fails) ----------------------------

class LexSalvage
  # Best-effort extraction of root keys and nested names without full AST.
  # Handles very large files: single pass over tokens with brace depth tracking.

  Result = Struct.new(:root_keys, :actions, :triggers, :methods, :notes, keyword_init: true)

  def initialize(filename:, source:)
    @filename = filename
    @source = source
  end

  def run
    toks = Ripper.lex(@source) # [[ [lineno, col], :on_xxx, "token", state ], ... ]
    return Result.new(root_keys: {}, actions: [], triggers: [], methods: [], notes: ['ripper_lex_nil']) unless toks

    root_keys = Hash.new { |h, k| h[k] = [] }
    actions, triggers, methods = [], [], []
    notes = []

    # Track {...} nesting; look for labels like "actions:", "triggers:", "methods:"
    stack = []
    i = 0
    while i < toks.length
      pos, type, str, _state = toks[i]
      if type == :on_lbrace
        stack << :brace
      elsif type == :on_rbrace
        stack.pop
      elsif type == :on_label # e.g., "actions:"
        key = str.sub(/:\z/, '')
        root_keys[key] << { line: pos[0], column: pos[1] } if stack.size <= 1 && Workato::ROOT_KEYS.include?(key)
        # crude scan for nested member names in immediate object after a root key
        if %w[actions triggers methods].include?(key)
          # Seek forward until next '{', then collect labels at depth+1
          depth = stack.size
          j = i + 1
          saw_open = false
          local_depth = 0
          while j < toks.length
            _, t2, s2, _ = toks[j]
            if t2 == :on_lbrace
              local_depth += 1
              saw_open = true if local_depth == 1
            elsif t2 == :on_rbrace
              local_depth -= 1
              break if saw_open && local_depth == 0
            elsif t2 == :on_label && saw_open && local_depth == 1
              name = s2.sub(/:\z/, '')
              case key
              when 'actions' then actions << { name: name, line: toks[j][0][0], column: toks[j][0][1] }
              when 'triggers' then triggers << { name: name, line: toks[j][0][0], column: toks[j][0][1] }
              when 'methods' then methods << { name: name, line: toks[j][0][0], column: toks[j][0][1] }
              end
            end
            j += 1
          end
        end
      end
      i += 1
    end

    Result.new(root_keys: root_keys, actions: actions, triggers: triggers, methods: methods, notes: notes)
  rescue => e
    Result.new(root_keys: {}, actions: [], triggers: [], methods: [], notes: ["salvage_error: #{e.class}: #{e.message}"])
  end
end

# ------------------------------ AST Walker -----------------------------------

class ConnectorWalker
  include Util

  def initialize(filename:, ast:, comments: {}, max_warnings: 10_000)
    @filename, @ast, @comments = filename, ast, (comments || {})
    @graph = IR::Graph.new(directed: true)
    @issues = []
    @stats = Hash.new(0)
    @methods_defined = Set.new
    @methods_called = []
    @lambda_records = []
    @max_warnings = max_warnings || 10_000
    @warnings_emitted = 0
    @warning_cap_announced = false
  end

  def walk
    return nil unless @ast
    # Locate the most plausible connector hash
    conn_hash = find_connector_hash(@ast)
    unless conn_hash
      issue(:warning, 'no_connector_hash', 'No plausible top-level connector hash found', node_loc(@ast))
      return nil
    end

    root = build_root(conn_hash)
    finalize_issues(root)
    IR::Bundle.new(root: root, issues: @issues, graph: @graph, stats: @stats, salvaged: false, lambdas: @lambda_records)
  end

  private

  def issue(sev, code, msg, loc = {}, ctx = {})
    sev_sym = sev.to_s.to_sym
    if sev_sym == :warning
      if @warnings_emitted >= @max_warnings
        unless @warning_cap_announced
          @issues << IR::Issue.new(
            severity: :info,
            code: 'warning_cap_reached',
            message: "Reached --max-warnings=#{@max_warnings}; further warnings suppressed",
            loc: {},
            context: {}
          )
          @warning_cap_announced = true
        end
        return
      end
      @warnings_emitted += 1
    end
    @issues << IR::Issue.new(severity: sev, code: code, message: msg, loc: loc || {}, context: ctx || {})
  end

  def traverse(node, &blk)
    return unless node.is_a?(Parser::AST::Node)
    yield node
    node.children.each { |c| traverse(c, &blk) if c.is_a?(Parser::AST::Node) }
  end

  def find_connector_hash(ast)
    candidates = []
    traverse(ast) do |n|
      next unless n.type == :hash
      keys = hash_keys(n)
      score = (keys & Workato::ROOT_KEYS).size
      candidates << [score, n] if score >= 2
    end
    candidates.sort_by { |s, _| -s }.dig(0, 1)
  end

  def hash_pairs(hash_node)
    hash_node.children.select { |c| c.type == :pair }
  end

  def hash_keys(hash_node)
    hash_pairs(hash_node).map { |p| Util.sym_or_str(p.children[0]) }.compact
  end

  def key_value(hash_node, key)
    hash_pairs(hash_node).find { |p| Util.sym_or_str(p.children[0]) == key }&.children&.last
  end

  def build_root(hash_node)
    keys = hash_keys(hash_node)
    unknown = keys - Workato::ROOT_KEYS - %w[title description]
    unknown.each do |k|
      issue(:info, 'unknown_root_key', "Unknown root key: #{k.inspect}", Util.node_loc(hash_node))
    end

    title_node = key_value(hash_node, 'title')
    root = IR::Node.new(
      kind: 'connector',
      name: stringish(title_node) || '(untitled)',
      loc: Util.node_loc(hash_node),
      meta: {
        filename: @filename,
        root_keys: keys.sort,
        doc: extract_docstring(hash_node)
      }
    )

    children = []
    children << extract_connection(hash_node)
    children << extract_test(hash_node)
    children << extract_methods(hash_node)
    children << extract_object_definitions(hash_node)
    children << extract_actions(hash_node)
    children << extract_triggers(hash_node)
    children << extract_pick_lists(hash_node)
    children << extract_webhook_keys(hash_node)
    children << extract_streams(hash_node)

    children.compact!

    root.with_children(children)
  end

  def stringish(node)
    return unless node.is_a?(Parser::AST::Node)
    case node.type
    when :str then node.children[0]
    else
      nil
    end
  end

  # ---------- extractors ------------------------------------------------------

  def extract_connection(root_hash)
    conn = key_value(root_hash, 'connection')
    return unless conn

    # Build meta up front; IR::Node freezes meta, so don't mutate after creation.
    meta = {}

    # base_uri: pair -> value -> literal string (if present)
    if (base_uri_pair = dig_pair(conn, %w[base_uri]))
      if (base_uri_val = base_uri_pair.children.last)
        if (lit = stringish(base_uri_val))
          meta[:base_uri_literal] = lit
        end
      end
    end

    # authorization.type: pair -> value -> literal string
    if (auth_type_pair = dig_pair(conn, %w[authorization type]))
      if (auth_type_val = auth_type_pair.children.last)
        if (lit = stringish(auth_type_val))
          meta[:authorization_type_literal] = lit
        end
      end
    end

    IR::Node.new(kind: 'connection', name: 'connection', loc: Util.node_loc(conn), meta: meta)
  end

  def extract_docstring(node)
    # Don't favor inline comments
    # Instead, prefer leading comments (associated by Parser::Source::Comment.associate)
    return nil unless node && @comments && @comments[node]
    # Keep order; strip leading '# '
    lines = @comments[node].map { |c| c.text.sub(/\A#\s?/, '') }
    lines.empty? ? nil : lines.join("\n")
  end

  def extract_test(root_hash)
    test = key_value(root_hash, 'test')
    return unless test
    lamb = first_lambda(test)
    if lamb
      register_lambda_in_graph('connector#test', lamb[:body], 'test', lamb[:loc])
    else
      issue(:warning, 'test_not_lambda', 'test key found but not a lambda/proc block', Util.node_loc(test))
    end
    IR::Node.new(kind: 'test', name: 'test', loc: Util.node_loc(test), meta: {})
  end

  def extract_methods(root_hash)
    meths = key_value(root_hash, 'methods')
    return unless meths && meths.type == :hash

    entries = []
    hash_pairs(meths).each do |pair|
      name = Util.sym_or_str(pair.children[0]) || '(dynamic)'
      body = pair.children[1]
      loc = Util.node_loc(body)
      @methods_defined << name if name
      lamb = first_lambda(body)
      if lamb
        register_lambda_in_graph("method:#{name}", lamb[:body], 'method', loc)
        entries << IR::Node.new(kind: 'method', name: name, loc: loc, meta: { args: lamb[:args] })
      else
        issue(:warning, 'method_not_lambda', "methods.#{name} is not a lambda/proc", loc)
      end
    end

    IR::Node.new(kind: 'methods', name: 'methods', loc: Util.node_loc(meths), meta: {}, children: entries)
  end

  def extract_object_definitions(root_hash)
    od = key_value(root_hash, 'object_definitions')
    return unless od && od.type == :hash

    defs = hash_pairs(od).map do |pair|
      name = Util.sym_or_str(pair.children[0]) || '(dynamic)'
      body = pair.children[1]
      IR::Node.new(kind: 'object_definition', name: name, loc: Util.node_loc(body), meta: {})
    end
    IR::Node.new(kind: 'object_definitions', name: 'object_definitions', loc: Util.node_loc(od), meta: {}, children: defs)
  end

  def extract_pick_lists(root_hash)
    pl = key_value(root_hash, 'pick_lists') || key_value(root_hash, 'picklists')
    return unless pl
    IR::Node.new(kind: 'pick_lists', name: 'pick_lists', loc: Util.node_loc(pl), meta: {})
  end

  def extract_actions(root_hash)
    actions = key_value(root_hash, 'actions')
    return unless actions && actions.type == :hash

    entries = []
    seen = Set.new
    hash_pairs(actions).each do |pair|
      name = Util.sym_or_str(pair.children[0]) || '(dynamic)'
      if seen.include?(name)
        issue(:warning, 'duplicate_action', "Duplicate action: #{name}", Util.node_loc(pair))
      end
      seen << name
      body = pair.children[1]
      d = extract_action(name, body)
      entries << d if d
    end

    IR::Node.new(kind: 'actions', name: 'actions', loc: Util.node_loc(actions), meta: {}, children: entries)
  end

  def extract_action(name, body_hash)
    unless body_hash&.type == :hash
      issue(:warning, 'action_not_hash', "Action #{name} is not a hash", Util.node_loc(body_hash))
      return IR::Node.new(kind: 'action', name: name, loc: Util.node_loc(body_hash), meta: {})
    end

    keys = hash_keys(body_hash)
    missing = Workato::ACTION_REQUIRED - keys
    unless missing.empty?
      issue(:warning, 'action_missing_required_keys', "Action #{name} missing keys: #{missing.join(', ')}", Util.node_loc(body_hash), { name: name, missing: missing })
    end

    # Lambdas of interest
    %w[input_fields execute output_fields sample_output].each do |k|
      n = key_value(body_hash, k)
      if n
        if (l = first_lambda(n))
          label = "action:#{name}##{k}"
          register_lambda_in_graph(label, l[:body], k, l[:loc])
        else
          issue(:warning, 'not_lambda', "Action #{name}.#{k} is not a lambda/proc", Util.node_loc(n))
        end
      end
    end

    IR::Node.new(kind: 'action', name: name, loc: Util.node_loc(body_hash), meta: { keys: keys.sort })
  end

  def extract_triggers(root_hash)
    triggers = key_value(root_hash, 'triggers')
    return unless triggers && triggers.type == :hash

    entries = []
    seen = Set.new
    hash_pairs(triggers).each do |pair|
      name = Util.sym_or_str(pair.children[0]) || '(dynamic)'
      if seen.include?(name)
        issue(:warning, 'duplicate_trigger', "Duplicate trigger: #{name}", Util.node_loc(pair))
      end
      seen << name
      body = pair.children[1]
      d = extract_trigger(name, body)
      entries << d if d
    end

    IR::Node.new(kind: 'triggers', name: 'triggers', loc: Util.node_loc(triggers), meta: {}, children: entries)
  end

  def extract_trigger(name, body_hash)
    unless body_hash&.type == :hash
      issue(:warning, 'trigger_not_hash', "Trigger #{name} is not a hash", Util.node_loc(body_hash))
      return IR::Node.new(kind: 'trigger', name: name, loc: Util.node_loc(body_hash), meta: {})
    end

    keys = hash_keys(body_hash)
    required_missing = Workato::TRIGGER_REQUIRED - keys
    if required_missing.any?
      issue(:warning, 'trigger_missing_required_keys', "Trigger #{name} missing keys: #{required_missing.join(', ')}", Util.node_loc(body_hash), { name: name, missing: required_missing })
    end

    %w[poll webhook_subscribe webhook_unsubscribe webhook_notification input_fields output_fields sample_output dedup].each do |k|
      n = key_value(body_hash, k)
      next unless n
      if (l = first_lambda(n))
        label = "trigger:#{name}##{k}"
        register_lambda_in_graph(label, l[:body], k, l[:loc])
      else
        issue(:warning, 'not_lambda', "Trigger #{name}.#{k} is not a lambda/proc", Util.node_loc(n))
      end
    end

    IR::Node.new(kind: 'trigger', name: name, loc: Util.node_loc(body_hash), meta: { keys: keys.sort })
  end

  def extract_streams(root_hash)
    node = key_value(root_hash, 'streams')
    return unless node
    # Collect immediate stream names (labels) for graphing/summary
    entries = (node.type == :hash ? hash_pairs(node) : []).map do |pair|
      name = Util.sym_or_str(pair.children[0]) || '(dynamic)'
      IR::Node.new(kind: 'stream', name: name, loc: Util.node_loc(pair.children[1]), meta: {})
    end
    IR::Node.new(kind: 'streams', name: 'streams', loc: Util.node_loc(node), meta: {}, children: entries)
  end

  def extract_webhook_keys(root_hash)
    wk = key_value(root_hash, 'webhook_keys')
    return unless wk
    if (l = first_lambda(wk))
      register_lambda_in_graph('connector#webhook_keys', l[:body], 'webhook_keys', l[:loc])
    else
      issue(:warning, 'webhook_keys_not_lambda', 'webhook_keys is not a lambda/proc', Util.node_loc(wk))
    end
    IR::Node.new(kind: 'webhook_keys', name: 'webhook_keys', loc: Util.node_loc(wk), meta: {})
  end

  # ---------- helpers ---------------------------------------------------------

  def first_child(node)
    node&.children&.first
  end

  def dig_pair(hash_node, path)
    # Returns the *pair node* for the final segment, or nil.
    # Example: dig_pair(conn_hash, %w[authorization type]) => the :type pair node.
    return nil unless hash_node.is_a?(Parser::AST::Node) && hash_node.type == :hash
    return nil unless path.is_a?(Array)

    cur = hash_node
    last_pair = nil

    path.each do |segment|
      return nil unless cur.is_a?(Parser::AST::Node) && cur.type == :hash
      last_pair = hash_pairs(cur).find { |p| Util.sym_or_str(p.children[0]) == segment }
      return nil unless last_pair
      cur = last_pair.children.last
    end

    last_pair
  end

  def first_lambda(node)
    return nil unless node.is_a?(Parser::AST::Node)
    case node.type
    when :block
      send_node, args_node, *_ = node.children
      is_lambda = (send_node.type == :lambda) ||
                  (send_node.type == :send && [:lambda, :proc].include?(send_node.children[1])) ||
                  (send_node.type == :send && send_node.children[1] == :new && Util.const_name(send_node.children[0]) == 'Proc')
      
      # `{}` blocks appear as :block too; keep same detection
      if is_lambda
        { body: node, args: extract_args(args_node), loc: Util.node_loc(node) }
      end
      when :numblock # Ruby 2.7+ shorthand `{ _1 }`; treat as lambda-like for salvage
        { body: node, args: [], loc: Util.node_loc(node) }
    else
      nil
    end
  end

  def extract_args(args_node)
    return [] unless args_node.is_a?(Parser::AST::Node) && args_node.type == :args
    args_node.children.map do |a|
      if a.type == :arg || a.type == :optarg
        a.children[0].to_s
      elsif a.type == :kwarg || a.type == :kwoptarg
        "#{a.children[0]}:"
      elsif a.type == :restarg
        "*#{a.children[0]}"
      elsif a.type == :kwrestarg
        "**#{a.children[0]}"
      else
        a.type.to_s
      end
    end
  end

  def register_lambda_in_graph(owner_label, block_node, role, loc)
    # Ensure node
    @graph.add_node(owner_label, label: owner_label, kind: role == 'method' ? 'method' : 'lambda')
    @lambda_records << { owner: owner_label, role: role, loc: loc }

    # Traverse for sends
    traverse(block_node) do |n|
      next unless n.type == :send
      recv, mname, *args = n.children

      if Workato::HTTP_VERBS.include?(mname)
        first_arg = args[0]
        lit = if first_arg&.type == :str
                first_arg.children[0]
              else
                nil
              end
        http_node_id = "#{owner_label}::http##{mname}(#{lit ? lit[0..50] : '...'})"
        @graph.add_node(http_node_id, label: "#{mname.upcase} #{lit || '(dynamic)'}", kind: 'http')
        @graph.add_edge(owner_label, http_node_id, label: 'calls')
        @stats["http_#{mname}"] += 1
      elsif mname == :call && recv.nil?
        if args[0] && (args[0].type == :sym || args[0].type == :str)
          meth = args[0].children[0].to_s
          # Only treat as a Workato method when it looks like a valid identifier,
          # not a model id like "gemini-1.5-pro" or "text-embedding-004".
          if meth.match?(/\A[A-Za-z_][A-Za-z0-9_]*\z/)
            call_loc = Util.node_loc(n) # <-- use the actual send-node location
            @methods_called << { from: owner_label, to: "method:#{meth}", name: meth, loc: call_loc }
            @graph.add_node("method:#{meth}", label: "method:#{meth}", kind: 'method')
            @graph.add_edge(owner_label, "method:#{meth}", label: 'calls')
            @stats['method_calls'] += 1
          end
        else
          # Dynamic dispatch: call(variable) — hard to analyze and can hide cycles
          issue(:warning, 'dynamic_call',
                "Dynamic method dispatch via call(#{args[0]&.type || 'unknown'}) in #{owner_label}",
                Util.node_loc(n))
        end
     elsif mname == :after_error_response
       @graph.add_edge(owner_label, owner_label, label: 'after_error_response')
       @stats['error_handlers'] += 1
     elsif mname == :checkpoint!
       @graph.add_edge(owner_label, owner_label, label: 'checkpoint!')
       @stats['streaming_checkpoints'] += 1
      elsif mname == :eval || mname == :system
        issue(:warning, 'dangerous_call', "Use of #{mname} inside #{owner_label}", Util.node_loc(n))
      end
    end
  end

  def finalize_issues(root)
    # Undefined/unused methods
    called = @methods_called.map { |c| c[:name] }.to_set
    undefined_methods = called - @methods_defined
    unless undefined_methods.empty?
      undefined_methods.each do |m|
        callsites = @methods_called.select { |c| c[:name] == m }
        callsites.each do |cs|
          issue(:warning, 'undefined_method', "call(#{m.inspect}) has no corresponding methods.#{m}", cs[:loc], { method: m })
        end
      end
    end

    unused = @methods_defined - called
    unused.each do |m|
      issue(:info, 'unused_method', "methods.#{m} is never called", {}, { method: m })
    end

    # Backticks detected? (command substitution)
    traverse(@ast) do |n|
      if n.type == :xstr
        issue(:warning, 'dangerous_xstr', 'Backtick command execution detected', Util.node_loc(n))
      end
    end

    # Method call cycles (methods: call(:foo) graph)
    detect_method_cycles

    # Counters
    @stats['actions'] = count_kind(root, 'action')
    @stats['triggers'] = count_kind(root, 'trigger')
    @stats['methods'] = @methods_defined.size
  end

  def count_kind(node, kind)
    return 0 unless node
    (node.kind == kind ? 1 : 0) + node.children.sum { |ch| count_kind(ch, kind) }
  end

  def detect_method_cycles
    # Build adjacency (method_name -> Set[method_name]) from recorded call edges
    adj      = Hash.new { |h,k| h[k] = Set.new }
    edge_loc = Hash.new { |h,k| h[k] = [] }

    @methods_called.each do |c|
      from_label = c[:from]            # e.g., "method:foo" or "action:bar#execute"
      to_label   = c[:to]              # e.g., "method:baz"
      next unless from_label.start_with?('method:') && to_label.start_with?('method:')
      from_m = from_label.split(':', 2)[1]
      to_m   = to_label.split(':',   2)[1]
      adj[from_m] << to_m
      edge_loc[[from_m, to_m]] << (c[:loc] || {})
    end

    nodes = (@methods_defined.to_a | adj.keys | adj.values.flat_map(&:to_a)).uniq
    sccs  = tarjan_scc(nodes, adj)

    cycle_count = 0

    sccs.each do |comp|
      if comp.size > 1
        cycle_count += 1
        loc = find_any_edge_loc_in_comp(comp, edge_loc) || {}
        issue(:error, 'method_cycle',
              "methods cycle: #{comp.join(' → ')} → #{comp.first}",
              loc, { cycle: comp })
      elsif comp.size == 1
        n = comp.first
        if adj[n].include?(n)
          cycle_count += 1
          loc = (edge_loc[[n, n]]&.first) || {}
          issue(:error, 'method_cycle',
                "methods self-recursion: #{n} calls itself",
                loc, { cycle: [n] })
        end
      end
    end

    @stats['method_graph_nodes'] = nodes.size
    @stats['method_graph_edges'] = adj.values.sum(&:size)
    @stats['method_cycles']      = cycle_count
  end

  def tarjan_scc(nodes, adj)
    index = 0
    stack = []
    onstk = {}
    idx   = {}
    low   = {}
    comps = []

    strongconnect = lambda do |v|
      idx[v] = index
      low[v] = index
      index += 1
      stack.push(v)
      onstk[v] = true

      (adj[v] || []).each do |w|
        if !idx.key?(w)
          strongconnect.call(w)
          low[v] = [low[v], low[w]].min
        elsif onstk[w]
          low[v] = [low[v], idx[w]].min
        end
      end

      if low[v] == idx[v]
        comp = []
        begin
          w = stack.pop
          onstk[w] = false
          comp << w
        end until w == v
        comps << comp
      end
    end

    nodes.each { |v| strongconnect.call(v) unless idx.key?(v) }
    comps
  end

  def find_any_edge_loc_in_comp(comp, edge_loc)
    comp.each do |a|
      comp.each do |b|
        locs = edge_loc[[a, b]]
        return locs.first if locs && !locs.empty?
      end
    end
    nil
  end

end

# ------------------------------ Emitters -------------------------------------

class Emit
  def self.write_all(bundle:, outdir:, base:, pretty:, graph_name:,
                     ndjson:, source: nil, emit: [], ai_pack: true, 
                     max_context_bytes: 12000)
    Dir.mkdir(outdir) unless Dir.exist?(outdir)
    paths = {}

    # JSON
    if emit.include?('json')
      paths[:json] = write_json(
        File.join(outdir, Util.safe_filename("#{base}.ir", '.json')), bundle.to_h, pretty)
    end

    # DOT
    if emit.include?('dot')
      paths[:dot] = write_text(
        File.join(outdir, Util.safe_filename("#{base}.graph", '.dot')),
        bundle.graph.to_dot(name: graph_name))
    end

    # Graph JSON
    if emit.include?('graphjson')
      paths[:graph_json] = write_json(
        File.join(outdir, Util.safe_filename("#{base}.graph", '.json')),
        { nodes: bundle.graph.nodes.map { |id, info| { id: id, **info } },
          edges: bundle.graph.edges.to_a.map { |from, to, meta| { from: from, to: to, **(meta || {}) } } }, pretty)
    end

    # Markdown summary
    if emit.include?('md')
      paths[:md] = write_text(
        File.join(outdir, Util.safe_filename("#{base}.summary", '.md')),
        Emit.markdown_summary(bundle)
      )
    end

    # NDJSON events (issues + http calls)
    if ndjson && emit.include?('ndjson')
      paths[:ndjson] = write_ndjson(File.join(outdir, Util.safe_filename("#{base}.events", '.ndjson'))) do |f|
        bundle.issues.each { |iss| f.puts({ type: 'issue', **iss.to_h }.to_json) }
        bundle.graph.edges.each do |from, to, meta|
          f.puts({ type: 'http_call', from: from, to: to, meta: meta }.to_json) if to.to_s.include?('::http#')
        end
      end
    end

    # Sourcemap JSON
    if source && bundle.root && emit.include?('sourcemap')
      smap = {}
      walk = lambda do |n|
        smap[n.id] = { kind: n.kind, name: n.name, loc: n.loc }
        n.children.each(&walk)
      end
      walk.call(bundle.root)
      paths[:sourcemap] = write_json(File.join(outdir, Util.safe_filename("#{base}.sourcemap", '.json')),
                                     smap, pretty)
    end

    # Embedding JSONL (atoms)
    if source && bundle.root && emit.include?('embed')
      paths[:embed] = write_ndjson(File.join(outdir, Util.safe_filename("#{base}.embed", '.jsonl'))) do |f|
        enumerate_atoms(bundle).each do |atom|
          text = Util.slice_source(source, atom[:loc])
          f.puts({ **atom, text: text }.to_json)
        end
      end
    end

    # Tokens NDJSON (fallback lexical stream)
    if source && emit.include?('tokens')
      paths[:tokens] = write_ndjson(File.join(outdir, Util.safe_filename("#{base}.tokens", '.ndjson'))) do |f|
        (Ripper.lex(source) || []).each do |(pos, type, str, _)|
          line, col = pos
          begin_pos = compute_begin_pos(source, line, col)
          f.puts({ line: line, column: col, begin: begin_pos, end: begin_pos + str.bytesize,
                   type: type, text: str }.to_json)
        end
      end
    end

    # SARIF
    paths[:sarif] = write_json(
      File.join(outdir, Util.safe_filename("#{base}.sarif", '.json')), sarif(bundle), pretty) if emit.include?('sarif')

    # JSON Schema for IR (coarse, versioned)
    paths[:schema] = write_json(
      File.join(outdir, Util.safe_filename("#{base}.schema", '.json')), ir_schema(version: "1-0-0"), true) if emit.include?('schema')
    
    # Index
    paths[:index] = write_json(
      File.join(outdir, Util.safe_filename("#{base}.index", '.json')), { artifacts: paths.compact }, true) if emit.include?('index')

    # AI Pack
    if ai_pack && source && bundle.root
      paths[:gemini_context] = write_ndjson(File.join(outdir, Util.safe_filename("#{base}.gemini.context", '.jsonl'))) do |f|
        enumerate_atoms(bundle).each do |atom|
          next unless atom[:kind] =~ /\A(action|trigger|method|lambda)\z/
          text = Util.slice_source(source, atom[:loc], max_bytes: max_context_bytes)
          next unless text && !text.empty?
          ctx = {
            id: atom[:id], kind: atom[:kind], role: role_of(atom),
            fqname: atom[:fqname], loc: atom[:loc], file: atom[:file],
            http: atom[:http], keys: atom[:keys], text: text, doc: nil
          }
          f.puts(JSON.dump(ctx))
        end
      end

      # Verex Upsert
      paths[:vertex_upsert] = write_ndjson(File.join(outdir, Util.safe_filename("#{base}.vertex.upsert", '.jsonl'))) do |f|
        enumerate_atoms(bundle).each do |atom|
          next unless atom[:kind] =~ /\A(action|trigger|method|lambda)\z/
          text = Util.slice_source(source, atom[:loc], max_bytes: max_context_bytes)
          next unless text && !text.empty?
          up = {
            id: atom[:id],
            namespace: "workato-connectors",
            doc_id: File.basename(atom[:file].to_s),
            section: atom[:fqname],
            text: text,
            metadata: {
              kind: atom[:kind], role: role_of(atom),
              http_verbs: atom.dig(:http, :verbs),
              endpoints:  atom.dig(:http, :endpoints),
              loc: atom[:loc],
              model_suggested_token_count: text.length / 4
            }
          }
          f.puts(JSON.dump(up))
        end
      end

      # Gemini prompts
      paths[:gemini_prompts] = write_text(File.join(outdir, Util.safe_filename("#{base}.gemini.prompts", '.md')),
        default_prompts_md)
      end

    paths
  end

  def self.role_of(atom)
    # Map fqname suffix to a simple role for prompting
    if atom[:fqname]&.include?('#')
      atom[:fqname].split('#').last
    else
      atom[:name] || 'unknown'
    end
  end

  def self.default_prompts_md
    <<~MD
    # Gemini Prompt Templates for Workato Connectors
    ## 1) Summarize the connector
    System: You are analyzing a Workato custom connector. Be precise; cite line numbers.
    User: Given these context blocks (each has file, loc), produce: (a) purpose, (b) list of actions/triggers, (c) HTTP inventory, (d) missing SDK-required keys.
    Provide a JSON object with fields: purpose, actions[], triggers[], http[], issues[].

    ## 2) Explain one action's IO contract
    System: You are analyzing Workato `actions.<name>`.
    User: Using the context blocks tagged role=execute|input_fields|output_fields for <name>, summarize inputs/outputs and any validations. JSON fields: inputs[], outputs[], validations[], notes[].

    ## 3) Risk scan
    System: Identify dangerous calls (eval, system, backticks) and missing `dedup` in triggers.
    User: Output JSON: { "danger":[], "missing_required":[], "webhook_notes":[] } with file+line per item.
    MD
  end

  def self.enumerate_atoms(bundle)
    atoms = []
    return atoms unless bundle.root
    each_node = lambda do |n, path|
      fq = (path + ["#{n.kind}.#{n.name}"]).join('/')
      atoms << { id: n.id, kind: n.kind, name: n.name, fqname: fq, loc: n.loc, file: n.meta[:filename],
                 keys: (n.meta[:keys] || n.meta[:root_keys] || []), http: http_summary(bundle, n) }
      n.children.each { |ch| each_node.call(ch, path + ["#{n.kind}.#{n.name}"]) }
    end
    each_node.call(bundle.root, [])
    # Also emit lambda “atoms” with their labels
    bundle.lambdas.each do |lam|
      atoms << { id: "lam:#{Digest::SHA1.hexdigest([lam[:owner], lam[:loc]&.[](:begin), lam[:loc]&.[](:end)].join("\0"))[0,16]}",
                 kind: 'lambda', name: lam[:role], fqname: lam[:owner], loc: lam[:loc], file: bundle.root.meta[:filename] }
    end
    atoms
  end

  def self.http_summary(bundle, node)
    prefix = case node.kind
             when 'action'  then "action:#{node.name}#"
             when 'trigger' then "trigger:#{node.name}#"
             when 'method'  then "method:#{node.name}"
             else nil
             end
    return {} unless prefix
    edges = bundle.graph.edges.to_a.select { |from, to, _| from.start_with?(prefix) && to.include?('::http#') }
    labels = edges.map { |_, to, _| (bundle.graph.nodes[to] || {})[:label] }.compact
    verbs = labels.map { |lbl| lbl.split(' ').first }.uniq
    { verbs: verbs, endpoints: labels }
  end

  def self.compute_begin_pos(source, line, col)
    idx = [0]
    source.each_line { |l| idx << (idx.last + l.bytesize) }
    (idx[line - 1] || 0) + col
  end

  def self.write_json(path, obj, pretty)
    File.write(path, pretty ? JSON.pretty_generate(obj) : JSON.dump(obj))
    path
  end

  def self.write_text(path, text)
    File.write(path, text)
    path
  end

  def self.write_ndjson(path)
    File.open(path, 'w') { |f| yield f }
    path
  end

  def self.sarif(bundle)
    rules = bundle.issues.map(&:code).uniq.map { |code| { "id" => code, "name" => code } }
    results = bundle.issues.map do |iss|
      {
        "ruleId" => iss.code,
        "level"  => {"info"=>"note","warning"=>"warning","error"=>"error"}[iss.severity.to_s] || "note",
        "message"=> { "text" => iss.message },
        "locations" => [{
          "physicalLocation" => {
            "artifactLocation" => { "uri" => bundle.root&.meta&.[](:filename).to_s },
            "region" => { "startLine" => iss.loc[:line].to_i, "startColumn" => iss.loc[:column].to_i }
          }
        }]
      }
    end
    {
      "version"=>"2.1.0",
      "$schema"=>"https://json.schemastore.org/sarif-2.1.0.json",
      "runs"=>[{
        "tool"=>{"driver"=>{"name"=>"Workato Connector Inspector","informationUri"=>"https://github.com/workato/workato-connector-sdk","rules"=>rules}},
        "results"=>results
      }]
    }
  end

  def self.ir_schema(version:)
    {
      "$id"=>"https://example.com/workato-connector-ir.schema.json",
      "$schema"=>"http://json-schema.org/draft-07/schema#",
      "title"=>"Workato Connector IR",
      "version"=>version,
      "type"=>"object",
      "required"=>["root","issues","graph","stats","salvaged"],
      "properties"=>{
        "root"=>{"$ref"=>"#/definitions/node"},
        "issues"=>{"type"=>"array","items"=>{"$ref"=>"#/definitions/issue"}},
        "graph"=>{"type"=>"object","properties"=>{
          "nodes"=>{"type"=>"object"},
          "edges"=>{"type"=>"array","items"=>{"type"=>"array"}}
        }},
        "stats"=>{"type"=>"object"},
        "salvaged"=>{"type"=>"boolean"},
        "lambdas"=>{"type"=>"array","items"=>{"type"=>"object"}}
      },
      "definitions"=>{
        "loc"=>{"type"=>"object","properties"=>{
          "line"=>{"type"=>"integer"}, "column"=>{"type"=>"integer"},
          "length"=>{"type"=>"integer"}, "begin"=>{"type"=>"integer"}, "end"=>{"type"=>"integer"}
        }},
        "node"=>{"type"=>"object","required"=>["id","kind","name","loc","meta","children"],
          "properties"=>{"id"=>{"type"=>"string"}, "kind"=>{"type"=>"string"}, "name"=>{"type"=>"string"},
            "loc"=>{"$ref"=>"#/definitions/loc"},
            "meta"=>{"type"=>"object"}, "children"=>{"type"=>"array","items"=>{"$ref"=>"#/definitions/node"}}}},
        "issue"=>{"type"=>"object","properties"=>{
          "severity"=>{"type"=>"string"}, "code"=>{"type"=>"string"}, "message"=>{"type"=>"string"},
          "loc"=>{"$ref"=>"#/definitions/loc"}, "context"=>{"type"=>"object"}}}
      }
    }
  end

  def self.markdown_summary(bundle)
    root = bundle.root
    return "# Connector summary\n\n_No connector structure found._\n" unless root

    actions = collect(root, 'action')
    triggers = collect(root, 'trigger')
    methods = collect(root, 'method')

    buf = +"# #{root.name}\n\n"
    buf << "File: `#{root.meta[:filename]}`\n\n"
    buf << "Root keys: #{(root.meta[:root_keys] || []).join(', ')}\n\n"
    buf << "Counts: **#{actions.size}** actions, **#{triggers.size}** triggers, **#{methods.size}** methods\n\n"

    unless actions.empty?
      buf << "## Actions\n"
      actions.sort_by!(&:name)
      actions.each do |a|
        buf << "- **#{a.name}** (line #{a.loc[:line]})\n"
      end
      buf << "\n"
    end

    unless triggers.empty?
      buf << "## Triggers\n"
      triggers.sort_by!(&:name)
      triggers.each do |t|
        buf << "- **#{t.name}** (line #{t.loc[:line]})\n"
      end
      buf << "\n"
    end

    unless methods.empty?
      buf << "## Methods\n"
      methods.sort_by!(&:name)
      methods.each do |m|
        buf << "- **#{m.name}** (line #{m.loc[:line]})\n"
      end
      buf << "\n"
    end

    unless bundle.issues.empty?
      buf << "## Issues (#{bundle.issues.size})\n"
      bundle.issues.each do |iss|
        loc = iss.loc[:line] ? "line #{iss.loc[:line]}" : "unknown loc"
        buf << "- [#{iss.severity}] **#{iss.code}** at #{loc}: #{iss.message}\n"
      end
      buf << "\n"
    end

    buf << "## Notes\n"
    buf << "- This summary is generated statically from source; no code was executed.\n"
    buf
  end

  def self.collect(node, kind, out = [])
    out << node if node.kind == kind
    node.children.each { |ch| collect(ch, kind, out) }
    out
  end
end

# ------------------------------- CLI -----------------------------------------

class CLI
  def self.run(argv)
    options = {
      outdir: './out',
      base: 'connector',
      pretty: true,
      emit: %w[json md dot ndjson],
      graph_name: 'Connector',
      max_warnings: 10_000
    }

    options[:emit] = %w[json md dot ndjson graphjson sourcemap embed tokens sarif schema index]

    optparser = OptionParser.new do |opts|
      opts.banner = "Usage: #{File.basename($PROGRAM_NAME)} PATH/TO/connector.rb [options]"

      opts.on('--outdir DIR', 'Output directory (default: ./out)') { |v| options[:outdir] = v }
      opts.on('--base NAME', 'Base filename for outputs (default: connector)') { |v| options[:base] = v }
      opts.on('--emit LIST', 'Comma-separated: json,md,dot,ndjson (default: all)') { |v| options[:emit] = v.split(',').map(&:strip) }
      opts.on('--pretty', 'Pretty-print JSON (default: on)') { options[:pretty] = true }
      opts.on('--no-pretty', 'Compact JSON') { options[:pretty] = false }
      opts.on('--graph-name NAME', 'Graph name for DOT') { |v| options[:graph_name] = v }
      opts.on('--max-warnings N', Integer, 'Cap number of warnings collected (default: 10000)') { |v| options[:max_warnings] = v }
      opts.on('-h', '--help', 'Show help') { puts opts; exit 0 }
    end

    optparser.parse!(argv)
    path = argv.first
    unless path && File.file?(path)
      $stderr.puts optparser
      exit 2
    end

    source, _ = Util.read_source(path)

    # 1) Parse
    ast_res = AstParser.new(filename: path, source: source).parse

    bundle =
      if ast_res[:ast]
        # 2) Walk AST
        walker = ConnectorWalker.new(
          filename: path,
          ast: ast_res[:ast],
          comments: ast_res[:comments],
          max_warnings: options[:max_warnings]
        )
        walker.walk
      else
        # 3) Salvage
        salvage = LexSalvage.new(filename: path, source: source).run
        issues = [
          IR::Issue.new(severity: :error, code: 'syntax_error', message: 'Parser failed; salvage mode engaged', loc: {}, context: { diagnostics: ast_res[:diagnostics] })
        ]
        root_children = []
        root_children << IR::Node.new(kind: 'actions', name: 'actions', loc: {}, meta: {}, children: salvage.actions.map { |a| IR::Node.new(kind: 'action', name: a[:name], loc: { line: a[:line], column: a[:column] }, meta: {}) })
        root_children << IR::Node.new(kind: 'triggers', name: 'triggers', loc: {}, meta: {}, children: salvage.triggers.map { |t| IR::Node.new(kind: 'trigger', name: t[:name], loc: { line: t[:line], column: t[:column] }, meta: {}) })
        root_children << IR::Node.new(kind: 'methods', name: 'methods', loc: {}, meta: {}, children: salvage.methods.map { |m| IR::Node.new(kind: 'method', name: m[:name], loc: { line: m[:line], column: m[:column] }, meta: {}) })
        root = IR::Node.new(kind: 'connector', name: '(salvaged)', loc: {}, meta: { filename: path, root_keys: salvage.root_keys.keys })
                 .with_children(root_children.compact)
        IR::Bundle.new(root: root, issues: issues, graph: IR::Graph.new, stats: {}, salvaged: true)
      end

    # 4) Emit
    Emit.write_all(bundle: bundle, outdir: options[:outdir], base: options[:base], 
      pretty: options[:pretty],
      graph_name: options[:graph_name],
      ndjson: options[:emit].include?('ndjson'),
      source: source,
      emit: options[:emit], 
      ai_pack: true)

    # 5) Selectively print summary path for convenience
    puts "Wrote outputs to #{options[:outdir]}"
  end
end

# ------------------------------ Entry point ----------------------------------

if __FILE__ == $PROGRAM_NAME
  # Safe debug log (no out-of-scope vars)
  if ENV['ANALYZER_DEBUG'] == '1'
    parser_ver = defined?(Parser::VERSION) ? Parser::VERSION : 'unknown'
    $stderr.puts "[env] ruby=#{RUBY_VERSION} parser=#{parser_ver}"
  end

  CLI.run(ARGV)
end
