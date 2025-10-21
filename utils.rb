# frozen_string_literal: true

require 'json'
require 'time'
require 'securerandom'
require 'digest'
require 'cgi'


{
  title: "RAG Utilities",
  description: "Custom utility functions for RAG email response system",
  version: "0.3.0",
  help: lambda do 
    { body: "Provides text processing, chunking, similarity, prompt building, and validation utilities for retrieval-augmented generation (RAG) systems." }
  end,

  # --------- CONNECTION ---------------------------------------------------
  connection: { 
    fields: [],
    authorization: { type: 'none' }
  },
  # --------- CONNECTION TEST ----------------------------------------------
  test: lambda do |_connection|
    { success: true, message: 'Connected successfully.' }
  end,
  # --------- METHODS ------------------------------------------------------
  methods: {

    guid: lambda do
      SecureRandom.uuid
    end,

    now_iso: lambda do
      Time.now.utc.iso8601
    end,

    generate_document_id: lambda do |file_path, checksum|
      Digest::SHA256.hexdigest("#{file_path.to_s.strip}|#{checksum.to_s.strip}")
    end,

    est_tokens: lambda do |s|
      s.to_s.strip.split(/\s+/).size
    end,

    # clamp int with defaults
    int: lambda do |v, default|
      i = (v.is_a?(Numeric) ? v : v.to_s[/^-?\d+$/] ? v.to_i : default)
      i.nil? ? default : i
    end,

    telemetry: lambda do |success, started_at, meta={}|
      { 'success'=>!!success, 'timestamp'=>Time.now.utc.iso8601,
        'metadata'=>meta||{}, 'trace'=>{ 'correlation_id'=>SecureRandom.uuid,
        'duration_ms'=>((Time.now-started_at)*1000).round } }
    end,
    
    util_last_boundary_end: lambda do |segment, regex|
      matches = segment.to_enum(:scan, regex).map { Regexp.last_match }
      matches.empty? ? nil : matches.last.end(0)
    end,

    chunk_text_smart: lambda do |text, size, overlap|
      return [] if text.to_s.empty? || size <= 0 || overlap.negative? || overlap >= size
      # treat size/overlap as approx tokens; convert to chars (≈4 chars/token)
      chars_per_chunk = size * 4
      char_overlap    = overlap * 4

      chunks, i, idx = [], 0, 0
      while i < text.length
        tentative_end = [i + chars_per_chunk, text.length].min
        chunk_end = tentative_end
        segment = text[i...tentative_end]

        # prefer paragraph break, then sentence break, near the end
        if tentative_end < text.length
          para_rel = call(:util_last_boundary_end, segment, %r{(?:\r?\n){2,}})
          chunk_end = i + para_rel if para_rel
        end
        if chunk_end == tentative_end && tentative_end < text.length
          sent_rel = call(:util_last_boundary_end, segment, %r{[.!?](?:"|'|\)|\])?\s})
          chunk_end = i + sent_rel if sent_rel
        end

        chunk_end = [i + chars_per_chunk, text.length].min if chunk_end <= i
        piece = text[i...chunk_end]
        chunks << {
          'chunk_id'    => SecureRandom.uuid,
          'chunk_index' => idx,
          'text'        => piece,
          'token_count' => (piece.length / 4.0).ceil,
          'start_char'  => i,
          'end_char'    => chunk_end,
          'metadata'    => {}
        }
        idx += 1
        break if chunk_end >= text.length
        i = [chunk_end - char_overlap, chunk_end].max
      end
      chunks
    end,

    util_diff_lines: lambda do |cur_text, prev_text|
      cur = cur_text.to_s.split("\n"); prev = prev_text.to_s.split("\n")
      i = j = 0; win = 20; added = []; removed = []; modified = []
      while i < cur.length && j < prev.length
        if cur[i] == prev[j]; i+=1; j+=1; next; end
        i2 = ((i+1)..[i+win, cur.length-1].min).find { |k| cur[k] == prev[j] }
        j2 = ((j+1)..[j+win, prev.length-1].min).find { |k| prev[k] == cur[i] }
        if i2
          block = cur[i...i2]; added.concat(block)
          modified << { 'type'=>'added','current_range'=>[i,i2-1],'previous_range'=>[j-1,j-1],'current_lines'=>block }
          i = i2
        elsif j2
          block = prev[j...j2]; removed.concat(block)
          modified << { 'type'=>'removed','current_range'=>[i-1,i-1],'previous_range'=>[j,j2-1],'previous_lines'=>block }
          j = j2
        else
          modified << { 'type'=>'modified','current_range'=>[i,i],'previous_range'=>[j,j],'current_lines'=>[cur[i]],'previous_lines'=>[prev[j]] }
          added << cur[i]; removed << prev[j]; i+=1; j+=1
        end
      end
      if i < cur.length
        block = cur[i..-1]; added.concat(block)
        modified << { 'type'=>'added','current_range'=>[i,cur.length-1],'previous_range'=>[j-1,j-1],'current_lines'=>block }
      elsif j < prev.length
        block = prev[j..]; removed.concat(block)
        modified << { 'type'=>'removed','current_range'=>[i-1,i-1],'previous_range'=>[j,prev.length-1],'previous_lines'=>block }
      end
      total = [cur.length, prev.length].max
      { added: added, removed: removed, modified_sections: modified, line_change_percentage: total.zero? ? 0.0 : (((added.length+removed.length).to_f/total)*100).round(2) }
    end,

    # --- Email/text cleanup helpers ---------------------------------------

    util_normalize_newlines: lambda do |s|
      s.to_s.gsub("\r\n", "\n").gsub("\r", "\n")
    end,

    util_preserve_angle_emails: lambda do |s|
      # Replace <user@domain> with sentinel so tag removal doesn't kill it, then restore.
      s.gsub(/<([A-Z0-9._%+\-]+@[A-Z0-9.\-]+\.[A-Z]{2,})>/i) { "‹EMAIL:#{Regexp.last_match(1)}›" }
    end,

    util_restore_angle_emails: lambda do |s|
      s.gsub(/‹EMAIL:([^›]+)›/) { "<#{Regexp.last_match(1)}>" }
    end,

    util_html_to_text_minimal: lambda do |s|
      t = call(:util_preserve_angle_emails, s.to_s)
      t = CGI.unescapeHTML(t)

      # line-breakish tags → newlines
      t = t.gsub(/<(br|br\/)\s*>/i, "\n")
           .gsub(/<\/p\s*>/i, "\n\n")
           .gsub(/<(p|div|section|header|footer|article)\b[^>]*>/i, "\n")
           .gsub(/<\/(div|section|header|footer|article)\s*>/i, "\n")

      # lists: keep bullets
      t = t.gsub(/<li\b[^>]*>/i, "\n• ").gsub(/<\/li\s*>/i, "")
      t = t.gsub(/<ul\b[^>]*>/i, "\n").gsub(/<\/ul\s*>/i, "\n")
      t = t.gsub(/<ol\b[^>]*>/i, "\n").gsub(/<\/ol\s*>/i, "\n")
      t = t.gsub(/<hr\b[^>]*>/i, "\n----\n")

      # strip remaining tags (now safer)
      t = t.gsub(/<[^>]+>/, "")

      # restore angle-bracket emails
      call(:util_restore_angle_emails, t)
    end,

    util_strip_reply_history: lambda do |s|
      lines = s.to_s.split("\n")

      # Find first line that strongly indicates the start of quoted history.
      cut_idx = nil
      lines.each_with_index do |line, idx|
        l = line.strip
        if l.start_with?('-----Original Message-----') ||
           l.match?(/^From:\s/i) && lines[idx,4].any? { |x| x&.match?(/^Sent:|^To:|^Subject:/i) } ||
           l.match?(/^On .{1,120} wrote:$/i) ||
           l.start_with?('Begin forwarded message:')
          cut_idx = idx
          break
        end
      end

      if cut_idx
        kept = lines[0...cut_idx]
        removed = lines[cut_idx..-1].join("\n")
        [kept.join("\n"), removed]
      else
        [s.to_s, ""]
      end
    end,

    util_strip_signature: lambda do |s|
      lines = s.to_s.split("\n")
      # Search for signature delimiter near the bottom
      sig_idx = nil
      window = [5, (lines.length * 0.6).to_i].max  # only consider last ~60%
      (lines.length-1).downto([lines.length - window, 0].max) do |i|
        if lines[i].strip.match?(/^--\s*$/) || lines[i].strip.match?(/^—\s*$/)
          sig_idx = i
          break
        end
      end
      if sig_idx
        kept = lines[0...sig_idx]
        removed = lines[sig_idx..-1].join("\n")
        [kept.join("\n"), removed]
      else
        [s.to_s, ""]
      end
    end,

    util_strip_disclaimers: lambda do |s|
      text = s.to_s
      patterns = [
        /this message.*confidential.*intended recipient/mi,
        /confidentiality notice/mi,
        /the information contained in this email.*confidential/mi,
        /^DISCLAIMER:\s.*$/mi
      ]
      removed = []
      patterns.each do |rx|
        if text.match?(rx)
          removed << text[rx]
          text = text.gsub(rx, '')
        end
      end
      [text, removed]
    end,

    util_extract_urls_normalized: lambda do |s|
      # capture http(s) and mailto. Strip trailing ),.]; dedupe.
      raw = s.to_s.scan(%r{(?:https?://\S+|mailto:[^\s)]+)})
      norm = raw.map { |u| u.sub(/[)\].,;:]+$/, '') }.uniq
      norm
    end,

    util_normalize_whitespace: lambda do |s|
      t = s.to_s
      t = t.gsub(/[ \t]+/, ' ')
      t = t.gsub(/\n{3,}/, "\n\n")
      t.strip
    end

  },

  # --------- OBJECT DEFINITIONS -------------------------------------------
  object_definitions: {

    chunk_object: {
      fields: lambda do |_connection, _config, _object_definitions|
        [
          { name: 'chunk_id' },
          { name: 'chunk_index', type: 'integer' },
          { name: 'text' },
          { name: 'token_count', type: 'integer' },
          { name: 'start_char', type: 'integer' },
          { name: 'end_char', type: 'integer' },
          { name: 'metadata', type: 'object' }
        ]
      end
    },

    chunking_result: {
      fields: lambda do |_connection, _config, _object_definitions|
        [
          { name: 'chunks_count', type: 'integer' },
          { name: 'chunks', type: 'array', of: 'object', properties: object_definitions['chunk_object'] },
          { name: 'first_chunk', type: 'object', properties: object_definitions['chunk_object'] },
          { name: 'chunks_json' },
          { name: 'total_chunks', type: 'integer' },
          { name: 'total_tokens', type: 'integer' },
          { name: 'average_chunk_size', type: 'integer' },
          { name: 'pass_fail', type: 'boolean' },
          { name: 'action_required' },
          { name: 'telemetry', type: 'object',
            properties: [
              { name: 'success', type: 'boolean' },
              { name: 'timestamp' },
              { name: 'metadata', type: 'object' },
              { name: 'trace', type: 'object',
                properties: [
                  { name: 'correlation_id' },
                  { name: 'duration_ms', type: 'integer' }
                ]
              }
            ]
          }
        ]
      end
    },

    cleaned_text: {
      fields: lambda do
        [
          { name: 'text' },
          { name: 'removed_sections', type: 'array', of: 'string' },
          { name: 'word_count', type: 'integer' },
          { name: 'cleaning_applied', type: 'object',
            properties: [
              { name: 'source_type' },
              { name: 'task_type' },
              { name: 'operations', type: 'array', of: 'string' },
              { name: 'original_length', type: 'integer' },
              { name: 'final_length', type: 'integer' },
              { name: 'reduction_percentage', type: 'number' }
            ]
          },
          { name: 'metadata', type: 'object',
            properties: [
              { name: 'source_type' },
              { name: 'task_type' },
              { name: 'processing_timestamp' },
              { name: 'extracted_urls', type: 'array', of: 'string' }
            ]
          }
        ]
      end
    },

    email_cleaning_result: {
      fields: lambda do
        [
          { name: 'cleaned_text' },
          { name: 'extracted_query' },
          { name: 'removed_sections_count', type: 'integer' },
          { name: 'removed_sections', type: 'array', of: 'string' },
          { name: 'urls_count', type: 'integer' },
          { name: 'extracted_urls', type: 'array', of: 'string' },
          { name: 'original_length', type: 'integer' },
          { name: 'cleaned_length', type: 'integer' },
          { name: 'reduction_percentage', type: 'number' },
          { name: 'pass_fail', type: 'boolean' },
          { name: 'action_required' },
          { name: 'has_content', type: 'boolean' }
        ]
      end
    },

    classification_rules_row: {
      fields: lambda do
        [
          { name: 'rule_id' }, { name: 'rule_type' }, { name: 'rule_pattern' },
          { name: 'action' },  { name: 'priority', type: 'integer' },
          { name: 'field_matched' }, { name: 'sample' }
        ]
      end
    },

    classification_result: {
      fields: lambda do |_connection, _config, _object_definitions|
        [
          { name: 'pattern_match', type: 'boolean' },
          { name: 'rule_source' },
          { name: 'selected_action' },
          { name: 'top_match', type: 'object', properties: object_definitions['classification_rules_row'] },
          { name: 'matches', type: 'array', of: 'object', properties: object_definitions['classification_rules_row'] },
          { name: 'standard_signals', type: 'object',
            properties: [
              { name: 'sender_flags', type: 'array', of: 'string' },
              { name: 'subject_flags', type: 'array', of: 'string' },
              { name: 'body_flags',   type: 'array', of: 'string' }
            ]
          },
          { name: 'debug', type: 'object',
            properties: [
              { name: 'evaluated_rules_count', type: 'integer' },
              { name: 'schema_validated', type: 'boolean' },
              { name: 'errors', type: 'array', of: 'string' }
            ]
          },
          { name: 'telemetry', type: 'object',
            properties: [
              { name: 'success', type: 'boolean' },
              { name: 'timestamp' },
              { name: 'metadata', type: 'object' },
              { name: 'trace', type: 'object',
                properties: [
                  { name: 'correlation_id' },
                  { name: 'duration_ms', type: 'integer' }
                ]
              }
            ]
          }
        ]
      end
    },

    document_metadata: {
      fields: lambda do
        [
          { name: 'document_id' }, { name: 'file_hash' },
          { name: 'word_count', type: 'integer' },
          { name: 'character_count', type: 'integer' },
          { name: 'estimated_tokens', type: 'integer' },
          { name: 'language' }, { name: 'summary' },
          { name: 'key_topics', type: 'array', of: 'string' },
          { name: 'entities', type: 'object' },
          { name: 'created_at' },
          { name: 'processing_time_ms', type: 'integer' }
        ]
      end
    },

    change_detection: {
      fields: lambda do
        [
          { name: 'has_changed', type: 'boolean' },
          { name: 'change_type' },
          { name: 'change_percentage', type: 'number' },
          { name: 'added_content', type: 'array', of: 'string' },
          { name: 'removed_content', type: 'array', of: 'string' },
          { name: 'modified_sections', type: 'array', of: 'object',
            properties: [
              { name: 'type' },
              { name: 'current_range', type: 'array', of: 'integer' },
              { name: 'previous_range', type: 'array', of: 'integer' },
              { name: 'current_lines', type: 'array', of: 'string' },
              { name: 'previous_lines', type: 'array', of: 'string' }
            ]
          },
          { name: 'requires_reindexing', type: 'boolean' }
        ]
      end
    },

    prompt_build_result: {
      fields: lambda do
        [
          { name: 'formatted_prompt' },
          { name: 'token_count', type: 'integer' },
          { name: 'context_used', type: 'integer' },
          { name: 'truncated', type: 'boolean' },
          { name: 'prompt_metadata', type: 'object' }
        ]
      end
    },

    embedding_batch_result: {
      fields: lambda do
        [
          { name: 'batches', type: 'array', of: 'object',
            properties: [
              { name: 'batch_id' },
              { name: 'batch_number', type: 'integer' },
              { name: 'requests', type: 'array', of: 'object',
                properties: [
                  { name: 'text' },
                  { name: 'metadata', type: 'object',
                    properties: [
                      { name: 'id' }, { name: 'title' },
                      { name: 'task_type' }, { name: 'batch_id' }
                    ]
                  }
                ]
              },
              { name: 'size', type: 'integer' }
            ]
          },
          { name: 'total_batches', type: 'integer' },
          { name: 'total_texts', type: 'integer' },
          { name: 'task_type' },
          { name: 'batch_generation_timestamp' }
        ]
      end
    },

    batch_of_chunks: {
      fields: lambda do |_connection, _config, _object_definitions|
        [
          { name: 'batch_id' },
          { name: 'chunks', type: 'array', of: 'object', properties: object_definitions['chunk_object'] },
          { name: 'document_count', type: 'integer' },
          { name: 'chunk_count', type: 'integer' },
          { name: 'batch_index', type: 'integer' }
        ]
      end
    },

    prepare_document_batch_result: {
      fields: lambda do |_connection, _config, _object_definitions|
        [
          { name: 'batches', type: 'array', of: 'object', properties: object_definitions['batch_of_chunks'] },
          { name: 'summary', type: 'object',
            properties: [
              { name: 'total_documents', type: 'integer' },
              { name: 'total_chunks', type: 'integer' },
              { name: 'total_batches', type: 'integer' },
              { name: 'processing_timestamp' },
              { name: 'successful_documents', type: 'integer' },
              { name: 'failed_documents', type: 'integer' }
            ]
          },
          { name: 'failed_documents', type: 'array', of: 'object',
            properties: [
              { name: 'file_name' }, { name: 'file_id' }, { name: 'error_message' }
            ]
          },
          { name: 'telemetry', type: 'object',
            properties: [
              { name: 'success', type: 'boolean' },
              { name: 'timestamp' },
              { name: 'metadata', type: 'object' },
              { name: 'trace', type: 'object',
                properties: [
                  { name: 'correlation_id' },
                  { name: 'duration_ms', type: 'integer' }
                ]
              }
            ]
          }
        ]
      end
    },

    gcs_chunk_and_embed_result: {
      fields: lambda do |_connection, _config, _object_definitions|
        [
          { name: 'chunks', type: 'array', of: 'object', properties: object_definitions['chunk_object'] },
          { name: 'chunk_count', type: 'integer' },
          { name: 'objects_processed', type: 'integer' },
          { name: 'skipped_objects', type: 'array', of: 'object',
            properties: [
              { name: 'bucket' }, { name: 'name' }, { name: 'reason' }
            ]
          },
          { name: 'embedding', type: 'object', properties: object_definitions['embedding_batch_result'] },
          { name: 'telemetry', type: 'object',
            properties: [
              { name: 'success', type: 'boolean' },
              { name: 'timestamp' },
              { name: 'metadata', type: 'object' },
              { name: 'trace', type: 'object',
                properties: [
                  { name: 'correlation_id' },
                  { name: 'duration_ms', type: 'integer' }
                ]
              }
            ]
          }
        ]
      end
    }
  },

  # --------- ACTIONS ------------------------------------------------------
  actions: {

    prepare_for_ai: {
      title: 'Prepare text for AI',
      input_fields: lambda do
        [
          { name: 'text', optional: false, hint: 'Raw text' },
          { name: 'source_type', control_type: 'select', pick_list: [%w[email email], %w[document document], %w[chat chat], %w[general general]], optional: true },
          { name: 'task_type', optional: true }
        ]
      end,
      output_fields: lambda { call('cleaned_text') },
      execute: lambda do |input|
        started = Time.now
        src_type = input['source_type'] || 'general'
        task     = input['task_type']

        ops = []
        removed_sections = []

        raw = input['text'].to_s
        t0  = raw

        t1 = call(:util_normalize_newlines, t0)
        ops << 'normalize_newlines' if t1 != t0

        t2 = call(:util_html_to_text_minimal, t1)
        ops << 'html_to_text_minimal' if t2 != t1

        t3, removed_history = call(:util_strip_reply_history, t2)
        ops << 'strip_reply_history' unless removed_history.to_s.empty?
        removed_sections << removed_history unless removed_history.to_s.empty?

        t4, removed_sig = call(:util_strip_signature, t3)
        ops << 'strip_signature' unless removed_sig.to_s.empty?
        removed_sections << removed_sig unless removed_sig.to_s.empty?

        t5, removed_disclaimers = call(:util_strip_disclaimers, t4)
        ops << 'strip_disclaimers' if removed_disclaimers.any?
        removed_sections.concat(removed_disclaimers) if removed_disclaimers.any?

        quoted_lines = t5.scan(/^(?:\s*>.*)$/i)
        t6 = t5.gsub(/^(?:\s*>.*)$/i, '')
        ops << 'strip_quoted_lines' unless quoted_lines.empty?
        removed_sections << quoted_lines.join("\n") unless quoted_lines.empty?

        t7 = call(:util_normalize_whitespace, t6)
        ops << 'normalize_whitespace' if t7 != t6

        urls = call(:util_extract_urls_normalized, t7)
        ops << 'extract_urls' unless urls.empty?

        cleaned = t7

        out = {
          'text' => cleaned,
          'removed_sections' => removed_sections.reject(&:empty?),
          'word_count' => cleaned.split(/\s+/).reject(&:empty?).size,
          'cleaning_applied' => {
            'source_type' => src_type,
            'task_type'   => task,
            'operations'  => ops,
            'original_length' => raw.length,
            'final_length'    => cleaned.length,
            'reduction_percentage' => (raw.length.zero? ? 0.0 : (100.0 * (raw.length - cleaned.length) / raw.length)).round(2),
            'telemetry' => call(:telemetry, true, started, { 'stage' => 'clean_email_text' })

          },
          'metadata' => {
            'source_type' => src_type,
            'task_type'   => task,
            'processing_timestamp' => call(:now_iso),
            'extracted_urls' => urls
          }
        }
        out
      end
    },

    clean_email_text: {
      title: 'Clean email text',
      input_fields: lambda do
        [{ name: 'text', optional: false }]
      end,
      output_fields: lambda do
        call('email_cleaning_result')
      end,
      execute: lambda do |input|
        started = Time.now
        raw = input['text'].to_s

        # 1) normalize newlines early
        step1 = call(:util_normalize_newlines, raw)

        # 2) minimal HTML → text (preserve <user@domain>)
        html_removed = call(:util_html_to_text_minimal, step1)

        # 3) drop quoted history
        no_history, removed_history = call(:util_strip_reply_history, html_removed)

        # 4) drop signature (conservative)
        no_sig, removed_sig = call(:util_strip_signature, no_history)

        # 5) drop common disclaimers
        sans_disclaimers, removed_disclaimers = call(:util_strip_disclaimers, no_sig)

        # 6) remove conventional quoted lines beginning with '>'
        quoted_lines = sans_disclaimers.scan(/^(?:\s*>.*)$/i)
        no_quote_lines = sans_disclaimers.gsub(/^(?:\s*>.*)$/i, '')

        # 7) whitespace normalization
        cleaned = call(:util_normalize_whitespace, no_quote_lines)

        # 8) URLs
        urls = call(:util_extract_urls_normalized, cleaned)

        removed_sections = []
        removed_sections << removed_history unless removed_history.empty?
        removed_sections << removed_sig     unless removed_sig.empty?
        removed_sections << quoted_lines.join("\n") unless quoted_lines.empty?
        removed_sections.concat(removed_disclaimers) if removed_disclaimers.any?

        out = {
          'cleaned_text'          => cleaned,
          'extracted_query'       => cleaned.split(/\n{2,}/).first.to_s[0, 600],
          'removed_sections_count'=> removed_sections.reject(&:empty?).size,
          'removed_sections'      => removed_sections.reject(&:empty?),
          'urls_count'            => urls.size,
          'extracted_urls'        => urls,
          'original_length'       => raw.length,
          'cleaned_length'        => cleaned.length,
          'reduction_percentage'  => (raw.length.zero? ? 0.0 : (100.0 * (raw.length - cleaned.length) / raw.length)).round(2),
          'pass_fail'             => cleaned.length.positive?,
          'action_required'       => cleaned.length.positive? ? '' : 'no_content',
          'has_content'           => cleaned.strip.length.positive?,
          'telemetry'             => call(:telemetry, true, started, { 'stage' => 'clean_email_text' })

        }
        out
      end

    },

    smart_chunk_text: {
      title: 'Chunk text (size + overlap)',
      input_fields: lambda do
        [
          { name: 'text', optional: false },
          { name: 'chunk_size', type: 'integer', optional: true, hint: 'default 1200' },
          { name: 'overlap', type: 'integer', optional: true, hint: 'default 200' }
        ]
      end,
      output_fields: lambda { call('chunking_result') },
      execute: lambda do |input|
        started = Time.now
        size = call(:int, input['chunk_size'], 1200)
        ov   = call(:int, input['overlap'], 200)
        chunks = call(:chunk_text_smart, input['text'].to_s, size, ov)
        total_tokens = chunks.sum { |c| c['token_count'].to_i }
        avg_size = chunks.empty? ? 0 : (chunks.sum { |c| c['text'].length } / chunks.size.to_f).round
        {
          'chunks_count' => chunks.size,
          'chunks' => chunks,
          'first_chunk' => chunks.first,
          'chunks_json' => JSON.dump(chunks),
          'total_chunks' => chunks.size,
          'total_tokens' => total_tokens,
          'average_chunk_size' => avg_size,
          'pass_fail' => chunks.size.positive?,
          'action_required' => chunks.size.positive? ? '' : 'input_too_small',
          'telemetry' => call(:telemetry, true, started, { 'chunk_size' => size, 'overlap' => ov })
        }
      end
    },

    process_document_for_rag: {
      title: 'Process single document for RAG',
      input_fields: lambda do
        [
          { name: 'document_id', optional: false },
          { name: 'text', optional: false },
          { name: 'chunk_size', type: 'integer', optional: true },
          { name: 'overlap', type: 'integer', optional: true }
        ]
      end,
      output_fields: lambda do
        [
          { name: 'document_id' },
          { name: 'chunks', type: 'array', of: 'object', properties: object_definitions['chunk_object'] },
          { name: 'document_metadata', type: 'object',
            properties: [
              { name: 'total_chunks', type: 'integer' },
              { name: 'total_characters', type: 'integer' },
              { name: 'total_words', type: 'integer' },
              { name: 'processing_timestamp' },
              { name: 'chunk_size_used', type: 'integer' },
              { name: 'overlap_used', type: 'integer' }
            ]
          },
          { name: 'ready_for_embedding', type: 'boolean' }
        ]
      end,
      execute: lambda do |input|
        started = Time.now
        size    = call(:int, input['chunk_size'], 1200)
        ov      = call(:int, input['overlap'], 200)
        chunks  = call(:chunk_text_smart, input['text'].to_s, size, ov)
        {
          'document_id'         => (input['document_id'] || call(:generate_document_id, 
                                                              input.dig('file_metadata','file_name') || 'doc',
                                                              input.dig('file_metadata','checksum') || 'no_checksum')),
          'chunks'              => chunks,
          'document_metadata'   => {
            'total_chunks' => chunks.size,
            'total_characters' => input['text'].to_s.length,
            'total_words' => input['text'].to_s.split(/\s+/).reject(&:empty?).size,
            'processing_timestamp' => call(:now_iso),
            'chunk_size_used' => size,
            'overlap_used' => ov },
          'ready_for_embedding' => chunks.size.positive?
        }
      end
    },

    prepare_document_batch: {
      title: 'Prepare batch of documents (chunk + summarize)',
      input_fields: lambda do
        [
          { name: 'documents', label: 'Documents', type: 'array', of: 'object', properties: [
              { name: 'document_id' }, { name: 'text' }
            ], optional: false },
          { name: 'chunk_size', type: 'integer', optional: true },
          { name: 'overlap', type: 'integer', optional: true },
          { name: 'batch_size', type: 'integer', optional: true, hint: 'default 100 chunks' }
        ]
      end,
      output_fields: lambda { call('prepare_document_batch_result') },
      execute: lambda do |input|
        started = Time.now
        size    = call(:int, input['chunk_size'], 1200)
        ov      = call(:int, input['overlap'], 200)
        bsz     = call(:int, input['batch_size'], 100)

        all_chunks  = []
        failed      = []

        Array(input['documents']).each do |doc|
          begin
            txt     = doc['text'].to_s
            next if txt.empty?
            cs      = call(:chunk_text_smart, txt, size, ov)
            doc_id  = call(:generate_document_id,
                          doc.dig('file_metadata', 'file_name') || 'doc',
                          doc.dig('file_metadata', 'checksum') || 'no_checksum')
            cs.each { |c| c['metadata']['document_id'] = doc_id }
            all_chunks.concat(cs)
          rescue => e
            fm = doc['file_metadata'] || {}
            failed << { 'file_name' => fm['file_name'], 'file_id' => fm['file_id'], 'error_message' => e.message }
          end
        end

        batches = []
        all_chunks.each_slice(bsz).with_index do |slice, i|
          batches << {
            'batch_id'        => SecureRandom.uuid,
            'chunks'          => slice,
            'document_count'  => slice.map { |c| c.dig('metadata', 'document_id') }.uniq.compact.size,
            'chunk_count'     => slice.size,
            'batch_index'     => i
          }
        end

        {
          'batches'          => batches,
          'summary'          => {
            'total_documents'       => Array(input['documents']).size,
            'total_chunks'          => all_chunks.size,
            'total_batches'         => batches.size,
            'processing_timestamp'  => call(:now_iso),
            'successful_documents'  => Array(input['documents']).size - failed.size,
            'failed_documents'      => failed.size },
          'failed_documents' => failed,
          'telemetry'        => call(:telemetry, true, started, { 'chunk_size' => size, 'overlap' => ov, 'batch_size' => bsz })
        }
      end
    },

    prepare_embedding_batch: {
      title: 'Prepare embedding batch',
      input_fields: lambda do
        [
          { name: 'texts', type: 'array', of: 'string', optional: false },
          { name: 'task_type', control_type: 'select',
            pick_list: [%w[RETRIEVAL_DOCUMENT RETRIEVAL_DOCUMENT], %w[QUERY QUERY], %w[SEMANTIC_SIMILARITY SEMANTIC_SIMILARITY]],
            optional: false },
          { name: 'batch_size', type: 'integer', optional: true, hint: 'default 100' }
        ]
      end,
      output_fields: lambda { call('embedding_batch_result') },
      execute: lambda do |input|
        bsz   = call(:int, input['batch_size'], 100)
        tts   = input['task_type']
        reqs  = Array(input['texts']).map { |t| { 'text' => t.to_s, 'metadata' => { 'id' => SecureRandom.uuid, 'task_type' => tts } } }
        batches = []
        reqs.each_slice(bsz).with_index do |slice, i|
          bid = SecureRandom.uuid
          slice.each { |r| r['metadata']['batch_id'] = bid }
          batches << { 'batch_id' => bid, 'batch_number' => i + 1, 'requests' => slice, 'size' => slice.size }
        end
        {
          'batches'       => batches,
          'total_batches' => batches.size,
          'total_texts'   => reqs.size,
          'task_type'     => tts,
          'batch_generation_timestamp' => call(:now_iso)
        }
      end
    },

    classify_by_pattern: {
      title: 'Classify by pattern rules',
      input_fields: lambda do
        [
          { name: 'sender' }, { name: 'subject' }, { name: 'body' },
          { name: 'rules', type: 'array', of: 'object', properties: [
              { name: 'rule_id' }, { name: 'rule_type' },
              { name: 'rule_pattern' }, { name: 'action' },
              { name: 'priority', type: 'integer' }, { name: 'sample' }
            ], optional: true }
        ]
      end,
      output_fields: lambda { call('classification_result') },
      execute: lambda do |input|
        started = Time.now
        rules   = Array(input['rules']).compact
        matches = []
        %w[sender subject body].each do |f|
          val = input[f].to_s
          next if val.empty?
          rules.each do |r|
            next unless r['rule_type'] == f || r['rule_type'].nil?
            begin
              rx = Regexp.new(r['rule_pattern'].to_s, Regexp::IGNORECASE)
              if val.match?(rx)
                matches << r.merge('field_matched' => f)
              end
            rescue
              # skip bad regex
            end
          end
        end
        top = matches.sort_by { |m| -(m['priority'] || 0) }.first
        {
          'pattern_match'     => matches.any?,
          'rule_source'       => matches.any? ? 'custom' : 'none',
          'selected_action'   => top && top['action'],
          'top_match'         => top,
          'matches'           => matches,
          'standard_signals'  => { 'sender_flags' => [], 'subject_flags' => [], 'body_flags' => [] },
          'debug'             => { 'evaluated_rules_count' => rules.size, 'schema_validated' => true, 'errors' => [] },
          'telemetry'         => call(:telemetry, true, started)
        }
      end
    },

    generate_document_metadata: {
      title: 'Generate document metadata',
      input_fields: lambda do
        [
          { name: 'document_id', optional: false },
          { name: 'text', optional: false }
        ]
      end,

      output_fields: lambda do |_connection, _config, _object_definitions|
        object_definitions['document_metadata']
      end,

      execute: lambda do |input|
        started = Time.now
        txt = input['text'].to_s
        doc_id = input['document_id'] || call(:generate_document_id, input['file_name'] || 'doc', Digest::SHA256.hexdigest(txt))
        {
          'document_id'       => doc_id,
          'file_hash'         => Digest::SHA256.hexdigest(txt),
          'word_count'        => txt.split(/\s+/).reject(&:empty?).size,
          'character_count'   => txt.length,
          'estimated_tokens'  => call(:est_tokens, txt),
          'language'          => nil,
          'summary'           => nil,
          'key_topics'        => [],
          'entities'          => {},
          'created_at'        => call(:now_iso),
          'processing_time_ms'=> ((Time.now - started) * 1000).round
        }
      end
    },

    check_document_changes: {
      title: 'Check document changes',

      input_fields: lambda do
        [
          { name: 'previous_text', optional: true },
          { name: 'current_text', optional: false }
        ]
      end,

      output_fields: lambda do |_connection, _config, _object_definitions|
        object_definitions['change_detection']
      end,

      execute: lambda do |input|
        prev = input['previous_text'].to_s
        curr = input['current_text'].to_s
        type = (input['check_type'] || 'hash').to_s

        if type == 'content' && !prev.empty?
          diff = call(:util_diff_lines, curr, prev)
          changed = diff[:added].any? || diff[:removed].any? || diff[:modified_sections].any?
          return {
            'has_changed'         => changed,
            'change_type'         => changed ? 'content_changed' : 'none',
            'change_percentage'   => diff[:line_change_percentage],
            'added_content'       => diff[:added],
            'removed_content'     => diff[:removed],
            'modified_sections'   => diff[:modified_sections],
            'requires_reindexing' => changed
          }
        end

        prev_h = Digest::SHA256.hexdigest(prev)
        curr_h = Digest::SHA256.hexdigest(curr)
        changed = prev_h != curr_h
        {
          'has_changed'         => changed,
          'change_type'         => changed ? 'hash_changed' : 'none',
          'change_percentage'   => changed ? 100.0 : 0.0,
          'added_content'       => [],
          'removed_content'     => [],
          'modified_sections'   => [],
          'requires_reindexing' => changed
        }
      end
    },

    chunk_gcs_batch_for_embedding: {
      title: 'Chunk "GCS" batch (pass-in text) + make embedding batches',
      help: 'Minimal, side-effect free: pass objects with inline text to avoid GCS calls.',

      input_fields: lambda do
        [
          { name: 'objects', type: 'array', of: 'object', properties: [
              { name: 'bucket' }, { name: 'name' }, { name: 'text' }
            ], optional: false },
          { name: 'chunk_size', type: 'integer', optional: true },
          { name: 'overlap', type: 'integer', optional: true },
          { name: 'embed_task_type', control_type: 'select',
            pick_list: [%w[RETRIEVAL_DOCUMENT RETRIEVAL_DOCUMENT], %w[QUERY QUERY], %w[SEMANTIC_SIMILARITY SEMANTIC_SIMILARITY]],
            optional: false },
          { name: 'embed_batch_size', type: 'integer', optional: true }
        ]
      end,

      output_fields: lambda do |_connection, _config, _object_definitions|
        object_definitions['gcs_chunk_and_embed_result']
      end,

      execute: lambda do |input|
        started = Time.now
        size    = call(:int, input['chunk_size'], 1200)
        ov      = call(:int, input['overlap'], 200)
        ebs     = call(:int, input['embed_batch_size'], 100)

        all_chunks  = []
        skipped     = []
        processed   = 0

        Array(input['objects']).each do |o|
          txt = o['text'].to_s
          if txt.strip.empty?
            skipped << { 'bucket' => o['bucket'], 'name' => o['name'], 'reason' => 'empty_text' }
            next
          end
          processed += 1
          doc_id = call(:generate_document_id, "#{o['bucket']}/#{o['name']}", o['checksum'] || 'no_checksum')
          cs = call(:chunk_text_smart, txt, size, ov)
          cs.each do |c|
            c['metadata'].merge!({ 'bucket' => o['bucket'], 'name' => o['name'], 'document_id' => doc_id })
          end
          all_chunks.concat(cs)
        end

        reqs = all_chunks.map do |c|
          { 'text' => c['text'], 'metadata' => { 'id' => c['chunk_id'], 'task_type' => input['embed_task_type'] } }
        end
        batches = []
        reqs.each_slice(ebs).with_index do |slice, i|
          bid = SecureRandom.uuid
          slice.each { |r| r['metadata']['batch_id'] = bid }
          batches << { 'batch_id' => bid, 'batch_number' => i + 1, 'requests' => slice, 'size' => slice.size }
        end

        {
          'chunks'            => all_chunks,
          'chunk_count'       => all_chunks.size,
          'objects_processed' => processed,
          'skipped_objects'   => skipped,
          'embedding'         => {
            'batches'                    => batches,
            'total_batches'              => batches.size,
            'total_texts'                => reqs.size,
            'task_type'                  => input['embed_task_type'],
            'batch_generation_timestamp' => call(:now_iso) },
          'telemetry'         => call(:telemetry, true, started, { 
                                  'chunk_size'        => size,
                                  'overlap'           => ov, 
                                  'embed_batch_size'  => ebs })
        }
      end
    },

    adapt_chunks_for_vertex: {
      title: 'Adapt chunks → Vertex records',
      input_fields: lambda do
        [{ name: 'chunks', type: 'array', of: 'object', properties: object_definitions['chunk_object'], optional: false }]
      end,
      output_fields: lambda do
        [
          { name: 'records', type: 'array', of: 'object',
            properties: [
              { name: 'id' }, { name: 'text' },
              { name: 'metadata', type: 'object' }
            ] },
          { name: 'count', type: 'integer' }
        ]
      end,
      execute: lambda do |input|
        recs = Array(input['chunks']).map { |c| { 'id' => c['chunk_id'], 'text' => c['text'], 'metadata' => (c['metadata'] || {}) } }
        { 'records' => recs, 'count' => recs.size }
      end
    },

    serialize_chunks_to_jsonl: {
      title: 'Serialize chunks/records → JSONL',
      input_fields: lambda do
        [
          { name: 'records', type: 'array', of: 'object', properties: [
              { name: 'id' }, { name: 'text' }, { name: 'metadata', type: 'object' }
            ], optional: false }
        ]
      end,
      output_fields: lambda do
        [
          { name: 'jsonl' },
          { name: 'lines', type: 'integer' }
        ]
      end,
      execute: lambda do |input|
        lines = Array(input['records']).map { |r| JSON.dump(r) }
        { 'jsonl' => lines.join("\n"), 'lines' => lines.size }
      end
    }
  }
}
