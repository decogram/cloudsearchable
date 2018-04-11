module Cloudsearchable
  class NoClausesError < StandardError; end
  class WarningInQueryResult < StandardError; end

  #
  # An object that represents a query to cloud search
  #
  class QueryChain
    include Enumerable

    attr_reader :domain, :fields

    # options:
    #   - fatal_warnings: if true, raises a WarningInQueryResult exception on warning. Defaults to false
    def initialize(domain, options = {})
      @fatal_warnings = options.fetch(:fatal_warnings, false)
      @domain         = domain
      @q              = "matchall"
      @clauses        = []
      @sort           = nil
      @limit          = 10000 # 10 is the CloudSearch default, 2kb limit will probably hit before this will
      @offset         = nil
      @fields         = Set.new
      @results        = nil
      @parser         = "structured"
      @location       = nil
    end

    #
    # This method can be called in several different forms.
    #
    # To do an equality search on several fields, you can pass a single hash, e.g.:
    #
    #   Collection.search.where(customer_id: "12345", another_field: "Some value")
    #
    # To do a search on a single field, you can pass three parameters in the
    # form: where(field, op, value)
    #
    #   Collection.search.where(:customer_id, :==, 12345)
    #
    # The value you provide must be of the same type as the field.  For text and literal
    # values, provide a string value.  For int fields, provide a numeric value.
    #
    # To search for any of several possible values for a field, use the :any operator:
    #
    #   Collection.search.where(:product_group, :any, %w{gl_kitchen gl_grocery})
    #
    # Equality and inequality operators (:==, :!=, :<, :<=, :>, :>=) are supported on
    # integers, and equality operators are supported on all scalars.
    # Currently, special operators against arrays (any and all) are not yet implemented.
    #
    def where(field_or_hash, op = nil, value = nil)
      raise if materialized?

      if field_or_hash.is_a? Hash
        field_or_hash.each_pair do |k, v|
          where(k, :==, v)
        end
      elsif field_or_hash.is_a? Symbol
        if (field = domain.fields[field_or_hash.to_sym]).nil?
          raise "cannot query on field '#{field_or_hash}' because it is not a member of this index"
        end
        @clauses << clause_for(field_or_hash, field.type, op, value)
      else
        raise "field_or_hash must be a Hash or Symbol, not a #{field_or_hash.class}"
      end

      self
    end

    #
    # Allows searching by text, overwriting any existing text search.
    #
    #   Collection.search.text('mens shoes')
    #
    # For more examples see http://docs.aws.amazon.com/cloudsearch/latest/developerguide/searching.text.html
    #
    def text(text)
      raise if materialized?
      search_terms = text.split(/\W+/).map {|word| "'#{word}'"}.join(" ")
      @q = "(or #{search_terms})"
      self
    end

    def plain_text(text)
      raise if materialized?
      @q = text
      self
    end

    #
    # Set a rank expression on the query, overwriting any existing expression. Defaults to "-text_relevance"
    #
    #   Collection.search.order('created_at')  # order by the created_at field ascending
    #   Collection.search.order('-created_at') # descending order
    #
    # For more examples see http://docs.amazonwebservices.com/cloudsearch/latest/developerguide/tuneranking.html
    #
    def order rank_expression
      raise if materialized?
      raise "order clause must be a string, not a #{rank_expression.class}" unless rank_expression.is_a? String
      @sort = rank_expression.to_s
      self
    end

    #
    # Limit the number of results returned from query to the given count.
    #
    #   Collection.search.limit(25)
    #
    def limit count
      raise if materialized?
      raise "limit value must be must respond to to_i, #{count.class} does not" unless count.respond_to? :to_i
      @limit = count.to_i
      self
    end

    #
    # Offset the results returned by the query by the given count.
    #
    #   Collection.search.offset(250)
    #
    def offset count
      raise if materialized?
      raise "limit value must be must respond to to_i, #{count.class} does not" unless count.respond_to? :to_i
      @offset = count.to_i
      self
    end

    #
    # Adds a one or more fields to the returned result set, e.g.:
    #
    #   my_query.returning(:collection_id)
    #   my_query.returning(:collection_id, :created_at)
    #
    #   x = [:collection_id, :created_at]
    #   my_query.returning(x)
    #
    def returning(*fields)
      raise if materialized?

      fields.flatten!
      fields.each do |f|
        @fields << f
      end
      self
    end

    #
    # True if the query has been materialized (e.g. the search has been
    # executed).
    #
    def materialized?
      !@results.nil?
    end

    #
    # Executes the query, getting a result set, returns true if work was done,
    # false if the query was already materialized.
    # Raises exception if there was a warning and not in production.
    #
    def materialize!
      return false if materialized?

      @results = domain.execute_query(to_q)

      if @results && @results["info"] && messages = @results["info"]["messages"]
        messages.each do |message|
          if message["severity"] == "warning"
            Cloudsearchable.logger.warn "Cloud Search Warning: #{message["code"]}: #{message["message"]}"
            raise(WarningInQueryResult, "#{message["code"]}: #{message["message"]}") if @fatal_warnings
          end
        end
      end

      true
    end

    def found_count
      materialize!
      if @results['hits']
        @results['hits']['found']
      else
        raise "improperly formed response. hits parameter not available. messages: #{@results["messages"]}"
      end
    end
    alias_method :total_count, :found_count

    def facet_values_for(index)
      materialize!
      if index == "latlon"
        buckets = [
          {
            value: "0-5 miles",
            count: 0
          },
          {
            value: "5-10 miles",
            count: 0
          },
          {
            value: "10-15 miles",
            count: 0
          },
          {
            value: "15-20 miles",
            count: 0
          },
          {
            value: "20+ miles",
            count: 0
          }
        ]
        self.each do |result_hit|
          distance = result_hit['exprs']["distance"].to_i
          if distance < 5
            buckets[0][:count] += 1
          elsif distance < 10
            buckets[1][:count] += 1
          elsif distance < 15
            buckets[2][:count] += 1
          elsif distance < 20
            buckets[3][:count] += 1
          else
            buckets[4][:count] += 1
          end
        end
        buckets
      else
        if @results['facets']
          if @results['facets'][index]
            @results['facets'][index]['buckets']
          else
            raise "Facet for #{index} unavailable."
          end
        else
          raise "improperly formed response. Facets parameter not available. messages: #{@results["messages"]}"
        end
      end
    end
    def set_location(location)
      @location = location
    end



    def each(&block)
      materialize!
     if @results['hits']
       @results['hits']['hit'].each(&block)
     else
       raise "improperly formed response. hits parameter not available. messages: #{@results["messages"]}"
     end
    end

    #
    # Turns this Query object into a query string hash that goes on the CloudSearch URL
    #
    def to_q
      raise NoClausesError, "no search terms were specified" if (@clauses.nil? || @clauses.empty?) && (@q.nil? || @q.empty?)

      fq = (@clauses.count > 1) ? "(and #{@clauses.join(' ')})" : @clauses.first
      base_query =
      {
        q: @q,
        return: @fields.reduce("") { |s,f| s << f.to_s },
        size: @limit,
        "q.parser" => @parser
      }
      if fq
        base_query[:fq] = fq
      end
      if @sort
        base_query[:sort] = @sort
      end
      if @offset
        base_query[:start] = @offset
      end


      base_query = add_facet_clause(base_query)

    end

    private

    def clause_for(field, type, op, value)
      # Operations for which 'value' is not a scalar
      if op == :any
        '(or ' + value.map { |v| "#{field}:#{query_clause_value(type, v)}" }.join(' ') + ')'
      elsif op == :within_range && (type == :int || type == :date)
        #needs to follow cloudsearch range definitions
        "(range field=#{field} #{value.to_s})"
      elsif op == :not_within_range && (type == :int || type == :date)
        #needs to follow cloudsearch range definitions
        "(not (range field=#{field} #{value.to_s}))"
      elsif op == :prefixed_with
        "(prefix field=#{field} '#{value.to_s}')"
      else
        value = query_clause_value(type, value)

        # Some operations are applicable to all types.
        case op
          when :==, :eq
            "#{field}:#{value}"
          when :!=
            "(not #{field}:#{value})"
          else
            # Operation-specific, type-specific operations on scalars
            case type
              when :int, :date
                case op
                  when :>
                    "#{field}:#{value+1}.."
                  when :<
                    "#{field}:..#{value-1}"
                  when :>=
                    "#{field}:#{value}.."
                  when :<=
                    "#{field}:..#{value}"
                  else
                    raise "op #{op} is unrecognized for value #{value} of type #{type}"
                end
              else
                raise "op #{op} is unrecognized for value #{value} of type #{type}"
            end
        end
      end
    end
    def add_facet_clause(base_query)

      domain.fields.each do |key, value|
        if value.type == :latlon && value.options[:facet_enabled] == true && @location != nil
          base_query['expr.distance'] = "haversin(#{@location[0]}, #{@location[1]}, location.latitude, location.longitude)*0.621371"
          base_query[:return] += ",distance"
        elsif value.type != :latlon && value.options[:facet_enabled] == true
          base_query["facet.#{key}"] = {}
        end
      end
      base_query
    end

    def query_clause_value(type, value)
      if type == :int
        Integer(value)
      elsif !value.nil?
        "'#{value.to_s}'"
      else
        raise "Value #{value} cannot be converted to query string on type #{type}"
      end
    end
  end
end
