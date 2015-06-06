module ETL
  class SafeHealthyParser < FileParser
    BASE_START_INDEX = 10

    attr_accessor :start_index

    def initialize
      filename = Rails.root.join('safe_healthy_211.csv')
      super(filename)
    end

    def clean_data
      header = clean_data_header()
      clean_data = []

      data[1..-1].each do |row|
        self.start_index = BASE_START_INDEX

        clean_data << Hash[header.map do |column|
                             key = column.to_sym
                             value = if array_valued?(column)
                                       get_value_array(row, column)
                                     else
                                       get_value(row, column)
                                     end


                             [ key , value ]
                           end ]
      end

      return clean_data
    end

    def to_csv
      CSV.open(output_file_name, "wb") do |csv|
        csv << clean_data_header

        clean_data.each do |data|
          data[:target_type] = nil if data[:target_type].is_a?(Array)
          csv << data.values
        end
      end
    end

    private

    def output_file_name
      base_name = "clean_data_#{filename.basename}"
      return File.join(filename.dirname, base_name)
    end

    def clean_data_header
      return %w{ name address citystate zip phone web email
                 target_type target_value program_type category subcategory alt_name }
    end

    def get_value(row, column)
      row[start_index..-1].each_with_index do |entry, i|
        if entry =~ valid_regex(column)
          next if invalid_values(column).member?(entry)

          self.start_index += ( i + 1 )
          return entry.strip

        elsif i > max_range(column)
          self.start_index += 1
          return nil
        end
      end
    end

    def get_value_array(row, column)
      value_array = []

      row[start_index..-1].each_with_index do |entry, i|
        unless i > max_range(column)
          if entry =~ valid_regex(column) && !invalid_values(column).member?(entry)
            value_array << entry.strip
          end
        end
      end

      self.start_index += value_array.size
      return value_array
    end

    def valid_regex(column)
      valid_regex = case column.to_sym
                    when :name then /^[A-Z][-_,0-9A-Z ]+$/
                    when :address then /^[0-9]+ \w+/
                    when :citystate then /^[, \w]+ [A-Z]{2}/
                    when :zip then /^[0-9]{5}/
                    when :phone then /\([0-9]{3}\) [0-9]{3}-[0-9]{4}/
                    when :web then /^https?:\/\/.*/
                    when :email then /mailto:.*@.*/
                    when :target_type then /^Target%20Populations/
                    when :target_value then /.*/
                    when :program_type then /.*%20.*/
                    when :category then /^[A-Z][A-Z\/ ]+$/
                    when :subcategory then /^[A-Z][()0-9A-Z ]+$/
                    when :alt_name then /^[A-Z][-_\/(),0-9A-Z ]+$/
                    else
                      raise "Unknown column type: #{column}"
                    end

      return valid_regex
    end

    def invalid_values(column)
      invalid_values = case column.to_sym
                       when :name then [ 'N' ]
                       else
                         return Array.new
                       end

      return invalid_values
    end

    def max_range(column)
      case column.to_sym
      when :address then 1
      when :citystate then 1
      when :zip then 1
      when :phone then 5
      when :web then 1
      when :email then 1
      when :target_value then 1
      when :program_type then 2
      when :category then 5
      when :subcategory then 1
      when :alt_name then 1
      else
        Float::INFINITY
      end
    end

    def array_valued?(column)
      return ( column.to_sym == :phone ) ? true : false
    end

  end
end
