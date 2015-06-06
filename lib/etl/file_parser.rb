require 'csv'

module ETL
  class FileParser
    attr_reader :filename, :data

    def initialize(filename)
      @filename = filename
      @data = ::CSV.read(filename.to_s)
    end


    def clean_data
      raise NotImplementedError
    end

  end
end
