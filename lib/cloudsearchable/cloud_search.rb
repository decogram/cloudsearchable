require 'aws-sdk'
require 'json'
require 'cloudsearchable/signer'

module CloudSearch
  API_VERSION = "2013-01-01"

  def self.client
    @client ||= Aws::CloudSearch::Client.new
  end

  def self.client=(client)
    @client = client
  end

  #
  # Send an SDF document to CloudSearch via http post request.
  # Returns parsed JSON response, or raises an exception
  #
  def self.post_sdf endpoint, sdf
    self.post_sdf_list endpoint, [sdf]
  end

  def self.post_sdf_list endpoint, sdf_list
    uri = URI.parse("https://#{endpoint}/#{API_VERSION}/documents/batch")
    body = JSON.generate sdf_list
    response = CloudSearchable::AwsSigner.send_signed_request("POST", uri, body)
    if response.is_a? Net::HTTPSuccess
      JSON.parse response.body
    else
      # Raise an exception based on the response see http://ruby-doc.org/stdlib-1.9.2/libdoc/net/http/rdoc/Net/HTTP.html
      response.error!
    end
  end
end
