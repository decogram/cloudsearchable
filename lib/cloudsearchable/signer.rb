module CloudSearch
  module AwsSigner
    def self.send_signed_request(method, uri, region, body, params )
      service = 'cloudsearch'
      endpoint = "https://#{uri.host}#{uri.path}"
      request_parameters = params

      host = uri.host

      t = Time.now.utc
      amzdate = t.strftime('%Y%m%dT%H%M%SZ')
      datestamp = t.strftime('%Y%m%d')

      access_key = Cloudsearchable::Config.aws_access_key
      secret_key = Cloudsearchable::Config.aws_secret_key
      if method == "GET"
        content_type = 'application/x-www-form-urlencoded; charset=utf-8'
      else
        content_type = "application/json"
      end

      # Task 1: Create a Canonical Request For Signature Version 4
      # http://docs.aws.amazon.com/general/latest/gr/sigv4-create-canonical-request.html
      canonical_uri = uri.path
      signed_headers = 'content-type;host;x-amz-content-sha256;x-amz-date'
      payload_hash = OpenSSL::Digest.new("sha256").hexdigest(body)
      canonical_headers = ["content-type: #{content_type}",
                           'host:' + host, "x-amz-content-sha256:#{payload_hash}",
                           'x-amz-date:' + amzdate].join("\n") + "\n"

      canonical_request = [method, canonical_uri, request_parameters, canonical_headers,
                           signed_headers, payload_hash].join("\n")

      puts canonical_request
      puts ""



      # Task 2: Create a String to Sign for Signature Version 4
      # http://docs.aws.amazon.com/general/latest/gr/sigv4-create-string-to-sign.html
      algorithm = 'AWS4-HMAC-SHA256'
      credential_scope = [datestamp, region, service, 'aws4_request'].join("/")
      string_to_sign = [
        algorithm, amzdate, credential_scope,
        OpenSSL::Digest.new("sha256").hexdigest(canonical_request)
      ].join("\n")
      puts string_to_sign
      puts ""

      # Task 3: Calculate the AWS Signature Version 4
      # http://docs.aws.amazon.com/general/latest/gr/sigv4-calculate-signature.html
      signing_key = getSignatureKey(secret_key, datestamp, region, service)


      # Task 4: Add the Signing Information to the Request
      # http://docs.aws.amazon.com/general/latest/gr/sigv4-add-signature-to-request.html
      signature = OpenSSL::HMAC.hexdigest('sha256', signing_key, string_to_sign)

      https = Net::HTTP.new(uri.host,uri.port)
      https.use_ssl = true
      if method == "GET"
        request = Net::HTTP::Get.new("#{canonical_uri}#{'?' + request_parameters}")
      else
        request = Net::HTTP::Post.new("#{canonical_uri}" )
        request.body = body
      end

      auth = "#{algorithm} Credential=#{access_key + '/' + credential_scope}, SignedHeaders=#{signed_headers}, Signature=#{signature}"

      request.add_field 'Content-Type', content_type
      request.add_field 'X-Amz-Date', amzdate
      request.add_field 'X-Amz-Content-Sha256', payload_hash
      request.add_field 'Authorization', auth
      res = https.request(request)


      Cloudsearchable.logger.info "CloudSearch execute: #{uri.to_s}"

      puts "#{res.code} #{res.body} #{res.message}"

      return res
    end
    def self.getSignatureKey key, dateStamp, regionName, serviceName
      kDate    = OpenSSL::HMAC.digest('sha256', "AWS4" + key, dateStamp)
      kRegion  = OpenSSL::HMAC.digest('sha256', kDate, regionName)
      kService = OpenSSL::HMAC.digest('sha256', kRegion, serviceName)
      kSigning = OpenSSL::HMAC.digest('sha256', kService, "aws4_request")

      kSigning
    end
  end
end
