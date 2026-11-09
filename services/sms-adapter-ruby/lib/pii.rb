# PII guardrail: anything that leaves this process as a log line goes through here.
#
# Why not hash the body? OTP bodies are short ("Your code is 482913"), so a hash
# can be brute-forced back to the code. Length is enough to debug with.
module PII
  module_function

  def mask_recipient(value)
    s = value.to_s
    return "" if s.empty?

    if s.include?("@")
      local, domain = s.split("@", 2)
      "#{local[0]}***@#{domain}"
    else
      last4 = s[-4..] || s
      "***#{last4}"
    end
  end

  def mask_body(value)
    "[redacted len=#{value.to_s.length}]"
  end
end
