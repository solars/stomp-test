$:.unshift(File.dirname(__FILE__))
#
# Test Ruby 1.8 with $KCODE='U'
#
require 'test_helper'
#
class TestMessageKcode < Test::Unit::TestCase
  include TestBase
  #
  def setup
		$KCODE = 'U' if RUBY_VERSION =~ /1\.8/
    @conn = Stomp::Connection.open(user, passcode, host, port)
    # Message body data
		@messages = [
			"normal text message",
			"bad byte: \372",
			"\004\b{\f:\tbody\"\001\207\004\b{\b:\016statusmsg\"\aOK:\017statuscodei\000:\tdata{\t:\voutput\"3Enabled, not running, last run 693 seconds ago:\frunningi\000:\fenabledi\006:\flastrunl+\aE\021\022M:\rsenderid\"\032xx.xx.xx.xx:\016requestid\"%849d647bbe3e421ea19ac9f947bbdde4:\020senderagent\"\fpuppetd:\016msgtarget\"%/topic/mcollective.puppetd.reply:\thash\"\001\257ZdQqtaDmmdD0jZinnEcpN+YbkxQDn8uuCnwsQdvGHau6d+gxnnfPLUddWRSb\nZNMs+sQUXgJNfcV1eVBn1H+Z8QQmzYXVDMqz7J43jmgloz5PsLVbN9K3PmX/\ngszqV/WpvIyAqm98ennWqSzpwMuiCC4q2Jr3s3Gm6bUJ6UkKXnY=\n:\fmsgtimel+\a\372\023\022M"
		]
		#
  end

  def teardown
    @conn.disconnect if @conn # allow tests to disconnect
  end

	# Various message bodies, including the failing test case reported
  def test_kcode_001
		#
		dest = make_destination
    @conn.subscribe dest
		@messages.each do |abody|
		  @conn.publish dest, abody
			msg = @conn.receive
			assert_instance_of Stomp::Message , msg, "type check for #{abody}"
			assert_equal abody, msg.body, "equal check for #{abody}"
		end
  end

	# All possible byte values
  def test_kcode_002
		#
		abody = ""
		"\000".upto("\377") {|abyte| abody << abyte } 
		#
		dest = make_destination
    @conn.subscribe dest
	  @conn.publish dest, abody
		msg = @conn.receive
		assert_instance_of Stomp::Message , msg, "type check for #{abody}"
		assert_equal abody, msg.body, "equal check for #{abody}"
  end

	# A single byte at a time
  def test_kcode_003
		#
		dest = make_destination
    @conn.subscribe dest
		#
		"\000".upto("\377") do |abody|
			@conn.publish dest, abody
			msg = @conn.receive
			assert_instance_of Stomp::Message , msg, "type check for #{abody}"
			assert_equal abody, msg.body, "equal check for #{abody}"
		end
  end

	#
	def test_kcode_004
		#
		assert_raise(Stomp::Error::InvalidFormat) {
			aframe = Stomp::Message.new("junk")
		}
		#
		assert_raise(Stomp::Error::InvalidFormat) {
			aframe = Stomp::Message.new("command\njunk")
		}
		#
		assert_raise(Stomp::Error::InvalidFormat) {
			aframe = Stomp::Message.new("command\nheaders\n\njunk")
		}
		#
		assert_raise(Stomp::Error::InvalidServerCommand) {
			aframe = Stomp::Message.new("junkcommand\nheaders\n\njunk\0\n\n")
		}
		#
		assert_raise(Stomp::Error::InvalidFormat) {
			aframe = Stomp::Message.new("ERROR\nbadheaders\n\njunk\0\n\n")
		}
		#
		assert_nothing_raised {
			aframe = Stomp::Message.new("CONNECTED\nh1:val1\n\njunk\0\n")
		}
		#
		assert_nothing_raised {
			aframe = Stomp::Message.new("MESSAGE\nh1:val1\n\njunk\0\n")
		}
		#
		assert_nothing_raised {
			aframe = Stomp::Message.new("MESSAGE\nh2:val2\n\n\0")
		}
		#
		assert_nothing_raised {
			aframe = Stomp::Message.new("RECEIPT\nh1:val1\n\njunk\0\n")
		}
		#
		assert_nothing_raised {
			aframe = Stomp::Message.new("ERROR\nh1:val1\n\njunk\0\n")
		}

	end

  private
    def make_destination
      name = caller_method_name unless name
      "/queue/test/rubyk01/stomp/" + name
    end
end

