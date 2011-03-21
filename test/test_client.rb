$:.unshift(File.dirname(__FILE__))

require 'test_helper'

class TestClient < Test::Unit::TestCase
  include TestBase
  
  def setup
    @client = Stomp::Client.new(user, passcode, host, port)
    # Multi_thread test data
    @max_threads = 20
    @max_msgs = 50
  end

  def teardown
    @client.close if @client # allow tests to close
  end

  def test_ack_api_works
    @client.publish destination, message_text, {:suppress_content_length => true}

    received = nil
    @client.subscribe(destination, {:ack => 'client'}) {|msg| received = msg}
    sleep 0.01 until received
    assert_equal message_text, received.body

    receipt = nil
    @client.acknowledge(received) {|r| receipt = r}
    sleep 0.01 until receipt
    assert_not_nil receipt.headers['receipt-id']
  end

  def test_asynch_subscribe
    received = false
    @client.subscribe(destination) {|msg| received = msg}
    @client.publish destination, message_text
    sleep 0.01 until received

    assert_equal message_text, received.body
  end

  def test_noack
    @client.publish destination, message_text

    received = nil
    @client.subscribe(destination, :ack => :client) {|msg| received = msg}
    sleep 0.01 until received
    assert_equal message_text, received.body
    @client.close

    # was never acked so should be resent to next client

    @client = Stomp::Client.new(user, passcode, host, port)
    received2 = nil
    @client.subscribe(destination) {|msg| received2 = msg}
    sleep 0.01 until received2

    assert_equal message_text, received2.body
    assert_equal received.body, received2.body
    assert_equal received.headers['message-id'], received2.headers['message-id']
  end

  def test_receipts
    receipt = false
    @client.publish(destination, message_text) {|r| receipt = r}
    sleep 0.1 until receipt

    message = nil
    @client.subscribe(destination) {|m| message = m}
    sleep 0.1 until message
    assert_equal message_text, message.body
  end

  def test_disconnect_receipt
    @client.close :receipt => "xyz789"
    assert_nothing_raised {
      assert_not_nil(@client.disconnect_receipt, "should have a receipt")
      assert_equal(@client.disconnect_receipt.headers['receipt-id'],
        "xyz789", "receipt sent and received should match")
    }
    @client = nil
  end

  def test_publish_then_sub
    @client.publish destination, message_text
    message = nil
    @client.subscribe(destination) {|m| message = m}
    sleep 0.01 until message

    assert_equal message_text, message.body
  end

  def test_subscribe_requires_block
    assert_raise(RuntimeError) do
      @client.subscribe destination
    end
  end

  def test_transactional_publish
    @client.begin 'tx1'
    @client.publish destination, message_text, :transaction => 'tx1'
    @client.commit 'tx1'

    message = nil
    @client.subscribe(destination) {|m| message = m}
    sleep 0.01 until message

    assert_equal message_text, message.body
  end

  def test_transaction_publish_then_rollback
    @client.begin 'tx1'
    @client.publish destination, "first_message", :transaction => 'tx1'
    @client.abort 'tx1'

    @client.begin 'tx1'
    @client.publish destination, "second_message", :transaction => 'tx1'
    @client.commit 'tx1'

    message = nil
    @client.subscribe(destination) {|m| message = m}
    sleep 0.01 until message
    assert_equal "second_message", message.body
  end

  def test_transaction_ack_rollback_with_new_client
    @client.publish destination, message_text

    @client.begin 'tx1'
    message = nil
    @client.subscribe(destination, :ack => 'client') {|m| message = m}
    sleep 0.01 until message
    assert_equal message_text, message.body
    @client.acknowledge message, :transaction => 'tx1'
    message = nil
    @client.abort 'tx1'

    # lets recreate the connection
    teardown
    setup
    @client.subscribe(destination, :ack => 'client') {|m| message = m}

    Timeout::timeout(4) do
      sleep 0.01 until message
    end
    assert_not_nil message
    assert_equal message_text, message.body

    @client.begin 'tx2'
    @client.acknowledge message, :transaction => 'tx2'
    @client.commit 'tx2'
  end

  def test_transaction_with_client_side_redelivery
    @client.publish destination, message_text

    @client.begin 'tx1'
    message = nil
    @client.subscribe(destination, :ack => 'client') { |m| message = m }

    sleep 0.1 while message.nil?

    assert_equal message_text, message.body
    @client.acknowledge message, :transaction => 'tx1'
    message = nil
    @client.abort 'tx1'

    sleep 0.1 while message.nil?

    assert_not_nil message
    assert_equal message_text, message.body

    @client.begin 'tx2'
    @client.acknowledge message, :transaction => 'tx2'
    @client.commit 'tx2'
  end
  
  def test_connection_frame
  	assert_not_nil @client.connection_frame
  end

  def test_unsubscribe
    message = nil
    dest = destination
    to_send = message_text
    client = Stomp::Client.new(user, passcode, host, port, true)
    assert_nothing_raised {
      client.subscribe(dest, :ack => 'client') { |m| message = m }
      @client.publish dest, to_send
      Timeout::timeout(4) do
        sleep 0.01 until message
      end
    }
    assert_equal to_send, message.body, "first body check"
    assert_nothing_raised {
      client.unsubscribe dest # was throwing exception on unsub at one point
      client.close
    }
    #  Same message should remain on the queue.  Receive it again with ack=>auto.
    message_copy = nil
    client = Stomp::Client.new(user, passcode, host, port, true)
    assert_nothing_raised {
      client.subscribe(dest, :ack => 'auto') { |m| message_copy = m }
      Timeout::timeout(4) do
        sleep 0.01 until message_copy
      end
    }
    assert_equal to_send, message_copy.body, "second body check"
    assert_equal message.headers['message-id'], message_copy.headers['message-id'], "header check"
  end

  def test_thread_one_subscribe
    msg = nil
    dest = destination
    Thread.new(@client) do |acli|
      assert_nothing_raised {
        acli.subscribe(dest) { |m| msg = m }
        Timeout::timeout(4) do
          sleep 0.01 until msg
        end
      }
    end
    #
    @client.publish(dest, message_text)
    sleep 1
    assert_not_nil msg
  end

  def test_thread_multi_subscribe
    #
    lock = Mutex.new
    msg_ctr = 0
    dest = destination
    1.upto(@max_threads) do |tnum|
      # Threads within threads .....
      Thread.new(@client) do |acli|
        assert_nothing_raised {
          acli.subscribe(dest) { |m| 
            msg = m
            lock.synchronize do
              msg_ctr += 1
            end
            # Simulate message processing
            sleep 0.05
          }
        }
      end
    end
    #
    1.upto(@max_msgs) do |mnum|
      msg = Time.now.to_s + " #{mnum}"
      @client.publish(dest, message_text)
    end
    #
    max_sleep=5
    sleep_incr = 0.10
    total_slept = 0
    while true
      break if @max_msgs == msg_ctr
      total_slept += sleep_incr
      break if total_slept > max_sleep
      sleep sleep_incr
    end
    assert_equal @max_msgs, msg_ctr
  end

  private
    def message_text
      name = caller_method_name unless name
      "test_client#" + name
    end

    def destination
      name = caller_method_name unless name
      "/queue/test/ruby/client/" + name
    end
end
