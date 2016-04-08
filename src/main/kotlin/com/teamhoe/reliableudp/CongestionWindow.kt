package com.teamhoe.reliableudp.net

import com.teamhoe.reliableudp.utils.Timestamped
import com.teamhoe.reliableudp.utils.UnsignedInt
import com.teamhoe.reliableudp.utils.positiveOffset
import java.util.*
import java.util.concurrent.*
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.thread
import kotlin.concurrent.withLock

internal class CongestionWindow(val estimatedRttErrorMargin:Long,initialEstimatedRtt:Double,initialMaxBytesInFlight:Double)
{
    var isSlowStart = true

    /**
     * the estimated round trip time of the connection.
     */
    var estimatedRtt:Double = initialEstimatedRtt

    /**
     * current maximum that [bytesInFlight] should not exceed, however,
     * this is not a hard maximum, as [bytesInFlight] sometimes exceeds
     * [maxBytesInFlight] in some circumstances. in such cases, the system
     * refrains from sending data.
     */
    private var maxBytesInFlight:Double = initialMaxBytesInFlight

    /**
     * current number of unacknowledged data in bytes; bytes that are still
     * travelling on the network.
     */
    private val bytesInFlight:Int
        get()
        {
            return pendingForAckPackets.sumBy {it.packet.datagram.length}
        }

    private val lock = ReentrantLock()

    private val signaledOnNetworkAvailable = lock.newCondition()

    /**
     * queue of all packets in the congestion window in the order that they
     * were [put], regardless of being retransmitted.
     */
    private val unackedPackets = LinkedBlockingQueue<Timestamped<ISeqPacket>>()

    /**
     * prioritized by order they should be sent in. queue of packets waiting
     * to be sent. should hold pending packets to transmit or retransmit.
     */
    private val pendingToSendPackets = PriorityBlockingQueue<ISeqPacket>(100,
        Comparator<ISeqPacket>
        {
            o1,o2 ->
            val relativeSequence1 = positiveOffset(unackedPackets.last().obj.sequenceNumber,o1.sequenceNumber)
            val relativeSequence2 = positiveOffset(unackedPackets.last().obj.sequenceNumber,o2.sequenceNumber)
            val difference = relativeSequence1-relativeSequence2
            difference.coerceIn(-1L..1L).toInt()
        })

    /**
     * packets waiting to be acknowledged. packets that time out and are
     * re-queued and also are placed back into the [pendingToSendPackets]. should
     * hold packets that have been transmitted at least once.
     */
    private val pendingForAckPackets = DelayQueue<DelayedPacket>()

    private var maxSequenceNumber:Int? = null

    /**
     * put a packet into the congestion window for transmission.
     */
    fun put(packets:Iterable<ISeqPacket>):Unit = lock.withLock()
    {
        // wait for there to be room in the congestion window
        while (bytesInFlight > maxBytesInFlight)
            signaledOnNetworkAvailable.await()

        packets.forEach()
        {
            // put the packet into the queue so we can calculate RTT
            unackedPackets.add(Timestamped(it))

            // add the packet to the [pendingToSendPackets] for sending
            pendingToSendPackets.add(it)
        }
    }

    /**
     * acknowledge that a transmitted or retransmitted packet was acknowledged
     * by the remote host.
     */
    fun ack(acknowledgementNumber:Int,windowSize:UnsignedInt):Unit = lock.withLock()
    {
        pendingForAckPackets.removeAll()
        {
            val shouldRemove = it.packet.sequenceNumber == acknowledgementNumber
            if (shouldRemove)
            {
                // bytes in flight book keeping
                val packetLength = it.packet.datagram.length
                // todo change these params too
                if (isSlowStart)
                    maxBytesInFlight += packetLength
                else
                    maxBytesInFlight += (packetLength.toFloat()/(bytesInFlight/packetLength.toFloat()))*10
            }

            // return true to remove; false otherwise
            return@removeAll shouldRemove
        }

        // abort retransmissions of the acknowledged packet
        pendingToSendPackets.removeAll()
        {
            return@removeAll it.sequenceNumber == acknowledgementNumber
        }

        // remove the packet from collection and update estimated RTT
        unackedPackets.removeAll()
        {
            val shouldRemove = it.obj.sequenceNumber == acknowledgementNumber
            if (shouldRemove)
            {
                // update estimated round trip time
                val packetRtt = System.currentTimeMillis()-it.timestamp+estimatedRttErrorMargin
                estimatedRtt += (0.1*(packetRtt-estimatedRtt)).toLong()
            }

            // return true to remove; false otherwise
            return@removeAll shouldRemove
        }

        // update remote receive window size
        maxSequenceNumber = (acknowledgementNumber.toLong()+windowSize.value).toInt()

        // signal condition met
        if (bytesInFlight < maxBytesInFlight)
            signaledOnNetworkAvailable.signalAll()

        // signal threads awaiting acks
        awaitAckCoundDownLatches[acknowledgementNumber]?.let()
        {
            latch ->
            latch.countDown()
            awaitAckCoundDownLatches.remove(acknowledgementNumber)
        }
    }

    private val awaitAckCoundDownLatches = LinkedHashMap<Int,CountDownLatch>()
    fun awaitAck(sequenceNumber:Int)
    {
        val latch = awaitAckCoundDownLatches.getOrPut(sequenceNumber,{CountDownLatch(1)})
        latch.await()
    }

    /**
     * removes the next packet that is ready for transmission or retransmission
     * because the network is free.
     */
    var packetsDropped = 0
    fun take():ISeqPacket
    {
        lock.withLock()
        {
            // wait for room on the network to send a packet so we don't
            // overwhelm the network with packets.
            while (bytesInFlight > maxBytesInFlight)
                signaledOnNetworkAvailable.await()
        }

        var retransmitThread = thread(name = "retransmitThread")
        {
            // packets removed from [pendingForAckPackets] have
            // timed out before they got acked; put them back in for
            // retransmission.
            val delayedPacket = try
            {
                pendingForAckPackets.take()
            }
            catch(ex:InterruptedException)
            {
                return@thread
            }
            pendingForAckPackets.put(delayedPacket)
            val packet = delayedPacket.packet

            // todo: tweak deez parameters
            estimatedRtt = Math.min(estimatedRtt*2,Double.MAX_VALUE)
            isSlowStart = false
            maxBytesInFlight -= packet.datagram.length
            rePut(packet)

            println("$this: estimatedRtt: $estimatedRtt, maxBytesInFlight: ${maxBytesInFlight}, packetsDropped: ${++packetsDropped}")
        }

        // send the next packet & update book keeping
        val packetToSend = pendingToSendPackets.take()
        pendingForAckPackets.add(DelayedPacket(packetToSend))
        retransmitThread.interrupt()        // stop the retransmit thread if it's still waiting on take()

        lock.withLock()
        {
            // wait for room in the remote receive window to send a packet so we
            // don't overwhelm the remote host with packets.
            while (positiveOffset(packetToSend.sequenceNumber,maxSequenceNumber ?: packetToSend.sequenceNumber+1) <= 0)
                signaledOnNetworkAvailable.await()
        }

        return packetToSend
    }

    /**
     * re-enqueue a packet into the congestion window for retransmission.
     */
    private fun rePut(packet:ISeqPacket)
    {
        // add the packet to the [pendingToSendPackets] for sending
        pendingToSendPackets.add(packet)
    }

    private inner class DelayedPacket(val packet:ISeqPacket):Delayed
    {
        private val timestampedPacket = Timestamped(packet)

        override fun getDelay(unit:TimeUnit):Long
        {
            val dequeueTimeMillis = timestampedPacket.timestamp+estimatedRtt.toLong()
            val delay = dequeueTimeMillis-System.currentTimeMillis()
            return Math.min(unit.convert(delay,TimeUnit.MILLISECONDS),1000)
        }

        override fun compareTo(other:Delayed):Int
        {
            return (getDelay(TimeUnit.MILLISECONDS)-other.getDelay(TimeUnit.MILLISECONDS))
                .coerceIn(-1L..1L).toInt()
        }
    }
}
