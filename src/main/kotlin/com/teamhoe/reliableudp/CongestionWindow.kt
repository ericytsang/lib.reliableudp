package com.teamhoe.reliableudp.net

import com.teamhoe.reliableudp.utils.UnsignedInt
import com.teamhoe.reliableudp.utils.positiveOffset
import java.util.concurrent.*
import java.util.concurrent.locks.ReentrantLock
import kotlin.concurrent.withLock

internal class CongestionWindow(initialEstimatedRttErrorMargin:Long,initialEstimatedRtt:Double,initialMaxBytesInFlight:Double)
{
    var isSlowStart = true

    var estimatedRttErrorMargin:Long = initialEstimatedRttErrorMargin
        private set

    /**
     * the estimated round trip time of the connection.
     */
    var estimatedRtt:Double = initialEstimatedRtt
        private set

    /**
     * current maximum that [bytesInFlight] should not exceed, however,
     * this is not a hard maximum, as [bytesInFlight] sometimes exceeds
     * [maxBytesInFlight] in some circumstances. in such cases, the system
     * refrains from sending data.
     */
    private var maxBytesInFlight:Double = initialMaxBytesInFlight
        set(value)
        {
            field = Math.max(0.0,value)
        }

    /**
     * the last sequence number we're allowed to send as specified by remote
     * host via acknowledgements.
     */
    private var maxSequenceNumber:Int? = null

    /**
     * current number of unacknowledged data in bytes; bytes that are still
     * travelling on the network.
     */
    private val bytesInFlight:Int
        get()
        {
            assert(packetsInFlightAccess.isHeldByCurrentThread)
            return packetsInFlight.sumBy {it.packet.datagram.length}
        }

    private val packetsInFlightAccess = ReentrantLock()

    private val signaledOnAck = packetsInFlightAccess.newCondition()

    /**
     * packets that have been transmitted and are waiting to be acknowledged.
     * packets that time out and are re-queued and immediately and
     * retransmitted.
     */
    private val packetsInFlight = DelayQueue<PacketInFlight>()

    /**
     * queue of all packets in the congestion window in the order that they
     * were [put], regardless of being retransmitted.
     */
    private val unackedPackets = LinkedBlockingQueue<ISeqPacket>()

    var consecutiveDroppedPacketCount:Int = 0
        private set

    val numPacketsInFlight:Int get() = packetsInFlight.size

    /**
     * put a packet into the congestion window.
     */
    fun put(packets:Iterable<ISeqPacket>):Unit = packetsInFlightAccess.withLock()
    {
        packets.forEach()
        {
            packet ->

            // wait for room on the network, and in packets in flight
            while (bytesInFlight > maxBytesInFlight)
            {
                signaledOnAck.await()
            }

            // add packet to packets in flight
            unackedPackets.add(packet)
            packetsInFlight.add(PacketInFlight(packet,System.currentTimeMillis(),0,false))
        }
    }

    /**
     * acknowledge that a transmitted or retransmitted packet was acknowledged
     * by the remote host.
     */
    fun ack(acknowledgementNumber:Int,windowSize:UnsignedInt):Unit = packetsInFlightAccess.withLock()
    {
        // remove packet from packets in flight & update perceived network state
        packetsInFlight.removeAll()
        {
            val shouldRemove = it.packet.sequenceNumber == acknowledgementNumber
            if (shouldRemove)
            {
                // bytes in flight book keeping
                val packetLength = it.packet.datagram.length
                if (isSlowStart)
                    maxBytesInFlight += packetLength
                else
                    maxBytesInFlight += (packetLength.toFloat()/Math.max(maxBytesInFlight.toFloat()/packetLength.toFloat(),1.0F))

                // update estimated round trip time
                val packetRtt = System.currentTimeMillis()-it.initialTransmissionTime
                val packetRttEror = Math.abs(packetRtt-estimatedRtt)
                estimatedRttErrorMargin += (0.1*(packetRttEror-estimatedRttErrorMargin)).toLong()
                estimatedRtt += (0.1*(packetRtt-estimatedRtt)).toLong()
            }

            // return true to remove; false otherwise
            return@removeAll shouldRemove
        }
        unackedPackets.removeAll()
        {
            it.sequenceNumber == acknowledgementNumber
        }

        // reset dropped packet count
        consecutiveDroppedPacketCount = 0

        // update remote receive window size
        maxSequenceNumber = (acknowledgementNumber.toLong()+windowSize.value).toInt()

        // signal condition met
        signaledOnAck.signalAll()
    }

    fun flush() = packetsInFlightAccess.withLock()
    {
        while(packetsInFlight.isNotEmpty())
        {
            signaledOnAck.await()
        }
    }

    /**
     * removes the next packet that is ready for transmission or retransmission.
     */
    fun take():ISeqPacket
    {
        // remove the next packet for transmission, but put them back into
        // packets in flight, because they should only be removed once they
        // are acknowledged
        val packetInFlight = packetsInFlight.take()
        val packet = packetInFlight.packet
//        println("estimatedRtt: $estimatedRtt estimatedRttErrorMargin: $estimatedRttErrorMargin")
        packetsInFlight.put(PacketInFlight(packet,packetInFlight.initialTransmissionTime,estimatedRtt.toLong()+estimatedRttErrorMargin*4,true))

        // update perceived network status
        if (packetInFlight.isRetransmission)
        {
//            println("Retransmit!")
            // estimatedRtt = Math.min(estimatedRtt*2,Double.MAX_VALUE)
            isSlowStart = false
            maxBytesInFlight -= packet.datagram.length
            consecutiveDroppedPacketCount++
        }

        // wait for room in the remote receive window to send a packet so we
        // don't overwhelm the remote host with packets.
        packetsInFlightAccess.withLock()
        {
            while (positiveOffset(packet.sequenceNumber,maxSequenceNumber ?: packet.sequenceNumber+1) <= 0)
                signaledOnAck.await()
        }

        return packet
    }

    private inner class PacketInFlight(val packet:ISeqPacket,val initialTransmissionTime:Long,val delay:Long,val isRetransmission:Boolean):Delayed
    {
        val timeTransmitted = System.currentTimeMillis()

        override fun getDelay(unit:TimeUnit):Long
        {
            val dequeueTimeMillis = timeTransmitted+delay
            val delay = dequeueTimeMillis-System.currentTimeMillis()
            return Math.max(0,Math.min(unit.convert(delay,TimeUnit.MILLISECONDS),1000))
        }

        override fun compareTo(other:Delayed):Int
        {
            var result = (getDelay(TimeUnit.MILLISECONDS)-other.getDelay(TimeUnit.MILLISECONDS)).coerceIn(-1L..1L).toInt()
            if (result == 0 && other is PacketInFlight)
            {
                val relativeSequence1 = positiveOffset(unackedPackets.last().sequenceNumber,packet.sequenceNumber)
                val relativeSequence2 = positiveOffset(unackedPackets.last().sequenceNumber,other.packet.sequenceNumber)
                val difference = relativeSequence1-relativeSequence2
                result = difference.coerceIn(-1L..1L).toInt()
            }
            return result
        }
    }
}
