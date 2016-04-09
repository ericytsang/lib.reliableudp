package com.teamhoe.reliableudp.net

import com.teamhoe.reliableudp.utils.isInRange
import com.teamhoe.reliableudp.utils.isInRange
import java.util.*

/**
 * the receive window receives sequenced packets, and orders them. it does not
 * allow the sequenced packets to be removed from the receive window until it is
 * in order.
 */
internal class ReceiveWindow(initialSequenceNumber:Int,val maxWindowSize:Int)
{
    /**
     * the last acceptable sequence number that this [ReceiveWindow] will
     * accept via [offer].
     */
    val lastSequenceNumber:Int
        get() = nextSequenceNumber+maxWindowSize

    /**
     * the sequence number of the next [ISeqPacket] to return
     */
    var nextSequenceNumber:Int = initialSequenceNumber
        private set

    /**
     * map of all received, unread sequenced packets.
     */
    private val packets:MutableMap<Int,ISeqPacket> = LinkedHashMap()

    private val releasedOnPacketAdded = Object()

    /**
     * tries to put the packet into the [ReceiveWindow]. if the operation is
     * siccessful, returns true; false otherwise.
     */
    fun offer(packet:ISeqPacket):Boolean = synchronized(releasedOnPacketAdded)
    {
        // if the packet sequence number is within the receive
        // window's acceptable sequence number range, take it
        val result = isInRange(packet.sequenceNumber,nextSequenceNumber,maxWindowSize)
        if (result)
        {
            packets.put(packet.sequenceNumber,packet)
            releasedOnPacketAdded.notifyAll()
        }
        return result
    }

    /**
     * returns the next consecutive [ISeqPacket]; blocks if necessary until it
     * is available.
     */
    fun peek():ISeqPacket = synchronized(releasedOnPacketAdded)
    {
        while (!packets.containsKey(nextSequenceNumber))
            releasedOnPacketAdded.wait()

        return packets[nextSequenceNumber]!!
    }

    /**
     * returns the next consecutive [ISeqPacket]; null if unavailable.
     */
    fun poll():ISeqPacket? = synchronized(releasedOnPacketAdded)
    {
        return packets[nextSequenceNumber]
    }

    /**
     * returns the next consecutive [ISeqPacket]; blocks if necessary until it
     * is available.
     */
    fun take():ISeqPacket = synchronized(releasedOnPacketAdded)
    {
        try
        {
            return peek()
        }
        finally
        {
            packets.remove(nextSequenceNumber)
            ++nextSequenceNumber
        }
    }
}
