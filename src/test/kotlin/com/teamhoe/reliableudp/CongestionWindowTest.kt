package com.teamhoe.reliableudp

import com.teamhoe.reliableudp.net.CongestionWindow
import com.teamhoe.reliableudp.net.DataPacket
import com.teamhoe.reliableudp.net.ISeqPacket
import com.teamhoe.reliableudp.utils.UnsignedInt
import org.junit.Test
import java.net.InetSocketAddress
import java.util.*
import kotlin.concurrent.thread

/**
 * Created by Eric on 3/8/2016.
 */
internal class CongestionWindowTest()
{
    val dummyAddress = InetSocketAddress(0)
    val dummyPayload = "dummy".toByteArray(Charsets.UTF_8)
    val congestionWindow = CongestionWindow(10L,50.0,2000.0)

    @Test
    fun canTakeOncePut()
    {
        val packets = ArrayList<ISeqPacket>()
        for (i in 0..10) packets.add(DataPacket(dummyAddress,i,1000.0,dummyPayload))
        packets.forEach()
        {
            congestionWindow.put(listOf(it))
            assert(congestionWindow.take() == it)
            congestionWindow.ack(it.sequenceNumber,UnsignedInt.fromInt(100))
        }
    }

    @Test
    fun takeOnceEmptyShouldBlock()
    {
        val packets = ArrayList<ISeqPacket>()
        for (i in 0..10) packets.add(DataPacket(dummyAddress,i,1000.0,dummyPayload))
        packets.forEach()
        {
            congestionWindow.put(listOf(it))
            assert(congestionWindow.take() == it)
            congestionWindow.ack(it.sequenceNumber,UnsignedInt.fromInt(100))
        }
        // subsequent takes from the window should be blocked
        val blocked = thread(isDaemon = true) {congestionWindow.take()}
        Thread.sleep(100)
        assert(blocked.state == Thread.State.WAITING)
    }

    @Test
    fun canTakeMultipleTimesAfterPut()
    {
        val packets = ArrayList<ISeqPacket>()
        for (i in 0..10) packets.add(DataPacket(dummyAddress,i,1000.0,dummyPayload))
        congestionWindow.put(packets)
        // acknowledge all packets in window so they are removed
        packets.forEach()
        {
            val packet = congestionWindow.take()
            println(packet)
            assert(packet == it)
            congestionWindow.ack(it.sequenceNumber,UnsignedInt.fromInt(100))
        }
    }

    @Test
    fun canPutWhileTaking()
    {
        val packets = ArrayList<ISeqPacket>()
        for (i in 0..10) packets.add(DataPacket(dummyAddress,i,1000.0,dummyPayload))
        // acknowledge all packets in window so they are removed
        thread()
        {
            packets.forEach()
            {
                assert(congestionWindow.take() == it)
                congestionWindow.ack(it.sequenceNumber,UnsignedInt.fromInt(100))
            }
        }
        Thread.sleep(10)
        congestionWindow.put(packets)
    }

    @Test
    fun canTakeUntilAcked()
    {
        val packets = ArrayList<ISeqPacket>()
        for (i in 0..10) packets.add(DataPacket(dummyAddress,i,1000.0,dummyPayload))
        congestionWindow.put(packets)
        // try to take more packets than there are from window...this guarantees
        // that some packets will time out, and so will be de-queued mre than
        // once.
        repeat(Math.round(packets.size*1.5F))
        {
            congestionWindow.take()
        }
        // acknowledge all packets in window so they are removed
        packets.forEach()
        {
            congestionWindow.ack(it.sequenceNumber,UnsignedInt.fromInt(100))
        }
        // subsequent takes from the window should be blocked
        val blocked = thread(isDaemon = true) {congestionWindow.take()}
        Thread.sleep(100)
        assert(blocked.state == Thread.State.WAITING)
    }
}
