package com.teamhoe.reliableudp
import com.teamhoe.reliableudp.net.DataPacket
import com.teamhoe.reliableudp.net.ISeqPacket
import com.teamhoe.reliableudp.net.ReceiveWindow
import org.junit.Test
import java.net.InetSocketAddress
import java.util.*
import kotlin.concurrent.thread

/**
 * Created by Eric on 3/7/2016.
 */

internal class ReceiveWindowTest()
{
    val dummyAddress = InetSocketAddress.createUnresolved("dummy",0)
    val dummyPayload = "dummy".toByteArray(Charsets.UTF_8)

    @Test
    fun normalSequenceNumbers()
    {
        // generate sequenced packets in sequence
        val packets = ArrayList<ISeqPacket>()
        val sequenceNumbers = ArrayList<Int>()
        for (i in 0..10) {sequenceNumbers.add(i)}
        sequenceNumbers.forEach()
        {
            packets.add((DataPacket(dummyAddress,it,1000.0,dummyPayload)))
        }

        // shuffle the order of the packets and insert them into a receive window
        Collections.shuffle(packets)
        val receiveWindow = ReceiveWindow(sequenceNumbers.first(),Int.MAX_VALUE)
        packets.forEach {receiveWindow.offer(it)}

        // remove the packets from the receive window and verify that they are in order
        sequenceNumbers.forEach()
        {
            assert(receiveWindow.take().sequenceNumber == it)
        }
    }

    @Test
    fun overflowedSequenceNumbers()
    {
        // generate sequenced packets in sequence
        val packets = ArrayList<ISeqPacket>()
        val sequenceNumbers = ArrayList<Int>()
        for (i in -10..10) { sequenceNumbers.add(i+Int.MAX_VALUE) }
        sequenceNumbers.forEach()
        {
            packets.add((DataPacket(dummyAddress,it,1000.0,dummyPayload)))
        }

        // shuffle the order of the packets and insert them into a receive window
        Collections.shuffle(packets)
        val receiveWindow = ReceiveWindow(sequenceNumbers.first(),Int.MAX_VALUE)
        packets.forEach {receiveWindow.offer(it)}

        // remove the packets from the receive window and verify that they are in order
        sequenceNumbers.forEach()
        {
            assert(receiveWindow.take().sequenceNumber == it)
        }
    }

    @Test
    fun canOfferWhileBlockedInTake()
    {
        // generate sequenced packets in sequence
        val packets = ArrayList<ISeqPacket>()
        val sequenceNumbers = ArrayList<Int>()
        for (i in 0..10) {sequenceNumbers.add(i)}
        sequenceNumbers.forEach()
        {
            packets.add((DataPacket(dummyAddress,it,1000.0,dummyPayload)))
        }

        val receiveWindow = ReceiveWindow(sequenceNumbers.first(),Int.MAX_VALUE)

        // remove the packets from the receive window and verify that they are in order
        val shouldBeBlocked = thread()
        {
            sequenceNumbers.forEach()
            {
                assert(receiveWindow.take().sequenceNumber == it)
            }
        }

        // verify that the reading thread is blocked
        Thread.sleep(100)
        assert(shouldBeBlocked.state == Thread.State.WAITING)

        // shuffle the order of the packets and insert them into a receive window
        Collections.shuffle(packets)
        packets.forEach {receiveWindow.offer(it)}

        // verify that the reading thread is dead
        Thread.sleep(100)
        assert(shouldBeBlocked.state == Thread.State.TERMINATED)
    }

    @Test
    fun blockOnMissingPacket()
    {
        // generate sequenced packets in sequence
        val packets = ArrayList<ISeqPacket>()
        val sequenceNumbers = ArrayList<Int>()
        for (i in 0..10) {if (i != 5) sequenceNumbers.add(i)}
        sequenceNumbers.forEach()
        {
            packets.add((DataPacket(dummyAddress,it,1000.0,dummyPayload)))
        }

        // shuffle the order of the packets and insert them into a receive window
        Collections.shuffle(packets)
        val receiveWindow = ReceiveWindow(sequenceNumbers.first(),Int.MAX_VALUE)
        packets.forEach {receiveWindow.offer(it)}

        // remove the packets from the receive window and verify that they are in order
        val shouldBeBlocked = thread()
        {
            sequenceNumbers.forEach()
            {
                assert(receiveWindow.take().sequenceNumber == it)
            }
        }

        // verify that the thread is blocked
        Thread.sleep(100)
        assert(shouldBeBlocked.state == Thread.State.WAITING)
        shouldBeBlocked.interrupt()
    }
}