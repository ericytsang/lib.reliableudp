package com.teamhoe.reliableudp.utils

import java.util.BitSet

internal class UnsignedInt private constructor(val bytes:Int,val value:Long)
{
    companion object
    {
        fun fromInt(num:Int):UnsignedInt
        {
            val ba = BitSet.valueOf(longArrayOf(num.toLong())).toByteArray()
            val long = BitSet.valueOf(byteArrayOf(
                if(0 in ba.indices) ba[0] else 0x00,
                if(1 in ba.indices) ba[1] else 0x00,
                if(2 in ba.indices) ba[2] else 0x00,
                if(3 in ba.indices) ba[3] else 0x00))
                .toLongArray()[0]
            return UnsignedInt(num,long)
        }

        fun fromLong(num:Long):UnsignedInt
        {
            return UnsignedInt(num.toInt(),num)
        }
    }
}
