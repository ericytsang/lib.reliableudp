package com.teamhoe.reliableudp.utils

/**
 * returns true if num is in the specified range; false otherwise. accounts for
 * overflow.
 */
internal fun isInRange(num:Int,offset:Int,length:Int):Boolean
{
    try
    {
        return num in offset..Math.addExact(offset,length)
    }
    catch(ex:ArithmeticException)
    {
        return num !in offset..offset+length
            || num == offset || num == offset+length
    }
}

/**
 * returns an integer where [src] + integer == [dst]. this method takes overflow
 * into account
 */
internal fun positiveOffset(src:Int,dst:Int):Long
{
    if(dst >= src) return (dst-src).toLong()
    else return positiveOffset(src,Int.MAX_VALUE)+positiveOffset(Int.MIN_VALUE,dst)+1
}
