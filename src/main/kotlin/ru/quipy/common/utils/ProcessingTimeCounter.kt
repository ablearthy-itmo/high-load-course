package ru.quipy.common.utils

import java.util.concurrent.atomic.AtomicLong

class ProcessingTimeCounter(private val alpha: Double = 0.1) {
    private val average = AtomicLong(0)

    fun record(processingTime: Long) {
        while (true) {
            val current = average.get()
            val next = if (current == 0L) {
                processingTime
            } else {
                processingTime.toDouble() * alpha + current.toDouble() *  (1.0 - alpha)
            }

            if (average.compareAndSet(current, next.toLong())) {
                break
            }
        }
    }

    fun getAverage(): Long {
        return average.get()
    }
}
