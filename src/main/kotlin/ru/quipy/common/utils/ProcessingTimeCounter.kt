package ru.quipy.common.utils

import java.util.concurrent.atomic.AtomicLong

/**
 * A thread-safe counter that calculates exponential moving average of processing times.
 * 
 * @param alpha The smoothing factor between 0.0 and 1.0 that determines the weight
 *              given to recent measurements. A higher alpha value gives more weight
 *              to recent data, making the average more responsive to changes.
 *              Default value is 0.1.
 * 
 * Example usage:
 * ```
 * val counter = ProcessingTimeCounter(alpha = 0.1)
 * counter.record(150)  // Record a processing time of 150ms
 * counter.record(200)  // Record a processing time of 200ms
 * val average = counter.getAverage()  // Get the current exponential average
 * ```
 */
class ProcessingTimeCounter(private val alpha: Double = 0.1) {
    private val average = AtomicLong(0)

    fun record(processingTimeMillis: Long) {
        while (true) {
            val current = average.get()
            val next = if (current == 0L) {
                processingTimeMillis
            } else {
                processingTimeMillis.toDouble() * alpha + current.toDouble() *  (1.0 - alpha)
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
