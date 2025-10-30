package ru.quipy.payments.logic

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.stereotype.Service
import org.springframework.web.server.ResponseStatusException
import org.springframework.http.HttpStatus
import ru.quipy.common.utils.CallerBlockingRejectedExecutionHandler
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.util.*
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import ru.quipy.common.utils.ProcessingTimeCounter
import ru.quipy.common.utils.TooManyRequestsException

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Tags

@Service
class OrderPayer(private val meterRegistry: MeterRegistry, private val paymentAccounts: List<PaymentExternalSystemAdapter>) {

    companion object {
        val logger: Logger = LoggerFactory.getLogger(OrderPayer::class.java)
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    @Autowired
    private lateinit var paymentService: PaymentService

    private val paymentTaskQueue = meterRegistry.gaugeCollectionSize("payment_task.queue_size", Tags.empty(), LinkedBlockingQueue<PaymentTask>())!!

    private val paymentAllTimer: Timer = Timer.builder("payment.all")
        .description("All time to process payment")
        .publishPercentileHistogram()
        .register(meterRegistry)

    private val paymentTaskTimer: Timer = Timer.builder("payment.task")
        .description("Time to process one payment task")
        .publishPercentileHistogram()
        .register(meterRegistry)

    private val additionalProcessingTime = paymentAccounts[0].getAverageProcessingTime()

    private val processingTimeCounter = ProcessingTimeCounter()

    private val workersCount = 16

    private val paymentExecutor = ThreadPoolExecutor(
        workersCount,
        workersCount,
        0L,
        TimeUnit.MILLISECONDS,
        LinkedBlockingQueue(8_000),
        NamedThreadFactory("payment-submission-executor"),
        CallerBlockingRejectedExecutionHandler()
    ).apply {
        repeat(workersCount) {
            submit {
                while (!Thread.currentThread().isInterrupted) {
                    try {
                        val task = paymentTaskQueue.take()
                        val taskStartedAt = now()
                        processPaymentTask(task)
                        val taskFinishedAt = now()
                        processingTimeCounter.record(taskFinishedAt - taskStartedAt)
                        paymentTaskTimer.record(taskFinishedAt - taskStartedAt, TimeUnit.MILLISECONDS)
                        paymentAllTimer.record(taskFinishedAt - task.createdAt, TimeUnit.MILLISECONDS)
                    } catch (e: InterruptedException) {
                        Thread.currentThread().interrupt()
                        break
                    } catch (e: Exception) {
                        logger.error("Error processing payment task", e)
                    }
                }
            }
        }
    }

    private fun processPaymentTask(task: PaymentTask) {
        val createdEvent = paymentESService.create {
            it.create(
                task.paymentId,
                task.orderId,
                task.amount
            )
        }
        logger.trace("Payment ${createdEvent.paymentId} for order ${task.orderId} created.")
        paymentService.submitPaymentRequest(task.paymentId, task.amount, task.createdAt, task.deadline)
    }

    fun processPayment(orderId: UUID, amount: Int, paymentId: UUID, deadline: Long): Long {
        val createdAt = System.currentTimeMillis()
        val averageProcessingTime = processingTimeCounter.getAverage()
        val maxProcessingTime = averageProcessingTime + additionalProcessingTime

        logger.info("Current averageProcessingTime is ${averageProcessingTime}ms, queueSize is ${paymentTaskQueue.size}")
        val queueProcessingTime = (paymentTaskQueue.size + workersCount) * maxProcessingTime / workersCount

        if (now() + queueProcessingTime > deadline) {
            logger.warn("Payment $paymentId for order $orderId not created (too many requests)")

            throw ResponseStatusException(
                HttpStatus.TOO_MANY_REQUESTS,
                "Too many requests. Please try again later."
            )
        }

        val task = PaymentTask(orderId, amount, paymentId, deadline, createdAt)
        logger.trace("Create task for payment $paymentId (orderId=$orderId)")
        paymentTaskQueue.put(task)

        return createdAt
    }

    private fun now() = System.currentTimeMillis()

    private data class PaymentTask(
        val orderId: UUID,
        val amount: Int,
        val paymentId: UUID,
        val deadline: Long,
        val createdAt: Long
    )

}
