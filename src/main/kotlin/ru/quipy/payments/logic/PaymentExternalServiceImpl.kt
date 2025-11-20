package ru.quipy.payments.logic

import org.springframework.beans.factory.annotation.Autowired
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Dispatcher
import okhttp3.Request
import okhttp3.ConnectionPool
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.common.utils.FixedWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.common.utils.BackgroundScopeProvider
import ru.quipy.common.utils.executeAsync
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit

import kotlinx.coroutines.*
import kotlinx.coroutines.sync.*

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.DistributionSummary

import java.util.concurrent.atomic.AtomicInteger
import java.util.concurrent.locks.ReentrantLock
import ru.quipy.common.utils.TooManyRequestsException
import java.util.concurrent.PriorityBlockingQueue



// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val meterRegistry: MeterRegistry,
    private val paymentProviderHostPort: String,
    private val token: String,
    private val backgroundScope: BackgroundScopeProvider
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    // config
    private val maxAttempts = 3
    private val riskCoeff = 3
    // end config

    private val taskQueue = PriorityBlockingQueue<Task>(10_000, Comparator<Task> { t1, t2 ->
        t1.paymentStartedAt.compareTo(t2.paymentStartedAt)
    })

    private val httpDispatcher = Dispatchers.IO.limitedParallelism(32)
    private val esDispatcher = Dispatchers.IO.limitedParallelism(4)

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val inProgressOrWaitingRequestsCount = AtomicInteger(0)
    private val inProgressRequestsCount = AtomicInteger(0)

    private val incomingLock = ReentrantLock()

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofMillis(1000L))
    // private val rateLimiter = FixedWindowRateLimiter(rateLimitPerSec, 1000, TimeUnit.MILLISECONDS)
    private val ongoingWindow = Semaphore(parallelRequests)

    private val client = OkHttpClient.Builder()
        .dispatcher(Dispatcher().apply {
            maxRequests = parallelRequests
            maxRequestsPerHost = parallelRequests
        })
        .connectTimeout(30, TimeUnit.SECONDS)
        .readTimeout(30, TimeUnit.SECONDS)
        .writeTimeout(30, TimeUnit.SECONDS)
        .callTimeout(30, TimeUnit.SECONDS)
        .connectionPool(ConnectionPool(parallelRequests, 5, TimeUnit.SECONDS))
        .build()

    private val waitingOrInProcessSummary = DistributionSummary.builder("payment.queue")
        .publishPercentiles(0.25, 0.5, 0.75, 0.9, 0.95, 0.99)
        .register(meterRegistry)  

    private val paymentRequestsCounter: Counter = Counter.builder("payment.requests")
        .description("Total number of payment requests received")
        .tags("serviceName", serviceName, "accountName", accountName)
        .register(meterRegistry)

    private val paymentStartedCounter: Counter = Counter.builder("payment.started")
        .description("Total number of payments started")
        .tags("serviceName", serviceName, "accountName", accountName)
        .register(meterRegistry)    

    private fun getPaymentSystemProcessingTimer(status: String): Timer {
        return Timer.builder("payment_system.processing")
            .description("Payment system response timings")
            .publishPercentileHistogram()
            .tags("serviceName", serviceName, "accountName", accountName, "status", status)
            .register(meterRegistry)
    }

    private val paymentProcessingTimer: Timer = Timer.builder("payment.processing")
        .description("Payment processing timings")
        .publishPercentileHistogram()
        .tags("serviceName", serviceName, "accountName", accountName)
        .register(meterRegistry)

    private val paymentQueueTimer: Timer = Timer.builder("payment.queue")
        .description("Time spent in payment queue")
        .publishPercentileHistogram()
        .tags("serviceName", serviceName, "accountName", accountName)
        .register(meterRegistry)

    private fun getPaymentResponsesCounter(status: String, attempt: Long): Counter {
        return Counter.builder("payment.responses")
            .description("Total number of payment responses received")
            .tags(
                "serviceName", serviceName,
                "accountName", accountName,
                "status", status,
                "attempt", attempt.toString()
            )
            .register(meterRegistry)
    }

    private fun getPaymentPerfCounter(part: String): Timer {
        return Timer.builder("payment.part")
        .description("Time spent in payment part")
        .publishPercentileHistogram()
        .tags("serviceName", serviceName, "accountName", accountName, "part", part)
        .register(meterRegistry)
    }

    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        incomingLock.lock()
        try {
            val iw = inProgressOrWaitingRequestsCount.get()
            val i = inProgressRequestsCount.get()
            val waiting = iw - i

            logger.warn("[$accountName] IW count $iw")
            val average = requestAverageProcessingTime.toMillis()
            val estimatedProcessingTime = average * iw / rateLimitPerSec

            if (estimatedProcessingTime > deadline - paymentStartedAt) {
                throw TooManyRequestsException(0)
            }

            val transactionId = UUID.randomUUID()
            val task = Task(paymentId, transactionId, amount, paymentStartedAt, deadline)

            performTaskAsync(task)
        } finally {
            incomingLock.unlock()
        }
    }

    private fun performTaskAsync(task: Task) {
        taskQueue.put(task)

        waitingOrInProcessSummary.record(inProgressOrWaitingRequestsCount.incrementAndGet().toDouble())
        backgroundScope.scope.launch {
            doAsync()
            inProgressOrWaitingRequestsCount.getAndDecrement()
            inProgressRequestsCount.getAndDecrement()
        }
    }

    suspend fun doAsync() {
        paymentRequestsCounter.increment()

        ongoingWindow.withPermit {
            // TODO: rate limiter
            val s1 = now()
            rateLimiter.tickCoro()
            getPaymentPerfCounter("rateLimiter").record(now() - s1, TimeUnit.MILLISECONDS)
            paymentStartedCounter.increment()

            val task = taskQueue.take()

            logger.info("[$accountName] Submit: ${task.paymentId} , txId: ${task.transactionId}")

            withContext(esDispatcher) {
                // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
                // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
                paymentESService.update(task.paymentId) {
                    it.logSubmission(success = true, task.transactionId, now(), Duration.ofMillis(now() - task.paymentStartedAt))
                }
            }

            val startedAt = now()
            paymentQueueTimer.record(startedAt - task.paymentStartedAt, TimeUnit.MILLISECONDS)

            inProgressRequestsCount.getAndIncrement()

            val success = doRequestAsync(task)
            if (!success && task.attempt < maxAttempts && now() + riskCoeff * requestAverageProcessingTime.toMillis() <= task.deadline) {
                logger.warn("Retrying payment ${task.paymentId}, attempt = ${task.attempt + 1}, because it failed")
                val newTask = task.copy(attempt = task.attempt + 1)

                performTaskAsync(newTask)
            }

            val finishedPaymentAt = now()
            paymentProcessingTimer.record(finishedPaymentAt - task.paymentStartedAt, TimeUnit.MILLISECONDS)
            logger.info("Payment ${task.paymentId} processed in ${finishedPaymentAt - task.paymentStartedAt}ms, time to deadline ${task.deadline - finishedPaymentAt}ms")
        } // end of ongoingWindow
    }

    private suspend fun doRequestAsync(task: Task): Boolean {
        val startedAt = now()
        try {
            val request = Request.Builder().run {
                val timeout = "%.2f".format(2 * requestAverageProcessingTime.toMillis() / 1000.0)
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=${task.transactionId}&paymentId=${task.paymentId}&amount=${task.amount}&timeout=PT${timeout}S")
                post(emptyBody)
            }.build()
            val response = client.newCall(request).executeAsync()
            val rawBody = withContext(httpDispatcher) { response.body?.string() }
            val finishedAt = now()
            logger.info("Request to payment system for payment ${task.paymentId} processed in ${finishedAt - task.paymentStartedAt}ms")

            val body = try {
                mapper.readValue(rawBody, ExternalSysResponse::class.java)
            } catch (e: Exception) {
                logger.error("[$accountName] [ERROR] Payment processed for txId: ${task.transactionId}, payment: ${task.paymentId}, result code: ${response.code}, reason: $rawBody")
                ExternalSysResponse(task.transactionId.toString(), task.paymentId.toString(), false, e.message)
                return false
            }
            
            val status = if (body.result) "success" else "error"

            getPaymentSystemProcessingTimer(status).record(finishedAt - startedAt, TimeUnit.MILLISECONDS)
            getPaymentResponsesCounter(status, task.attempt).increment()

            return body.result
        } catch (e: Exception) {
            when (e) {
                is SocketTimeoutException -> {
                    logger.error("[$accountName] Payment timeout for txId: ${task.transactionId}, payment: ${task.paymentId}", e)
                    withContext(esDispatcher) {
                        paymentESService.update(task.paymentId) {
                            it.logProcessing(false, now(), task.transactionId, reason = "Request timeout.")
                        }
                    }
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: ${task.transactionId}, payment: ${task.paymentId}", e)

                    withContext(esDispatcher) {
                        paymentESService.update(task.paymentId) {
                            it.logProcessing(false, now(), task.transactionId, reason = e.message)
                        }
                    }
                }
            }

            getPaymentSystemProcessingTimer("exception_error").record(now() - startedAt, TimeUnit.MILLISECONDS)
            getPaymentResponsesCounter("exception_error", task.attempt).increment()

            return false
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    private data class Task(val paymentId: UUID, val transactionId: UUID, val amount: Int, val paymentStartedAt: Long, val deadline: Long, val attempt: Long = 0)

}

public fun now() = System.currentTimeMillis()
