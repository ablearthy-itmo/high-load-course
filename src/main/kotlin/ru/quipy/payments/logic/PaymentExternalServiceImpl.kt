package ru.quipy.payments.logic

import org.springframework.beans.factory.annotation.Autowired
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Dispatcher
import okhttp3.Request
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

    private val paymentSystemProcessingTimer: Timer = Timer.builder("payment_system.processing")
        .description("Payment system response timings")
        .publishPercentileHistogram()
        .tags("serviceName", serviceName, "accountName", accountName)
        .register(meterRegistry)

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

    private fun getPaymentResponsesCounter(status: String): Counter {
        return Counter.builder("payment.responses")
            .description("Total number of payment responses received")
            .tags(
                "serviceName", serviceName,
                "accountName", accountName,
                "status", status
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
            val estimatedProcessingTime = 1000.0 * iw / rateLimitPerSec + 1.0 * requestAverageProcessingTime.toMillis()

            if (estimatedProcessingTime > deadline - paymentStartedAt) {
                throw TooManyRequestsException(0)
            }

            
            waitingOrInProcessSummary.record(inProgressOrWaitingRequestsCount.incrementAndGet().toDouble())
        } finally {
            incomingLock.unlock()
        }
        backgroundScope.scope.launch {
            doAsync(paymentId, amount, paymentStartedAt, deadline)
            inProgressOrWaitingRequestsCount.getAndDecrement()
            inProgressRequestsCount.getAndDecrement()
        }
    }

    suspend fun doAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val enteredPaymentAt = now()
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        paymentRequestsCounter.increment()

        val transactionId = UUID.randomUUID()

        withContext(esDispatcher) {
            // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
            // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        // ongoingWindow.withPermit {
            // TODO: rate limiter
            val s1 = now()
            while (!rateLimiter.tick()) { delay(10L) }
            getPaymentPerfCounter("rateLimiter").record(now() - s1, TimeUnit.MILLISECONDS)
            paymentStartedCounter.increment()

            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()

            val startedAt = now()
            paymentQueueTimer.record(startedAt - enteredPaymentAt, TimeUnit.MILLISECONDS)

            inProgressRequestsCount.getAndIncrement()
            try {
                val response = client.newCall(request).executeAsync()

                val rawBody = withContext(httpDispatcher) { response.body?.string() }

                val finishedAt = now()
                paymentSystemProcessingTimer.record(finishedAt - startedAt, TimeUnit.MILLISECONDS)
                logger.info("Request to payment system for payment $paymentId processed in ${finishedAt - enteredPaymentAt}ms")

                val body = try {
                    mapper.readValue(rawBody, ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: $rawBody")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                if (finishedAt > deadline) {
                    getPaymentResponsesCounter("real_expired").increment()
                } else {
                    getPaymentResponsesCounter(if (body.result) "success" else "error").increment()
                }
                withContext(esDispatcher) {
                    // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                    // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                }

            } catch (e: Exception) {
                when (e) {
                    is SocketTimeoutException -> {
                        logger.error("[$accountName] Payment timeout for txId: $transactionId, payment: $paymentId", e)
                        withContext(esDispatcher) {
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                            }
                        }
                    }

                    else -> {
                        logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                        withContext(esDispatcher) {
                            paymentESService.update(paymentId) {
                                it.logProcessing(false, now(), transactionId, reason = e.message)
                            }
                        }
                    }
                }
                getPaymentResponsesCounter("exception_error").increment()
            } finally {
                val finishedPaymentAt = now()
                paymentProcessingTimer.record(finishedPaymentAt - enteredPaymentAt, TimeUnit.MILLISECONDS)
                logger.info("Payment $paymentId processed in ${finishedPaymentAt - enteredPaymentAt}ms, time to deadline ${deadline - finishedPaymentAt}ms")
            }
        // } // end of ongoingWindow
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()
