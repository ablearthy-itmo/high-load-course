package ru.quipy.payments.logic

import org.springframework.beans.factory.annotation.Autowired
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.OngoingWindow
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Timer
import io.micrometer.core.instrument.MeterRegistry


// Advice: always treat time as a Duration
class PaymentExternalSystemAdapterImpl(
    private val properties: PaymentAccountProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>,
    private val meterRegistry: MeterRegistry,
    private val paymentProviderHostPort: String,
    private val token: String
) : PaymentExternalSystemAdapter {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalSystemAdapter::class.java)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofSeconds(1))
    private val ongoingWindow = OngoingWindow(parallelRequests)

    private val client = OkHttpClient.Builder().build()

    private val paymentRequestsCounter: Counter = Counter.builder("payment.requests")
        .description("Total number of payment requests received")
        .tags("serviceName", serviceName, "accountName", accountName)
        .register(meterRegistry)

    private val paymentSystemProcessingTimer: Timer = Timer.builder("payment_system.processing")
        .description("Payment system response timings")
        .tags("serviceName", serviceName, "accountName", accountName)
        .register(meterRegistry)

    private val paymentProcessingTimer: Timer = Timer.builder("payment.processing")
        .description("Payment processing timings")
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


    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val startedPaymentAt = now()
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")
        paymentRequestsCounter.increment()

        val transactionId = UUID.randomUUID()

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")

        ongoingWindow.acquire()
    
        try {
            rateLimiter.tickBlocking()

            val request = Request.Builder().run {
                url("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount")
                post(emptyBody)
            }.build()


            // TODO isagila
            // if ((now() + requestAverageProcessingTime.toMillis() + 4000) > deadline) {
            //     logger.warn("[$accountName] Payment expired for txId: $transactionId, payment: $paymentId")
            //     getPaymentResponsesCounter("theoretical_expired").increment()
            //     paymentESService.update(paymentId) {
            //         it.logProcessing(false, now(), transactionId, reason = "Payment expired.")
            //     }
            //     return
            // }

            val startedAt = now()
            client.newCall(request).execute().use { response ->
                val finishedAt = now()
                paymentSystemProcessingTimer.record(finishedAt - startedAt, TimeUnit.MILLISECONDS)
                logger.info("Request to payment system for payment $paymentId processed in ${finishedAt - startedPaymentAt}ms")

                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(),false, e.message)
                }

                logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                if (finishedAt > deadline) {
                    getPaymentResponsesCounter("real_expired").increment()
                } else {
                    getPaymentResponsesCounter(if (body.result) "success" else "error").increment()
                }
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
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error("[$accountName] Payment failed for txId: $transactionId, payment: $paymentId", e)

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = e.message)
                    }
                }
            }
            getPaymentResponsesCounter("exception_error").increment()
        } finally {
            ongoingWindow.release()
            val finishedPaymentAt = now()
            paymentProcessingTimer.record(finishedPaymentAt - startedPaymentAt, TimeUnit.MILLISECONDS)
            logger.info("Payment $paymentId processed in ${finishedPaymentAt - startedPaymentAt}ms, time to deadline ${deadline - finishedPaymentAt}ms")
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

}

public fun now() = System.currentTimeMillis()
