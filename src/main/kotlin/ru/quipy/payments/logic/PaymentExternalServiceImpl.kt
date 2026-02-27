package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.BackgroundScopeProvider
import ru.quipy.common.utils.SlidingWindowRateLimiter
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors


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

        val mapper = ObjectMapper().registerKotlinModule()
    }

    private val NETWORKING_DELAY_MILLIS = 1000

    private val serviceName = properties.serviceName
    private val accountName = properties.accountName
    private val requestAverageProcessingTime = properties.averageProcessingTime
    private val rateLimitPerSec = properties.rateLimitPerSec
    private val parallelRequests = properties.parallelRequests

    private val rateLimiter = SlidingWindowRateLimiter(rateLimitPerSec.toLong(), Duration.ofMillis(1000L))
    // private val rateLimiter = FixedWindowRateLimiter(rateLimitPerSec, 1000, TimeUnit.MILLISECONDS)
    private val ongoingWindow = Semaphore(parallelRequests)

    private val httpClient = HttpClient.newBuilder()
        .version(HttpClient.Version.HTTP_2)
        .connectTimeout(Duration.ofSeconds(3))
        .executor(Executors.newFixedThreadPool(4))
        .build()



    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        backgroundScope.scope.launch {
            performPaymentAsyncImpl(paymentId, amount, paymentStartedAt, deadline)
        }
    }

    suspend fun performPaymentAsyncImpl(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        logger.warn("[$accountName] Submitting payment request for payment $paymentId")

        val transactionId = UUID.randomUUID()

//        backgroundScope.esScope.launch {
//            // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
//            // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
//            paymentESService.update(paymentId) {
//                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
//            }
//        }

        logger.info("[$accountName] Submit: $paymentId , txId: $transactionId")


        ongoingWindow.withPermit {
            rateLimiter.tickCoro()

            val request = HttpRequest.newBuilder()
                .uri(URI.create("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
                .timeout(Duration.ofMillis(2 * requestAverageProcessingTime.toMillis() + NETWORKING_DELAY_MILLIS))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();

            val rawBody = httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString()).thenApply { it.body() }.await();
            val body = try {
                mapper.readValue(rawBody, ExternalSysResponse::class.java)
            } catch (e: Exception) {
                logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, reason: $rawBody")
                ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
            }

            logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

//            backgroundScope.esScope.launch {
//                // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
//                // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
//                paymentESService.update(paymentId) {
//                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
//                }
//            }
        }
    }


    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    private data class Task(val paymentId: UUID, val transactionId: UUID, val amount: Int, val paymentStartedAt: Long, val deadline: Long, val attempt: Long = 0)

}

public fun now() = System.currentTimeMillis()
