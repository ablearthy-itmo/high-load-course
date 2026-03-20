package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.micrometer.core.instrument.MeterRegistry
import kotlinx.coroutines.future.await
import kotlinx.coroutines.launch
import kotlinx.coroutines.*
import kotlinx.coroutines.selects.select
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

// import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Timer
import java.util.concurrent.TimeUnit
//import java.net.http.HttpTimeoutException
import java.io.IOException
import java.lang.InterruptedException

import io.github.resilience4j.circuitbreaker.CircuitBreakerConfig
import io.github.resilience4j.circuitbreaker.CircuitBreakerRegistry


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

    private val HTTP_CLIENT_REQUEST_TIMEOUT_MILLIS = 500L

    private val NETWORKING_DELAY_MILLIS = 200

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

    private val expectedProcessingTimeMillis = 2 * requestAverageProcessingTime.toMillis() + NETWORKING_DELAY_MILLIS

    private val circuitBreakerConfig = CircuitBreakerConfig.custom()
        .slidingWindowType(CircuitBreakerConfig.SlidingWindowType.TIME_BASED)
        .slidingWindowSize(3) // seconds
        .failureRateThreshold(25f) 
        // .slowCallRateThreshold(25f)
        // .slowCallDurationThreshold(Duration.ofMillis(expectedProcessingTimeMillis))
        .waitDurationInOpenState(Duration.ofMillis(15_000))
        .permittedNumberOfCallsInHalfOpenState(1)
        .recordExceptions(IOException::class.java, InterruptedException::class.java)
        // .ignoreExceptions(BusinessException::class.java, OtherBusinessException::class.java)
        .build()
    private val circuitBreakerRegistry = CircuitBreakerRegistry.of(circuitBreakerConfig)
    private val circuitBreaker = circuitBreakerRegistry.circuitBreaker("payment-system")


    private fun getHttpClientRequestDurationTimer(dest: String, status: String, error: Boolean): Timer {
        return Timer.builder("http_client_requests_duration_seconds")
            .description("HTTP Client requests duration")
            .publishPercentileHistogram()
            .tags("serviceName", serviceName, "accountName", accountName, "dest", dest, "status", status, "error", if (error) "true" else "false")
            .register(meterRegistry)
    }


    override fun performPaymentAsync(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        backgroundScope.scope.launch {
            select<Unit> {
                async { performPaymentAsyncImpl(paymentId, amount, paymentStartedAt, deadline) }.onAwait {}
                // async {
                //     delay(expectedProcessingTimeMillis)
                //     performPaymentAsyncImpl(paymentId, amount, paymentStartedAt, deadline)
                // }.onAwait {}
                // async {
                //     delay(2 * expectedProcessingTimeMillis)
                //     performPaymentAsyncImpl(paymentId, amount, paymentStartedAt, deadline)
                // }.onAwait {}
            }
            coroutineContext.cancelChildren()
        }
    }

    suspend fun performPaymentAsyncImpl(paymentId: UUID, amount: Int, paymentStartedAt: Long, deadline: Long) {
        val transactionId = UUID.randomUUID()

        backgroundScope.esScope.launch {
            // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
            // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
            paymentESService.update(paymentId) {
                it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
            }
        }

        ongoingWindow.withPermit {
            val request = HttpRequest.newBuilder()
                .uri(URI.create("http://$paymentProviderHostPort/external/process?serviceName=$serviceName&token=$token&accountName=$accountName&transactionId=$transactionId&paymentId=$paymentId&amount=$amount"))
                .timeout(Duration.ofMillis(HTTP_CLIENT_REQUEST_TIMEOUT_MILLIS))
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();

            var i = 0
            while (i < 10 && now() < deadline) {
                rateLimiter.tickCoro()

                if (!circuitBreaker.tryAcquirePermission()) {
                  continue
                }

                val startedAt = now()
                val response = try {
                    httpClient.sendAsync(request, HttpResponse.BodyHandlers.ofString()).await();
                } catch (e: Exception) {
                    val err = if (e is IOException) "TIMEOUT" else "ERROR"

                    logger.info("current state: ${circuitBreaker.getState()}")


                    getHttpClientRequestDurationTimer(
                      "payment_service",
                      err,
                      true
                    ).record(now() - startedAt, TimeUnit.MILLISECONDS)
                    circuitBreaker.onError(now() - startedAt, TimeUnit.MILLISECONDS, e)
                    i += 1
                    continue
                }
                val rawBody = response.body()
                val statusCode = response.statusCode()
                if (statusCode >= 500) {
                    getHttpClientRequestDurationTimer(
                      "payment_service",
                      statusCode.toString(),
                      true
                    ).record(now() - startedAt, TimeUnit.MILLISECONDS)
                    circuitBreaker.onError(now() - startedAt, TimeUnit.MILLISECONDS, IOException("500"))
                    continue
                }

                val body = try {
                    mapper.readValue(rawBody, ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, reason: $rawBody")
                    ExternalSysResponse(transactionId.toString(), paymentId.toString(), false, e.message)
                }
                val finishedAt = now()

                getHttpClientRequestDurationTimer(
                  "payment_service",
                  statusCode.toString(),
                  !body.result
                ).record(finishedAt - startedAt, TimeUnit.MILLISECONDS)
                circuitBreaker.onSuccess(finishedAt - startedAt, TimeUnit.MILLISECONDS)
 
                backgroundScope.esScope.launch {
                    // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                    // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                }
                return
            }

           backgroundScope.esScope.launch {
               paymentESService.update(paymentId) {
                   it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
               }
           }
        }
    }

    override fun price() = properties.price

    override fun isEnabled() = properties.enabled

    override fun name() = properties.accountName

    private data class Task(val paymentId: UUID, val transactionId: UUID, val amount: Int, val paymentStartedAt: Long, val deadline: Long, val attempt: Long = 0)

}

public fun now() = System.currentTimeMillis()
