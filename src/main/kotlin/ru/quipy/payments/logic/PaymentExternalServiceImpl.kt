package ru.quipy.payments.logic


import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import io.github.resilience4j.ratelimiter.RateLimiter
import io.github.resilience4j.ratelimiter.RateLimiterConfig
import kotlinx.coroutines.*
import okhttp3.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors

class PaymentExternalServiceImpl(
    private val defaultProperties: ExternalServiceProperties,
    private val alternativeProperties: ExternalServiceProperties
) : PaymentExternalService {


    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)
        val paymentOperationTimeout = Duration.ofSeconds(80)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }


    private val defaultRateLimiter: RateLimiter = RateLimiter.of("defaultRateLimiter", RateLimiterConfig.custom()
        .limitRefreshPeriod(Duration.ofSeconds(1))
        .limitForPeriod(defaultProperties.rateLimitPerSec)
        .timeoutDuration(paymentOperationTimeout)
        .build())


    private val alternativeRateLimiter: RateLimiter = RateLimiter.of("alternativeRateLimiter", RateLimiterConfig.custom()
        .limitRefreshPeriod(Duration.ofSeconds(1))
        .limitForPeriod(alternativeProperties.rateLimitPerSec)
        .timeoutDuration(paymentOperationTimeout)
        .build())


    private val httpClientExecutor = Executors.newSingleThreadExecutor()


    private val client = OkHttpClient.Builder().run {
        dispatcher(Dispatcher(httpClientExecutor))
        build()
    }


    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>


    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        val elapsedTime = Duration.ofMillis(now() - paymentStartedAt)
        val rateLimiterToUse = if (elapsedTime < paymentOperationTimeout) {
            defaultRateLimiter
        } else {
            alternativeRateLimiter
        }


        CoroutineScope(Dispatchers.IO).launch {
            val result = runRateLimitedRequest(rateLimiterToUse, paymentId, amount)


            result.onSuccess { response ->
                processPaymentResponse(paymentId, response)
            }.onFailure { exception ->
                handlePaymentError(paymentId, exception)
            }
        }
    }


    private suspend fun runRateLimitedRequest(
        rateLimiter: RateLimiter, paymentId: UUID, amount: Int
    ): Result<ExternalSysResponse> {
        return withContext(Dispatchers.IO) {
            try {
                val supplier = RateLimiter.decorateCheckedSupplier(rateLimiter) {
                    executePaymentRequest(paymentId, amount)
                }
                Result.success(supplier.get())
            } catch (e: Exception) {
                Result.failure(e)
            }
        }
    }




    private fun executePaymentRequest(paymentId: UUID, amount: Int): ExternalSysResponse {
        val transactionId = UUID.randomUUID()
        val request = buildRequest(transactionId)
        val response = client.newCall(request).execute()
        return processHttpResponse(response)
    }


    private fun buildRequest(transactionId: UUID): Request {
        return Request.Builder()
            .url("http://localhost:1234/external/process?serviceName=${defaultProperties.serviceName}&accountName=${defaultProperties.accountName}&transactionId=$transactionId")
            .post(emptyBody)
            .build()
    }


    private fun processHttpResponse(response: Response): ExternalSysResponse {
        return response.use { resp ->
            try {
                mapper.readValue(resp.body?.string(), ExternalSysResponse::class.java)
            } catch (e: Exception) {
                logger.error("[$defaultProperties.accountName] [ERROR] Payment processed, result code: ${resp.code}, reason: ${resp.body?.string()}")
                ExternalSysResponse(false, e.message)
            }
        }
    }






    private fun processPaymentResponse(paymentId: UUID, response: ExternalSysResponse) {
        logger.warn("[$defaultProperties.accountName] Payment processed for payment $paymentId, succeeded: ${response.result}, message: ${response.message}")
        paymentESService.update(paymentId) {
            it.logProcessing(response.result, now(), UUID.randomUUID(), reason = response.message)
        }
    }


    private fun handlePaymentError(paymentId: UUID, exception: Throwable) {
        if (exception is SocketTimeoutException) {
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), UUID.randomUUID(), reason = "Request timeout.")
            }
        } else {
            logger.error("[$defaultProperties.accountName] Payment failed for payment $paymentId", exception)
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), UUID.randomUUID(), reason = exception.message)
            }
        }
    }
}
fun now() = System.currentTimeMillis()