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
import java.time.Instant
import java.time.ZoneId
import java.util.*
import java.util.concurrent.Executors

class PaymentExternalServiceImpl(
    private val defaultProperties: ExternalServiceProperties,
    private val alternativeProperties: ExternalServiceProperties
) : PaymentExternalService {


    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)
        val paymentOperationTimeout = Duration.ofSeconds(3)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }


    private val defaultRateLimiter: RateLimiter = RateLimiter.of("defaultRateLimiter", RateLimiterConfig.custom()
        .limitForPeriod(defaultProperties.rateLimitPerSec)
        .timeoutDuration(paymentOperationTimeout)
        .build())


    private val alternativeRateLimiter: RateLimiter = RateLimiter.of("alternativeRateLimiter", RateLimiterConfig.custom()
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
        val currentTimeMillis = System.currentTimeMillis()
        val elapsedTime = Duration.ofMillis(currentTimeMillis - paymentStartedAt)
        logger.warn(" elapsedTime $elapsedTime paymentStartedAt $paymentStartedAt ")

        val rateLimiterToUse = if (elapsedTime < paymentOperationTimeout) {
            defaultRateLimiter
        } else {
            alternativeRateLimiter
        }
        val currentAccount = if (elapsedTime < paymentOperationTimeout) {
            defaultProperties
        } else {
            alternativeProperties
        }

        CoroutineScope(Dispatchers.IO).launch {
            val result = runRateLimitedRequest(rateLimiterToUse, paymentId, amount, currentAccount)


            result.onSuccess { response ->
                processPaymentResponse(paymentId, response, currentAccount)
            }.onFailure { exception ->
                handlePaymentError(paymentId, exception, currentAccount)
            }
        }
    }


    private suspend fun runRateLimitedRequest(
        rateLimiter: RateLimiter, paymentId: UUID, amount: Int, currentAccount: ExternalServiceProperties
    ): Result<ExternalSysResponse> {
        return withContext(Dispatchers.IO) {
            try {
                val supplier = RateLimiter.decorateCheckedSupplier(rateLimiter) {
                    executePaymentRequest(paymentId, amount, currentAccount)
                }
                Result.success(supplier.get())
            } catch (e: Exception) {
                logger.error("[$currentAccount.accountName] [ERROR] Payment processed")
                Result.failure(e)
            }
        }
    }




    private fun executePaymentRequest(paymentId: UUID, amount: Int, currentAccount: ExternalServiceProperties): ExternalSysResponse {
        val transactionId = UUID.randomUUID()
        val request = buildRequest(transactionId, currentAccount)
        val response = client.newCall(request).execute()
        return processHttpResponse(response, currentAccount)
    }


    private fun buildRequest(transactionId: UUID, currentAccount: ExternalServiceProperties): Request {
        return Request.Builder()
            .url("http://localhost:1234/external/process?serviceName=${currentAccount.serviceName}&accountName=${currentAccount.accountName}&transactionId=$transactionId")
            .post(emptyBody)
            .build()
    }


    private fun processHttpResponse(response: Response, currentAccount: ExternalServiceProperties): ExternalSysResponse {
        return response.use { resp ->
            try {
                mapper.readValue(resp.body?.string(), ExternalSysResponse::class.java)
            } catch (e: Exception) {
                logger.error("[$currentAccount.accountName] [ERROR] Payment processed, result code: ${resp.code}, reason: ${resp.body?.string()}")
                ExternalSysResponse(false, e.message)
            }
        }
    }






    private fun processPaymentResponse(
        paymentId: UUID,
        response: ExternalSysResponse,
        currentAccount: ExternalServiceProperties
    ) {
        logger.warn("[$currentAccount.accountName] Payment processed for payment $paymentId, succeeded: ${response.result}, message: ${response.message}")
        paymentESService.update(paymentId) {
            it.logProcessing(response.result, now(), UUID.randomUUID(), reason = response.message)
        }
    }


    private fun handlePaymentError(paymentId: UUID, exception: Throwable, currentAccount: ExternalServiceProperties) {
        if (exception is SocketTimeoutException) {
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), UUID.randomUUID(), reason = "Request timeout.")
            }
        } else {
            logger.error("[$currentAccount.accountName] Payment failed for payment $paymentId", exception)
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), UUID.randomUUID(), reason = exception.message)
            }
        }
    }
}
fun now() = System.currentTimeMillis()