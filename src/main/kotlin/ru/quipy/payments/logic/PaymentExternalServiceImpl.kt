package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.time.Duration
import java.util.*
import java.util.concurrent.CompletableFuture
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException


// Advice: always treat time as a Duration
class PaymentExternalServiceImpl(
    private val defaultProperties: ExternalServiceProperties,
    private val alternativeProperties: ExternalServiceProperties,
) : PaymentExternalService {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)

        val paymentOperationTimeout = Duration.ofSeconds(80)

        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private val httpClientExecutor = Executors.newSingleThreadExecutor()

    private val client = OkHttpClient.Builder().run {
        dispatcher(Dispatcher(httpClientExecutor))
        build()
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        logger.warn("[${defaultProperties.accountName}] Submitting payment request for payment $paymentId. Already passed: ${now() - paymentStartedAt} ms")

        val transactionId = UUID.randomUUID()
        logger.info("[${defaultProperties.accountName}] Submit for $paymentId , txId: $transactionId")

        // Log that the payment was submitted regardless of the outcome.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        // Check if we can make a request with the first account.
        if (!defaultProperties.rateLimiter.tick()) {
            // If the first account cannot make a request, check the second account.
            if (!alternativeProperties.rateLimiter.tick()) {
                // If both accounts cannot make a request, log it and do not submit a request.
                logger.warn("Both accounts are rate limited, cannot submit payment request for payment $paymentId")
                return
            } else {
                // Use the second account to submit the request.
                submitPaymentRequestWithProperties(alternativeProperties, paymentId, transactionId)
            }
        } else {
            // Use the first account to submit the request.
            submitPaymentRequestWithProperties(defaultProperties, paymentId, transactionId)
        }
    }

    private fun submitPaymentRequestWithProperties(
        properties: ExternalServiceProperties,
        paymentId: UUID,
        transactionId: UUID
    ) {
        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${properties.serviceName}&accountName=${properties.accountName}&transactionId=$transactionId")
            post(emptyBody)
        }.build()

        // Use a CompletableFuture to handle the asynchronous request.
        val future = CompletableFuture<ExternalSysResponse>()

        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) {
                future.completeExceptionally(e)
            }

            override fun onResponse(call: Call, response: Response) {
                val body = try {
                    mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                } catch (e: Exception) {
                    logger.error("[${properties.accountName}] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                    ExternalSysResponse(false, e.message)
                }

                logger.warn("[${properties.accountName}] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                future.complete(body)
            }
        })

        // Use a timeout to handle the case where the request takes too long.
        future.orTimeout(paymentOperationTimeout.toMillis(), TimeUnit.MILLISECONDS)
            .whenComplete { body, throwable ->
                if (throwable is TimeoutException) {
                    logger.error("[${properties.accountName}] Payment failed for txId: $transactionId, payment: $paymentId due to timeout.")
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                } else if (throwable != null) {
                    logger.error(
                        "[${properties.accountName}] Payment failed for txId: $transactionId, payment: $paymentId",
                        throwable
                    )
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = throwable.message)
                    }
                } else {
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                }
            }
    }
}


public fun now() = System.currentTimeMillis()