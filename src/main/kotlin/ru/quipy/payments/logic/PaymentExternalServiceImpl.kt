package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.*
import okhttp3.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException


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

    val externalScope = CoroutineScope(Dispatchers.IO)

    @Autowired
    private lateinit var paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>

    private val httpClientExecutor = Executors.newSingleThreadExecutor()

    private val client = OkHttpClient.Builder().run {
        dispatcher(Dispatcher(httpClientExecutor))
        build()
    }

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        logger.warn("Submitting payment request for payment $paymentId. Already passed: ${now() - paymentStartedAt} ms")

        val transactionId = UUID.randomUUID()
        logger.info(" Submit for $paymentId , txId: $transactionId")

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
                externalScope.launch {
                    val response = submitPaymentRequestWithProperties(alternativeProperties, paymentId, transactionId)
                    if (response != null) {
                        paymentESService.update(paymentId) { currentState ->
                            currentState.logProcessing(response.result, now(), UUID.randomUUID(), reason = response.message)
                        }
                    }
                }
            }
        } else {
            // Use the first account to submit the request.
            externalScope.launch {
                val response = submitPaymentRequestWithProperties(defaultProperties, paymentId, transactionId)
                if (response != null) {
                    paymentESService.update(paymentId) { currentState ->
                        currentState.logProcessing(response.result, now(), UUID.randomUUID(), reason = response.message)
                    }
                }
            }
        }
    }

    private suspend fun submitPaymentRequestWithProperties(
        properties: ExternalServiceProperties,
        paymentId: UUID,
        transactionId: UUID
    ): ExternalSysResponse? {
//         Try to acquire a window for the account.
//        val windowResponse = properties.nonBlockingWindow.putIntoWindow()
//        logger.info("[${properties.accountName}] Submit for $paymentId , txId: $transactionId")

//        if (windowResponse !is NonBlockingOngoingWindow.WindowResponse.Success) {
//            logger.warn("[${properties.accountName}] Window is full for payment $paymentId, cannot submit request")
//            return null
//        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${properties.serviceName}&accountName=${properties.accountName}&transactionId=$transactionId")
            post(emptyBody)
        }.build()

        // Use a withTimeout to handle the timeout.
        return try {
            withTimeout(paymentOperationTimeout.toMillis()) {
                suspendCancellableCoroutine<ExternalSysResponse?> { cont ->
                    client.newCall(request).enqueue(object : Callback {
                        override fun onFailure(call: Call, e: IOException) {
                            cont.resumeWithException(e)
                        }

                        override fun onResponse(call: Call, response: Response) {
                            try {
                                val body = mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                                logger.warn("[${properties.accountName}] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")
                                paymentESService.update(paymentId) {
                                    it.logProcessing(body.result, now(), transactionId, reason = body.message)
                                }
                                cont.resume(body)
                            } catch (e: Exception) {
                                cont.resumeWithException(e)
                            }
                        }
                    })
                }
            }
        } catch (e: TimeoutCancellationException) {
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
            }
            logger.warn("[${properties.accountName}] Payment request for payment $paymentId timed out after ${paymentOperationTimeout.toMillis()} ms")
            null
        } catch (e: CancellationException) {
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = e.message)
            }
            null
        } catch (e: Exception) {
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = e.message)
            }
            logger.error("[${properties.accountName}] An error occurred while processing the payment request: ${e.message}")
            null
        }
//        finally {
//            // Release the window after the request
//            properties.nonBlockingWindow.releaseWindow()
//        }
    }
}

public fun now() = System.currentTimeMillis()