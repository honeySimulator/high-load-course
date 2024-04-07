package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import kotlinx.coroutines.*
import okhttp3.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.common.utils.Summary
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit
import kotlin.coroutines.resume
import kotlin.coroutines.resumeWithException


// Advice: always treat time as a Duration
class PaymentExternalServiceImpl(
    private val firstProperties: ExternalServiceProperties,
    private val secondProperties: ExternalServiceProperties,
    private val thirdProperties: ExternalServiceProperties
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

    private val httpClientExecutor = Executors.newCachedThreadPool()

    private val client = OkHttpClient.Builder().run {
        dispatcher(Dispatcher(httpClientExecutor))
        connectionPool(ConnectionPool(100, 5, TimeUnit.MINUTES))
        protocols(listOf(Protocol.H2_PRIOR_KNOWLEDGE))
        build()
    }

    private val requestSummary = Summary(0.0)

    override fun submitPaymentRequest(paymentId: UUID, amount: Int, paymentStartedAt: Long) {
        logger.warn("Submitting payment request for payment $paymentId. Already passed: ${now() - paymentStartedAt} ms")

        val transactionId = UUID.randomUUID()
        logger.info(" Submit for $paymentId , txId: $transactionId")

        // Log that the payment was submitted regardless of the outcome.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        // Check if we can make a request with the first account.
        val propertiesToUse = when {
            firstProperties.rateLimiter.tick() -> firstProperties
            secondProperties.rateLimiter.tick() -> secondProperties
            thirdProperties.rateLimiter.tick() -> thirdProperties
            else -> {
                logger.warn("All accounts are rate limited, cannot submit payment request for payment $paymentId")
                return
            }
        }

        externalScope.launch {
            val startTime = now()
            val response = submitPaymentRequestWithProperties(propertiesToUse, paymentId, transactionId)
            val endTime = now()
            requestSummary.reportExecution(endTime - startTime)

            if (response != null) {
                paymentESService.update(paymentId) { currentState ->
                    currentState.logProcessing(
                        response.result,
                        now(),
                        UUID.randomUUID(),
                        reason = response.message
                    )
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
        val windowResponse = properties.nonBlockingWindow.putIntoWindow()
        logger.info("[${properties.accountName}] Submit for $paymentId , txId: $transactionId")

        if (windowResponse !is NonBlockingOngoingWindow.WindowResponse.Success) {
            logger.warn("[${properties.accountName}] Window is full for payment $paymentId, cannot submit request")
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Window is full.")
            }

        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${properties.serviceName}&accountName=${properties.accountName}&transactionId=$transactionId")
            post(emptyBody)
        }.build()

        // Use a withTimeout to handle the timeout.
        return try {
            withContext(Dispatchers.IO) {
                withTimeoutOrNull(paymentOperationTimeout.toMillis()) {
                    suspendCancellableCoroutine<ExternalSysResponse?> { cont ->
                        client.newCall(request).enqueue(object : Callback {
                            override fun onFailure(call: Call, e: IOException) {
                                cont.resumeWithException(e)
                            }

                            override fun onResponse(call: Call, response: Response) {
                                try {
                                    val body =
                                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
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
                } ?: ExternalSysResponse(false, "Request timeout.")
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
        } finally {
            // Release the window after the request
            properties.nonBlockingWindow.releaseWindow()
        }
    }
}

public fun now() = System.currentTimeMillis()