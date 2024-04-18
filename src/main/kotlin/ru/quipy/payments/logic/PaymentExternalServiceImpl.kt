package ru.quipy.payments.logic

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.kotlin.registerKotlinModule
import okhttp3.*
import org.slf4j.LoggerFactory
import ru.quipy.common.utils.MyCircuitBreaker
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import java.io.IOException
import java.net.SocketTimeoutException
import java.time.Duration
import java.util.*
import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit


// Advice: always treat time as a Duration
class PaymentExternalServiceImpl(
    private val properties: ExternalServiceProperties,
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) {

    companion object {
        val logger = LoggerFactory.getLogger(PaymentExternalServiceImpl::class.java)
        val paymentOperationTimeout = Duration.ofSeconds(80)
        val emptyBody = RequestBody.create(null, ByteArray(0))
        val mapper = ObjectMapper().registerKotlinModule()
    }


    val serviceName = properties.serviceName
    val accountName = properties.accountName
    val requestAverageProcessingTime = properties.request95thPercentileProcessingTime
    val speed = properties.speed
    val cost = properties.cost
    private val callbackExecutor = Executors.newFixedThreadPool(
        Runtime.getRuntime().availableProcessors(),
        NamedThreadFactory("callback-$accountName")
    )


    private val client = OkHttpClient.Builder().run {
        dispatcher(Dispatcher().apply {
            maxRequests = properties.parallelRequests
            maxRequestsPerHost = properties.parallelRequests
        })
        connectionPool(
            ConnectionPool(
                properties.parallelRequests,
                properties.request95thPercentileProcessingTime.seconds,
                TimeUnit.SECONDS
            )
        )
        protocols(Collections.singletonList(Protocol.H2_PRIOR_KNOWLEDGE))
        connectTimeout(requestAverageProcessingTime)
        readTimeout(requestAverageProcessingTime)
        build()
    }

    fun submitPaymentRequest(
        paymentId: UUID,
        amount: Int,
        paymentStartedAt: Long,
        window: NonBlockingOngoingWindow,
        circuitBreaker: MyCircuitBreaker
    ) {
        val passed = now() - paymentStartedAt
        logger.warn("[$accountName] Submitting payment request for payment $paymentId. Already passed: $passed ms")

        val transactionId = UUID.randomUUID()

        if (Duration.ofMillis(passed) > paymentOperationTimeout) {
            paymentESService.update(paymentId) {
                it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
            }
            return
        }
        logger.warn("[$accountName] Submit for $paymentId , txId: $transactionId")

        // Вне зависимости от исхода оплаты важно отметить что она была отправлена.
        // Это требуется сделать ВО ВСЕХ СЛУЧАЯХ, поскольку эта информация используется сервисом тестирования.
        paymentESService.update(paymentId) {
            it.logSubmission(success = true, transactionId, now(), Duration.ofMillis(now() - paymentStartedAt))
        }

        val request = Request.Builder().run {
            url("http://localhost:1234/external/process?serviceName=${serviceName}&accountName=${accountName}&transactionId=$transactionId")
            post(emptyBody)
        }.build()

        val now = now()
        client.newCall(request).enqueue(object : Callback {
            override fun onFailure(call: Call, e: IOException) {
                window.releaseWindow()
                circuitBreaker.submitFailure()
                callbackExecutor.submit {
                    handleException(paymentId, transactionId, e)
                }
            }

            override fun onResponse(call: Call, response: Response) {
                logger.warn("${(now() - now) / 1000.0} s. for account $accountName. Dispatcher: ${client.dispatcher.runningCallsCount()} running, ${client.dispatcher.queuedCallsCount()} queued")
                window.releaseWindow()
                circuitBreaker.submitFailure()
                callbackExecutor.submit {
                    val body = try {
                        mapper.readValue(response.body?.string(), ExternalSysResponse::class.java)
                    } catch (e: Exception) {
                        logger.error("[$accountName] [ERROR] Payment processed for txId: $transactionId, payment: $paymentId, result code: ${response.code}, reason: ${response.body?.string()}")
                        ExternalSysResponse(false, e.message)
                    }

                    logger.warn("[$accountName] Payment processed for txId: $transactionId, payment: $paymentId, succeeded: ${body.result}, message: ${body.message}")

                    // Здесь мы обновляем состояние оплаты в зависимости от результата в базе данных оплат.
                    // Это требуется сделать ВО ВСЕХ ИСХОДАХ (успешная оплата / неуспешная / ошибочная ситуация)
                    paymentESService.update(paymentId) {
                        it.logProcessing(body.result, now(), transactionId, reason = body.message)
                    }
                }
            }
        })
    }

    private fun handleException(paymentId: UUID, transactionId: UUID, exception: Exception) {
        try {
            when (exception) {
                is SocketTimeoutException -> {
                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = "Request timeout.")
                    }
                }

                else -> {
                    logger.error(
                        "[$accountName] Payment failed for txId: $transactionId, payment: $paymentId",
                        exception
                    )

                    paymentESService.update(paymentId) {
                        it.logProcessing(false, now(), transactionId, reason = exception.message)
                    }
                }
            }
        } catch (e: Exception) {
            logger.error(
                "[$accountName] Exception during handling payment error!",
                exception
            )
        }
    }

    fun destroy() {
        logger.warn("Shutting down the callback executor")
        callbackExecutor.shutdown()
//        try {
//        if (!callbackExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
//            logger.warn("Callback executor did not terminate within the specified time")
//            callbackExecutor.shutdownNow()
//            logger.warn("Forcibly shut down the callback executor")
//        }
//    } catch (e: InterruptedException) {
//        logger.error("Interrupted while waiting for callback executor to terminate", e)
//        callbackExecutor.shutdownNow()
//        logger.warn("Forcibly shut down the callback executor after interruption")
//        Thread.currentThread().interrupt()
//    }
        logger.warn("Shutting down the client dispatcher executor")
        client.dispatcher.executorService.shutdown()
//        try {
//        if (!client.dispatcher.executorService.awaitTermination(60, TimeUnit.SECONDS)) {
//            logger.warn("Client dispatcher executor did not terminate within the specified time")
//            client.dispatcher.executorService.shutdownNow()
//            logger.warn("Forcibly shut down the client dispatcher executor")
//        }
//    } catch (e: InterruptedException) {
//        logger.error("Interrupted while waiting for client dispatcher executor to terminate", e)
//        client.dispatcher.executorService.shutdownNow()
//        logger.warn("Forcibly shut down the client dispatcher executor after interruption")
//        Thread.currentThread().interrupt()
//    }
//        logger.warn("Callback executor and client dispatcher executor have been shut down")
    }
}

public fun now() = System.currentTimeMillis()