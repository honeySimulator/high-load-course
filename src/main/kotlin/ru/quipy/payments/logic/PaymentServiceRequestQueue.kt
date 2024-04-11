package ru.quipy.payments.logic

import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.payments.config.ServiceConfigurer
import ru.quipy.payments.logic.PaymentExternalServiceImpl.Companion.paymentOperationTimeout
import java.util.concurrent.Executors

class PaymentServiceRequestQueue(
    private val paymentServiceConfig: ServiceConfigurer,
) {
    val accountName = paymentServiceConfig.service.accountName
    private val queueExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), NamedThreadFactory("payment-queue-$accountName"))

    fun tryEnqueue(request: PaymentRequest): Boolean {
        while (true) {
            val timePassed = now() - request.paymentStartedAt
            val timeLeft = paymentOperationTimeout.toMillis() - timePassed
            // Calculate the expected request processing time taking into account the processing speed
            val expectedProcessingTime = paymentServiceConfig.service.requestAverageProcessingTime.toMillis() / paymentServiceConfig.service.speed
            // Calculate how many tasks can be queued before the operation timeout expires
            val canWait = (timeLeft - expectedProcessingTime) / 1000.0
            val queued = paymentServiceConfig.window.jobCount.get()
            if (canWait - queued >= 1) {
                if (paymentServiceConfig.window.jobCount.compareAndSet(queued, queued + 1)) {
                    queueExecutor.submit{ queueJob(request) }
                    return true
                }
            } else {
                return false
            }
        }
    }

    private fun queueJob(request: PaymentRequest)
    {
        paymentServiceConfig.window.acquireWindow()
        paymentServiceConfig.rateLimiter.tickBlocking()
        paymentServiceConfig.service.submitPaymentRequest(request.paymentId, request.amount, request.paymentStartedAt, paymentServiceConfig.window)
    }

    fun destroy() {
        queueExecutor.shutdown()
//        try {
//            if (!queueExecutor.awaitTermination(60, TimeUnit.SECONDS)) {
//                queueExecutor.shutdownNow()
//            }
//        } catch (e: InterruptedException) {
//            queueExecutor.shutdownNow()
//        }

        paymentServiceConfig.service.destroy()
    }
}