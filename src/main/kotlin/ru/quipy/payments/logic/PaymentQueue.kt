package ru.quipy.payments.logic

import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.payments.config.ServiceSet
import java.util.concurrent.Executors

class PaymentQueue(
    private val set: ServiceSet,
) {
    val accountName = set.service.accountName
    private val queueExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), NamedThreadFactory("payment-queue-$accountName"))

    fun tryEnqueue(request: PaymentRequest): Boolean {
        while (true) {
            val timePassed = now() - request.paymentStartedAt
            val timeLeft = PaymentOperationTimeout.toMillis() - timePassed
            val canWait = (timeLeft - set.service.requestAverageProcessingTime.toMillis()) / 1000.0 * set.service.speed
            val queued = set.window.jobCount.get()
            if (canWait - queued >= 1) {
                if (set.window.jobCount.compareAndSet(queued, queued + 1)) {
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
        set.window.acquireWindow()
        set.rateLimiter.tickBlocking()
        set.service.submitPaymentRequest(request.paymentId, request.amount, request.paymentStartedAt, set.window)
    }

    fun destroy() {
        queueExecutor.shutdown()
        set.service.destroy()
    }
}