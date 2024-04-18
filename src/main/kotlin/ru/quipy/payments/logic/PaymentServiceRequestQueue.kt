package ru.quipy.payments.logic

import org.slf4j.LoggerFactory
import ru.quipy.common.utils.CircuitBreakerOpenException
import ru.quipy.common.utils.NamedThreadFactory
import ru.quipy.common.utils.NonBlockingOngoingWindow
import ru.quipy.payments.config.ServiceConfigurer
import ru.quipy.payments.logic.PaymentExternalServiceImpl.Companion.paymentOperationTimeout
import java.util.concurrent.Executors

class PaymentServiceRequestQueue(
    private val paymentServiceConfig: ServiceConfigurer,
) {
    val accountName = paymentServiceConfig.service.accountName
    private val queueExecutor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), NamedThreadFactory("payment-queue-$accountName"))
    private val logger = LoggerFactory.getLogger(PaymentServiceRequestQueue::class.java)
    var fallback : (request: PaymentRequest) -> Unit = {}

    fun tryEnqueue(request: PaymentRequest): Boolean {
        logger.warn("Attempting to enqueue payment request: ${request.paymentId}")

        // Проверка, может ли быть выполнена операция через блокировку цепи
        if (!paymentServiceConfig.circuitBreaker.canMakeCall()) {
            logger.warn("Circuit breaker is open, cannot enqueue payment request: ${request.paymentId}")
            return false
        }

        // Вычисление времени, прошедшего с момента начала оплаты
        val timePassed = now() - request.paymentStartedAt

        // Вычисление ожидаемого времени обработки запроса, учитывая скорость обработки
        val expectedProcessingTime = paymentServiceConfig.service.requestAverageProcessingTime.toMillis() / paymentServiceConfig.service.speed

        // Вычисление, сколько задач может быть поставлено в очередь до истечения времени ожидания операции
        val canWait = paymentOperationTimeout.toMillis()/ timePassed
        logger.warn("canWait: ${canWait}, expectedProcessingTime: ${expectedProcessingTime}, timePassed: ${timePassed}, requestAverageProcessingTime: ${paymentServiceConfig.service.requestAverageProcessingTime.toMillis()}")

        // Проверка, может ли запрос быть поставлен в очередь на основе времени, прошедшего и ожидаемого времени обработки
            // Попытка поместить задачу в окно
        if (timePassed < expectedProcessingTime && canWait >= 1) {
        val windowResponse = paymentServiceConfig.window.putIntoWindow()
            if (NonBlockingOngoingWindow.WindowResponse.Success::class.java.isInstance(windowResponse)) {
                logger.warn("Successfully put payment request into the queue: ${request.paymentId}")
                // Если задача успешно помещена в окно, отправка ее в очередь
                queueExecutor.submit { queueJob(request) }
                return true
            } else {
                logger.warn("Failed to put payment request into the queue due to window being full: ${request.paymentId}")
                return false
            }
        } else {
            logger.warn("Payment request ${request.paymentId} has been waiting for too long, not enqueuing")
            return false
        }
        
    }


            private fun queueJob(request: PaymentRequest) {
        try {
            logger.warn("Processing payment request: ${request.paymentId}")
            paymentServiceConfig.circuitBreaker.submitExecution()
            paymentServiceConfig.rateLimiter.tickBlocking()
            paymentServiceConfig.service.submitPaymentRequest(request.paymentId, request.amount, request.paymentStartedAt, paymentServiceConfig.window, paymentServiceConfig.circuitBreaker)
        } catch (e: CircuitBreakerOpenException) {
            logger.error("Circuit breaker is open, falling back for payment request: ${request.paymentId}")
            fallback(request)
        } catch (e: Exception) {
            logger.error("Error while making payment on account $accountName: ${e.message}")
        } finally {
            // Release the window after the job is done or if an error occurred
            paymentServiceConfig.window.releaseWindow()
            logger.warn("Window size released")
        }
    }

    fun destroy() {
        logger.warn("Shutting down the queue executor")
        queueExecutor.shutdown()
//        try {
//        // Ожидание завершения всех задач в queueExecutor
//        if (!queueExecutor.awaitTermination(10, TimeUnit.SECONDS)) {
//            logger.warn("Queue executor did not terminate within the specified time")
//            // Принудительное завершение queueExecutor, если не удалось дождаться завершения задач
//            queueExecutor.shutdownNow()
//            logger.warn("Forcibly shut down the queue executor")
//        }
//    } catch (e: InterruptedException) {
//        logger.error("Interrupted while waiting for queue executor to terminate", e)
//        // Принудительное завершение queueExecutor при возникновении исключения
//        queueExecutor.shutdownNow()
//        logger.warn("Forcibly shut down the queue executor after interruption")
//        Thread.currentThread().interrupt()
//    }
//    logger.warn("Queue executor has been shut down")

        paymentServiceConfig.service.destroy()

        paymentServiceConfig.circuitBreaker.destroy()
    }
}