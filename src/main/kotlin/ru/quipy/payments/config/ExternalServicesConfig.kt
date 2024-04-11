package ru.quipy.payments.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import ru.quipy.common.utils.*
import ru.quipy.core.EventSourcingService
import ru.quipy.payments.api.PaymentAggregate
import ru.quipy.payments.logic.*
import java.time.Duration
import java.util.*
import java.util.concurrent.TimeUnit


@Configuration
class ExternalServicesConfig(
    private val paymentESService: EventSourcingService<UUID, PaymentAggregate, PaymentAggregateState>
) {
    companion object {
        const val PRIMARY_PAYMENT_BEAN = "PRIMARY_PAYMENT_BEAN"

        // Ниже приведены готовые конфигурации нескольких аккаунтов провайдера оплаты.
        // Заметьте, что каждый аккаунт обладает своими характеристиками и стоимостью вызова.

//        private val accountProps_1 = ExternalServiceProperties(
//            serviceName = "test",
//            accountName = "default-1",
//            nonBlockingWindow = NonBlockingOngoingWindow(1000),
//            request95thPercentileProcessingTime = Duration.ofMillis(1000),
//            rateLimiter = CoroutineRateLimiter(100) // Используем CoroutineRateLimiter с ограничением в 100 запросов в секунду
//        )

        private val accountProps_2 = ExternalServiceProperties(
            // Call costs 70
            "test",
            "default-2",
            parallelRequests = 100,
            rateLimitPerSec = 30,
            request95thPercentileProcessingTime = Duration.ofMillis(10_000),
            cost = 70
        )

        private val accountProps_3 = ExternalServiceProperties(
            // Call costs 40
            "test",
            "default-3",
            parallelRequests = 30,
            rateLimitPerSec = 8,
            request95thPercentileProcessingTime = Duration.ofMillis(10_000),
            cost = 40
        )

        // Call costs 30
        private val accountProps_4 = ExternalServiceProperties(
            "test",
            "default-4",
            parallelRequests = 8,
            rateLimitPerSec = 5,
            request95thPercentileProcessingTime = Duration.ofMillis(10_000),
            cost = 30
        )
    }

    @Bean(PRIMARY_PAYMENT_BEAN)
    fun fastExternalService() =
        PaymentServiceBalancer(
            listOf(
                ServiceConfigurer(
                    PaymentExternalServiceImpl(accountProps_2, paymentESService),
                    RateLimiter(accountProps_2.rateLimitPerSec, TimeUnit.SECONDS),
                    JobExecutionWindow(NonBlockingOngoingWindow(accountProps_2.parallelRequests)),
                ),
                ServiceConfigurer(
                    PaymentExternalServiceImpl(accountProps_3, paymentESService),
                    RateLimiter(accountProps_3.rateLimitPerSec, TimeUnit.SECONDS),
                    JobExecutionWindow(NonBlockingOngoingWindow(accountProps_3.parallelRequests)),
                ),
                ServiceConfigurer(
                    PaymentExternalServiceImpl(accountProps_4, paymentESService),
                    RateLimiter(accountProps_4.rateLimitPerSec, TimeUnit.SECONDS),
                    JobExecutionWindow(NonBlockingOngoingWindow(accountProps_4.parallelRequests)),
                ),
            ),
            paymentESService
        )
}

class ServiceConfigurer(
    val service: PaymentExternalServiceImpl,
    val rateLimiter: RateLimiter,
    val window: JobExecutionWindow
)